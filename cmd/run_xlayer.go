package main

import (
	"os"
	"os/signal"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/urfave/cli/v2"

	"github.com/0xPolygonHermez/zkevm-bridge-service/config"
	apolloconfig "github.com/0xPolygonHermez/zkevm-bridge-service/config/apollo_xlayer"
	"github.com/0xPolygonHermez/zkevm-bridge-service/db"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"github.com/0xPolygonHermez/zkevm-bridge-service/metrics"
	"github.com/0xPolygonHermez/zkevm-bridge-service/server"
	"github.com/0xPolygonHermez/zkevm-bridge-service/utils"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/estimatetime"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/iprestriction"
	kmsDB "github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/kms"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/localcache"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/messagepush"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/nacos"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/redisstorage"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/sentinel"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/tokenlogoinfo"
	xlayerUtils "github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/utils"
)

func runAPI(ctx *cli.Context) error {
	configFilePath := ctx.String(flagCfg)
	network := ctx.String(flagNetwork)
	cfg, err := config.Load(configFilePath, network)
	if err != nil {
		return err
	}

	// NOTE: Load XLayer config over the upstream configuration.
	c, err := config.LoadXLayerCfg(cfg)
	apolloconfig.SetLogger()
	loadKmsPasswords(c.UpstreamCfg)
	setupLog(c.UpstreamCfg.Log)

	// Init global vars from config
	xlayerUtils.InnitOkInnerChainIdMapper(c.BusinessConfig)
	iprestriction.InitClient(c.IPRestriction)
	tokenlogoinfo.InitClient(c.TokenLogoServiceConfig)

	redisStorage, err := redisstorage.NewRedisStorage(c.Redis)
	if err != nil {
		log.Error(err)
		return err
	}

	apiStorage, err := db.NewStorage(c.UpstreamCfg.BridgeServer.DB)
	if err != nil {
		log.Error(err)
		return err
	}

	if err = localcache.InitDefaultCache(apiStorage); err != nil {
		log.Error(err)
		return err
	}

	// Used in bridge service API
	if err = estimatetime.InitDefaultCalculator(apiStorage); err != nil {
		log.Error(err)
		return err
	}

	// NOTE: for fake producer
	var messagePushProducer messagepush.KafkaProducer
	if c.MessagePushProducer.Enabled {
		log.Infof("message push producer's switch is open, so init producer!")
		messagePushProducer, err = messagepush.NewKafkaProducer(c.MessagePushProducer)
		if err != nil {
			log.Error(err)
			return err
		}
		defer func() {
			err := messagePushProducer.Close()
			if err != nil {
				log.Errorf("close kafka producer error: %v", err)
			}
		}()
	}

	l1ChainId := c.Etherman.L1ChainId
	l2ChainIds := c.Etherman.L2ChainIds
	var chainIDs = []uint{l1ChainId}
	chainIDs = append(chainIDs, l2ChainIds...)
	l1Etherman, l2Ethermans, err := newEthermans(c.UpstreamCfg)
	if err != nil {
		return err
	}

	networkID := l1Etherman.GetNetworkID()
	var networkIDs = []uint32{networkID}
	for _, cl := range l2Ethermans {
		networkID := cl.GetNetworkID()
		log.Infof("l2 network id: %d", networkID)
		networkIDs = append(networkIDs, networkID)
		xlayerUtils.InitRollupNetworkId(uint(networkID))
	}
	xlayerUtils.InitChainIdManager(networkIDs, chainIDs)

	l2NodeClients := make([]*utils.Client, len(c.UpstreamCfg.Etherman.L2URLs))
	l2Auths := make([]*bind.TransactOpts, len(c.UpstreamCfg.Etherman.L2URLs))
	for i := range c.UpstreamCfg.Etherman.L2URLs {
		nodeClient, err := utils.NewClient(
			ctx.Context,
			c.UpstreamCfg.Etherman.L2URLs[i],
			c.UpstreamCfg.NetworkConfig.L2PolygonBridgeAddresses[i],
		)
		if err != nil {
			log.Error(err)
			return err
		}
		auth, err := nodeClient.GetSignerFromKeystore(ctx.Context, c.UpstreamCfg.ClaimTxManager.PrivateKey)
		if err != nil {
			log.Error(err)
			return err
		}
		l2NodeClients[i] = nodeClient
		l2Auths[i] = auth
	}

	registerNacos(c.NacosConfig)

	if c.Apollo.Enabled {
		err = sentinel.InitApolloDataSource(c.Apollo)
	} else {
		err = sentinel.InitFileDataSource(c.BridgeServer.SentinelConfigFilePath)
	}
	if err != nil {
		log.Infof("init sentinel error[%v]; ignored and proceed with no sentinel config", err)
	}

	bridgeService := server.NewBridgeService(c.UpstreamCfg.BridgeServer, c.UpstreamCfg.BridgeController.Height, networkIDs, apiStorage).
		WithRedisStorage(redisStorage).
		WithMainCoinsCache(localcache.GetDefaultCache()).
		WithMessagePushProducer(messagePushProducer).
		SetupL2Clients(l2NodeClients, l2Auths, networkIDs)

	if err = server.RunServer(c.UpstreamCfg.BridgeServer, bridgeService); err != nil { // non-blocking
		return err
	}

	if c.UpstreamCfg.Metrics.Enabled {
		// This uses upstream metrics
		metrics.Init()
		go startMetricsHttpServer(c.UpstreamCfg.Metrics)
	}

	waitUnlessInterrupt()
	return err
}

func runPushTask(ctx *cli.Context) error {
	waitUnlessInterrupt()
	return nil
}

func runTask(ctx *cli.Context) error {
	// messagebridge.InitUSDCLxLyProcessor(c.BusinessConfig.USDCContractAddresses, c.BusinessConfig.USDCTokenAddresses)
	// messagebridge.InitWstETHProcessor(c.BusinessConfig.WstETHContractAddresses, c.BusinessConfig.WstETHTokenAddresses)
	// messagebridge.InitEURCProcessor(c.BusinessConfig.EURCContractAddresses, c.BusinessConfig.EURCTokenAddresses)

	waitUnlessInterrupt()
	return nil
}

func waitUnlessInterrupt() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
}

func registerNacos(cfg nacos.Config) {
	var err error
	if cfg.NacosUrls != "" {
		err = nacos.InitNacosClient(cfg.NacosUrls, cfg.NamespaceId, cfg.ApplicationName, cfg.ExternalListenAddr)
	}
	log.Debugf("Init nacos NacosUrls[%s] NamespaceId[%s] ApplicationName[%s] ExternalListenAddr[%s] Error[%v]", cfg.NacosUrls, cfg.NamespaceId, cfg.ApplicationName, cfg.ExternalListenAddr, err)
}

func loadKmsPasswords(c *config.Config) error {
	var err error
	c.BridgeServer.DB.Password, err = kmsDB.GetDBPassword(c.BridgeServer.DB.Password)
	if err != nil {
		log.Fatal(err)
	}
	c.SyncDB.Password, err = kmsDB.GetDBPassword(c.SyncDB.Password)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}
