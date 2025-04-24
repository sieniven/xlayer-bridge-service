package main

import (
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/urfave/cli/v2"

	"github.com/0xPolygonHermez/zkevm-bridge-service/db"
	"github.com/0xPolygonHermez/zkevm-bridge-service/etherman"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"github.com/0xPolygonHermez/zkevm-bridge-service/metrics"
	"github.com/0xPolygonHermez/zkevm-bridge-service/server"
	"github.com/0xPolygonHermez/zkevm-bridge-service/utils"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/estimatetime"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/iprestriction"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/localcache"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/messagepush"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/pushtask"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/redisstorage"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/sentinel"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/tokenlogoinfo"

	apolloconfig "github.com/0xPolygonHermez/zkevm-bridge-service/config/apollo_xlayer"
	xlayerUtils "github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/utils"
)

func runAPI(ctx *cli.Context) error {
	c, err := setupConfig(ctx)
	if err != nil {
		return err
	}

	apolloconfig.SetLogger()
	setupLog(c.UpstreamCfg.Log)

	loadKmsPasswords(c.UpstreamCfg)
	if err = db.RunMigrations(c.UpstreamCfg.SyncDB); err != nil {
		return err
	}

	// Init global vars from config
	xlayerUtils.InnitOkInnerChainIdMapper(c.BusinessConfig)
	iprestriction.InitClient(c.IPRestriction)
	tokenlogoinfo.InitClient(c.TokenLogoServiceConfig)

	redisStorage, err := redisstorage.NewRedisStorage(c.BridgeServer.Redis)
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

	var messagePushProducer messagepush.KafkaProducer
	if c.MessagePushProducer.Enabled {
		messagePushProducer, err = setupKafkaProducer(c.MessagePushProducer)
		if err != nil {
			return err
		}
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
	c, err := setupConfig(ctx)
	if err != nil {
		return err
	}

	apiStorage, err := db.NewStorage(c.UpstreamCfg.BridgeServer.DB)
	if err != nil {
		log.Error(err)
		return err
	}

	redisStorage, err := redisstorage.NewRedisStorage(c.BridgeServer.Redis)
	if err != nil {
		log.Error(err)
		return err
	}

	var messagePushProducer messagepush.KafkaProducer
	if c.MessagePushProducer.Enabled {
		messagePushProducer, err = setupKafkaProducer(c.MessagePushProducer)
		if err != nil {
			return err
		}
	}

	l1Etherman, err := etherman.NewClient(c.UpstreamCfg.Etherman,
		c.UpstreamCfg.NetworkConfig.PolygonBridgeAddress,
		c.UpstreamCfg.NetworkConfig.PolygonZkEVMGlobalExitRootAddress,
		c.UpstreamCfg.NetworkConfig.PolygonRollupManagerAddress)
	rollupID, err := l1Etherman.PolygonRollupManager.RollupAddressToID(
		&bind.CallOpts{Pending: false},
		c.UpstreamCfg.NetworkConfig.PolygonRollupManagerAddress,
	)

	// Initialize the push task for L1 block num change
	l1BlockNumTask, err := pushtask.NewL1BlockNumTask(
		c.UpstreamCfg.Etherman.L1URL, apiStorage, redisStorage, messagePushProducer, uint(rollupID))
	if err != nil {
		return err
	}

	// Initialize the push task for sync l2 commit batch
	syncCommitBatchTask, err := pushtask.NewCommittedBatchHandler(
		c.UpstreamCfg.Etherman.L2URLs[0], apiStorage, redisStorage, messagePushProducer, uint(rollupID))
	if err != nil {
		return err
	}

	// Initialize the push task for sync verify batch
	syncVerifyBatchTask, err := pushtask.NewVerifiedBatchHandler(c.UpstreamCfg.Etherman.L2URLs[0], redisStorage)
	if err != nil {
		return err
	}

	go l1BlockNumTask.Start(ctx.Context)
	go syncCommitBatchTask.Start(ctx.Context)
	go syncVerifyBatchTask.Start(ctx.Context)

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
