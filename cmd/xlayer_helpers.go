package main

import (
	"os"
	"os/signal"

	cli "github.com/urfave/cli/v2"

	"github.com/0xPolygonHermez/zkevm-bridge-service/config"
	"github.com/0xPolygonHermez/zkevm-bridge-service/etherman"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/iprestriction"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/messagepush"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/nacos"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/tokenlogoinfo"

	apolloconfig "github.com/0xPolygonHermez/zkevm-bridge-service/config/apollo_xlayer"
	kmsDB "github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/kms"
	xlayerUtils "github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/utils"
)

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

func setupConfigAndLog(ctx *cli.Context) (*config.XLayerConfig, error) {
	configFilePath := ctx.String(flagCfg)
	network := ctx.String(flagNetwork)
	cfg, err := config.Load(configFilePath, network)
	if err != nil {
		return nil, err
	}

	if err = loadKmsPasswords(cfg); err != nil {
		return nil, err
	}

	// NOTE: Load XLayer config over the upstream configuration.
	c, err := config.LoadXLayerCfg(cfg)
	setupLog(c.UpstreamCfg.Log)
	xlayerUtils.InnitOkInnerChainIdMapper(c.BusinessConfig)
	iprestriction.InitClient(c.IPRestriction)
	tokenlogoinfo.InitClient(c.TokenLogoServiceConfig)

	if c.Apollo.Enabled {
		apolloconfig.SetLogger()
		if err = apolloconfig.Init(c.Apollo); err != nil {
			return nil, err
		}
		if err = apolloconfig.Load(&c.UpstreamCfg); err != nil {
			return nil, err
		}
	}

	return c, err
}

func setupKafkaProducer(cfg messagepush.Config) (messagepush.KafkaProducer, error) {
	var messagePushProducer messagepush.KafkaProducer
	log.Infof("message push producer's switch is open, so init producer!")
	messagePushProducer, err := messagepush.NewKafkaProducer(cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := messagePushProducer.Close()
		if err != nil {
			log.Errorf("close kafka producer error: %v", err)
		}
	}()
	return messagePushProducer, nil
}

// The list of network IDs will follow as: {L1 network ID, L2 network IDs...}
func setupNetworkIDs(networkID uint32, l2Ethermans []*etherman.Client) []uint32 {
	var networkIDs = []uint32{networkID}
	for _, cl := range l2Ethermans {
		networkID := cl.GetNetworkID()
		log.Infof("l2 network id: %d", networkID)
		networkIDs = append(networkIDs, networkID)
		xlayerUtils.InitRollupNetworkId(uint(networkID))
	}
	return networkIDs
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
