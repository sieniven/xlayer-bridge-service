package main

import (
	"os"
	"os/signal"

	cli "github.com/urfave/cli/v2"

	"github.com/0xPolygonHermez/zkevm-bridge-service/config"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/messagepush"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/nacos"

	kmsDB "github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/kms"
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

func setupConfig(ctx *cli.Context) (*config.XLayerConfig, error) {
	configFilePath := ctx.String(flagCfg)
	network := ctx.String(flagNetwork)
	cfg, err := config.Load(configFilePath, network)
	if err != nil {
		return nil, err
	}
	// NOTE: Load XLayer config over the upstream configuration.
	return config.LoadXLayerCfg(cfg)
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
