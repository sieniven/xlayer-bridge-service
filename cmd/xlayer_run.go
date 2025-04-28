package main

import (
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"

	"github.com/0xPolygonHermez/zkevm-bridge-service/bridgectrl"
	"github.com/0xPolygonHermez/zkevm-bridge-service/claimtxman"
	"github.com/0xPolygonHermez/zkevm-bridge-service/db"
	"github.com/0xPolygonHermez/zkevm-bridge-service/etherman"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"github.com/0xPolygonHermez/zkevm-bridge-service/server"
	"github.com/0xPolygonHermez/zkevm-bridge-service/synchronizer"
	"github.com/0xPolygonHermez/zkevm-bridge-service/utils"
	"github.com/0xPolygonHermez/zkevm-bridge-service/utils/gerror"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/coinmiddleware"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/estimatetime"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/iprestriction"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/localcache"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/messagepush"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/metrics"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/pushtask"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/redisstorage"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/sentinel"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/tokenlogoinfo"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/utils/messagebridge"

	apolloconfig "github.com/0xPolygonHermez/zkevm-bridge-service/config/apollo_xlayer"
	client "github.com/0xPolygonHermez/zkevm-bridge-service/jsonrpcclient"
	xlayerUtils "github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/utils"
)

func runAPI(ctx *cli.Context) error {
	c, err := setupConfig(ctx)
	if err != nil {
		return err
	}

	if c.UpstreamCfg.Metrics.Enabled {
		go metrics.StartMetricsHttpServer(struct {
			Env  string
			Host string
			Port int
		}{
			Env:  c.Metrics.Env,
			Host: c.UpstreamCfg.Metrics.Host,
			Port: c.UpstreamCfg.Metrics.Port,
		})
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
	networkIDs := setupNetworkIDs(networkID, l2Ethermans)
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

	waitUnlessInterrupt()
	return err
}

func runPushTask(ctx *cli.Context) error {
	c, err := setupConfig(ctx)
	if err != nil {
		return err
	}

	if c.UpstreamCfg.Metrics.Enabled {
		go metrics.StartMetricsHttpServer(struct {
			Env  string
			Host string
			Port int
		}{
			Env:  c.Metrics.Env,
			Host: c.UpstreamCfg.Metrics.Host,
			Port: c.UpstreamCfg.Metrics.Port,
		})
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
	log.Info("push service: RollupID =", rollupID)

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
	// Use this to run Go routines
	errs, _ := errgroup.WithContext(ctx.Context)

	c, err := setupConfig(ctx)
	if err != nil {
		return err
	}

	messagebridge.InitUSDCLxLyProcessor(c.BusinessConfig.USDCContractAddresses, c.BusinessConfig.USDCTokenAddresses)
	messagebridge.InitWstETHProcessor(c.BusinessConfig.WstETHContractAddresses, c.BusinessConfig.WstETHTokenAddresses)
	messagebridge.InitEURCProcessor(c.BusinessConfig.EURCContractAddresses, c.BusinessConfig.EURCTokenAddresses)

	if c.UpstreamCfg.Metrics.Enabled {
		go metrics.StartMetricsHttpServer(struct {
			Env  string
			Host string
			Port int
		}{
			Env:  c.Metrics.Env,
			Host: c.UpstreamCfg.Metrics.Host,
			Port: c.UpstreamCfg.Metrics.Port,
		})
	}

	// Initialize all stores
	apiStorage, err := db.NewStorage(c.UpstreamCfg.BridgeServer.DB)
	if err != nil {
		return err
	}
	storage, err := db.NewStorage(c.UpstreamCfg.SyncDB)
	if err != nil {
		return err
	}
	redisStorage, err := redisstorage.NewRedisStorage(c.BridgeServer.Redis)
	if err != nil {
		return err
	}

	l1Etherman, l2Ethermans, err := newEthermans(c.UpstreamCfg)
	if err != nil {
		return err
	}

	networkID := l1Etherman.GetNetworkID()
	networkIDs := setupNetworkIDs(networkID, l2Ethermans)

	var bridgeController *bridgectrl.BridgeController
	if c.UpstreamCfg.BridgeController.Store == "postgres" {
		bridgeController, err = bridgectrl.NewBridgeController(
			ctx.Context, c.UpstreamCfg.BridgeController, networkIDs, storage)
		if err != nil {
			return err
		}
	} else {
		return gerror.ErrStorageNotRegister
	}

	var messagePushProducer messagepush.KafkaProducer
	if c.MessagePushProducer.Enabled {
		messagePushProducer, err = setupKafkaProducer(c.MessagePushProducer)
		if err != nil {
			return err
		}
	}

	bridgeService := server.NewBridgeService(c.UpstreamCfg.BridgeServer, c.UpstreamCfg.BridgeController.Height, networkIDs, apiStorage)

	chSynced := make(chan uint32)
	var chsExitRootEvent []chan *etherman.GlobalExitRoot
	var chsSyncedL2 []chan uint32
	for i, l2EthermanClient := range l2Ethermans {
		L2URL := c.UpstreamCfg.Etherman.L2URLs[i]
		zkEVMClient := client.NewClient(L2URL)
		rollupID := l2EthermanClient.GetNetworkID() // RollupID == networkID

		chExitRootEventL2 := make(chan *etherman.GlobalExitRoot)
		chsExitRootEvent = append(chsExitRootEvent, chExitRootEventL2)
		chSyncedL2 := make(chan uint32)
		chsSyncedL2 = append(chsSyncedL2, chSyncedL2)

		sync, err := synchronizer.NewSynchronizer(
			ctx.Context, storage, bridgeController,
			l2EthermanClient, zkEVMClient, 0,
			chExitRootEventL2, nil, chSyncedL2,
			c.UpstreamCfg.Synchronizer, []uint32{}, c.UpstreamCfg.NetworkConfig.RequireSovereignChainSmcs[i],
		)
		if err != nil {
			return err
		}
		cliSyncL2, ok := sync.(*synchronizer.ClientSynchronizer)
		if !ok {
			log.Fatalf("Failed to cast synchronizer")
		}
		cliSyncL2 = cliSyncL2.
			SetProducer(messagePushProducer).
			SetRedis(redisStorage).
			SetRollupID(uint(rollupID))
		errs.Go(cliSyncL2.Sync)

		if c.UpstreamCfg.ClaimTxManager.Enabled {
			nodeClient, err := utils.NewClient(ctx.Context, L2URL, c.UpstreamCfg.NetworkConfig.L2PolygonBridgeAddresses[i])
			if err != nil {
				log.Error(err)
				return err
			}
			nonceCache, err := claimtxman.NewNonceCache(ctx.Context, nodeClient)
			if err != nil {
				log.Fatalf("error creating nonceCache for L2 %s. Error: %v", L2URL, err)
			}
			auth, err := nodeClient.GetSignerFromKeystore(ctx.Context, c.UpstreamCfg.ClaimTxManager.PrivateKey)
			if err != nil {
				log.Error(err)
				return err
			}

			claimTxManager, err := claimtxman.NewClaimTxManager(
				ctx.Context, c.UpstreamCfg.ClaimTxManager, chsExitRootEvent[i], chsSyncedL2[i],
				L2URL, networkIDs[i+1], c.UpstreamCfg.NetworkConfig.L2PolygonBridgeAddresses[i], bridgeService,
				storage, rollupID, l2Ethermans[i], nonceCache, auth,
			)
			claimTxManager.SetRedis(redisStorage)
			claimTxManager.SetProducer(messagePushProducer)
			claimTxManager.SetFreegas(c.ClaimTxManager.FreeGas)
			claimTxManager.SetOptClaim(c.ClaimTxManager.OptClaim)
			if err != nil {
				log.Fatalf("error creating claim tx manager for L2 %s. Error: %v", L2URL, err)
			}
			go claimTxManager.StartXLayer()
		}
	}

	sync, err := synchronizer.NewSynchronizer(
		ctx.Context, storage, bridgeController, l1Etherman,
		nil, c.UpstreamCfg.NetworkConfig.GenBlockNumber, nil,
		chsExitRootEvent, chSynced, c.UpstreamCfg.Synchronizer, networkIDs, false,
	)
	if err != nil {
		return err
	}
	cliSyncL1, ok := sync.(*synchronizer.ClientSynchronizer)
	if ok {
		cliSyncL1 = cliSyncL1.
			SetProducer(messagePushProducer).
			SetRedis(redisStorage).
			SetRollupID(uint(networkID))
	}
	errs.Go(cliSyncL1.Sync)

	if !c.UpstreamCfg.ClaimTxManager.Enabled {
		log.Warn("ClaimTxManager not configured")
		go func() {
			for {
				select {
				case netID := <-chSynced:
					log.Debug("L1 Network synced. NetowrkID: ", netID)
					for _, ch := range chsSyncedL2 {
						ch <- netID
					}
				case <-ctx.Done():
					log.Debug("Stopping goroutine that listen new GER updates")
					return
				}
			}
		}()

		for i := range chsExitRootEvent {
			monitorChannel(ctx.Context, chsExitRootEvent[i], chsSyncedL2[i], networkIDs[i+1], storage)
		}
	}

	tokenlogoinfo.InitClient(c.TokenLogoServiceConfig)

	if len(c.CoinKafkaConsumer.Brokers) > 0 {
		coinKafkaConsumer, err := coinmiddleware.NewKafkaConsumer(c.CoinKafkaConsumer, redisStorage)
		if err != nil {
			return err
		}
		defer func() {
			if err := coinKafkaConsumer.Close(); err != nil {
				log.Errorf("close kafka consumer error: %v", err)
			}
		}()
		log.Debugf("finish initializing kafka consumer")
		go coinKafkaConsumer.Start(ctx.Context)
	}

	return errs.Wait()
}
