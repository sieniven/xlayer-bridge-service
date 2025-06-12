package pushtask

import (
	"context"
	"fmt"
	"time"

	"github.com/0xPolygonHermez/zkevm-bridge-service/bridgectrl/pb"
	"github.com/0xPolygonHermez/zkevm-bridge-service/db"
	"github.com/0xPolygonHermez/zkevm-bridge-service/db/pgstorage"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// These are actual values used in `txtype` in
	// `sync.notification_tracker` table.
	CLAIMED         = "claimed"
	READY_FOR_CLAIM = "ready_for_claim"
)

type DepositNotifierConfig struct {
	// If false, nothing is run.
	Enable bool `mapstructure:"Enable"`

	// Maximum number of notifications messages to push to Kafka.
	// This means up to `Limit` records will be fetched for each
	// `claimed` and `ready_for_claim` deposits.
	Limit uint `mapstructure:"Limit"`
	// time string (1s, 1m)
	Interval string `mapstructure:"Interval"`
	// Kafka topic to push messages to
	Topic string `mapstructure:"Topic"`
}

type DepositNotifier struct {
	cfg *DepositNotifierConfig

	storage DepositNotifierStorage

	bridgeCli pb.BridgeServiceClient
}

func NewDepositNotifier(cfg *DepositNotifierConfig, storage db.Storage) (*DepositNotifier, error) {
	if cfg == nil {
		return nil, fmt.Errorf("DepositNotifierConfig is nil")
	}
	store, ok := storage.(DepositNotifierStorage)
	if !ok {
		return nil, fmt.Errorf("Failed to cast DepositNotifierStorage")
	}

	conn, err := grpc.NewClient("xlayer-bridge-service-api:9090", // TODO: allow this to be set in config
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("did not connect: %v", err)
	}
	c := pb.NewBridgeServiceClient(conn)

	return &DepositNotifier{
		cfg:       cfg,
		storage:   store,
		bridgeCli: c,
	}, nil
}

func (dn *DepositNotifier) Start(ctx context.Context) error {
	// Capture any panics in Go routine and log.
	defer func() {
		if r := recover(); r != nil {
			log.Info("Notifier panicked: ", r)
		}
	}()

	duration, err := time.ParseDuration(dn.cfg.Interval)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(duration)
	for {
		select {
		case <-ctx.Done():
			log.Warn("Notifier is shutting down...")
			return nil
		case <-ticker.C:
			var deposits []*pgstorage.DepositToNotify
			// Pull the oldest records for each type for fairness.
			claimedDeposits, err := dn.storage.GetDepositsForNotification(ctx, CLAIMED, dn.cfg.Limit)
			readyDeposits, err := dn.storage.GetDepositsForNotification(ctx, READY_FOR_CLAIM, dn.cfg.Limit)
			deposits = append(deposits, claimedDeposits...)
			deposits = append(deposits, readyDeposits...)

			log.Infow("There are >= 1 deposits to notify", CLAIMED, len(claimedDeposits), READY_FOR_CLAIM, len(readyDeposits))

			// Nothing to notify, moving on...
			if len(deposits) == 0 {
				continue
			}

			if err != nil {
				log.Warnw("GetDepositsForNotification FAILED", "err", err)
				return err
			}

			for _, dep := range deposits {
				grpcCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				r, err := dn.bridgeCli.GetBridge(grpcCtx, &pb.GetBridgeRequest{
					DepositCnt: dep.DepositCount,
					NetId:      dep.NetworkID,
				})
				if err != nil {
					log.Warnf("Failed to get bridge. Cnt = %d, NetId = %d, %v", dep.DepositCount, dep.NetworkID, err)
					continue
				}

				if dep.Txtype == CLAIMED {
				} else if dep.Txtype == READY_FOR_CLAIM {
					log.Infof("Bridge: %s", r.GetDeposit())
					proofResp, err := dn.bridgeCli.GetProof(grpcCtx, &pb.GetProofRequest{
						DepositCnt: dep.DepositCount,
					})

					// TODO: if any requests failed, we should make it `is_sent` as true to avoid double send.
					if err != nil {
						log.Warnf("Failed to get merkle proof. Cnt = %d, NetId = %d, %v", dep.DepositCount, dep.NetworkID, err)
						continue
					}
					log.Infof("Proof: %s", proofResp.GetProof())
				} else {
					log.Error("Unknown txtype, not possible")
				}
			}
		}
	}
}
