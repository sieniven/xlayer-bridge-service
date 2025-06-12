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
	// Url to the bridge API (gRPC).
	BridgeUrl string `mapstructure:"BridgeUrl"`
}

type DepositNotifier struct {
	cfg *DepositNotifierConfig

	storage DepositNotifierStorage

	bridgeCli pb.BridgeServiceClient
}

const retryPolicy = `{
	"methodConfig": [{
		"name": [{"service": "your_project.YourService"}],
		"retryPolicy": {
			"MaxAttempts": 4,
			"InitialBackoff": "0.1s",
			"MaxBackoff": "1s",
			"BackoffMultiplier": 2.0,
			"RetryableStatusCodes": [
				"UNAVAILABLE",
				"INTERNAL",
				"DEADLINE_EXCEEDED"
			]
		}
	}]
}`

func NewDepositNotifier(cfg *DepositNotifierConfig, storage db.Storage) (*DepositNotifier, error) {
	if cfg == nil {
		return nil, fmt.Errorf("DepositNotifierConfig is nil")
	}

	store, ok := storage.(DepositNotifierStorage)
	if !ok {
		return nil, fmt.Errorf("Failed to cast DepositNotifierStorage")
	}

	conn, err := grpc.NewClient(
		cfg.BridgeUrl,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(retryPolicy), // Apply the retry policy
		grpc.WithMaxCallAttempts(4),                // Important: This caps the total attempts, ensure it matches MaxAttempts in policy
	)
	if err != nil {
		return nil, fmt.Errorf("did not connect: %v", err)
	}

	return &DepositNotifier{
		cfg:       cfg,
		storage:   store,
		bridgeCli: pb.NewBridgeServiceClient(conn),
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
			claimedDeposits, err := dn.storage.GetDepositsForNotification(ctx, pgstorage.CLAIMED, dn.cfg.Limit)
			readyDeposits, err := dn.storage.GetDepositsForNotification(ctx, pgstorage.READY_FOR_CLAIM, dn.cfg.Limit)
			deposits = append(deposits, claimedDeposits...)
			deposits = append(deposits, readyDeposits...)

			log.Infow("There are >= 1 deposits to notify", pgstorage.CLAIMED, len(claimedDeposits), pgstorage.READY_FOR_CLAIM, len(readyDeposits))

			// Nothing to notify, moving on...
			if len(deposits) == 0 {
				continue
			}

			if err != nil {
				log.Warnw("GetDepositsForNotification FAILED", "err", err)
				return err
			}

			for _, dep := range deposits {
				grpcCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				r, err := dn.bridgeCli.GetBridge(grpcCtx, &pb.GetBridgeRequest{
					DepositCnt: dep.DepositCount,
					NetId:      dep.NetworkID,
				})
				if err != nil {
					log.Warnf("Failed to get bridge. Cnt = %d, NetId = %d, %v", dep.DepositCount, dep.NetworkID, err)
					continue
				}

				if dep.Txtype == pgstorage.CLAIMED {
				} else if dep.Txtype == pgstorage.READY_FOR_CLAIM {
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
