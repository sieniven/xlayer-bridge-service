package pushtask

import (
	"context"
	"fmt"
	"time"

	"github.com/0xPolygonHermez/zkevm-bridge-service/bridgectrl/pb"
	"github.com/0xPolygonHermez/zkevm-bridge-service/db"
	"github.com/0xPolygonHermez/zkevm-bridge-service/db/pgstorage"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/messagepush"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	VERSION = "1"
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
	// Use bridge DB pull deposit information.
	storage DepositNotifierStorage
	// Calls bridge gRPC endpoints to build claim messages
	bridgeCli pb.BridgeServiceClient
	// Reuse bridge kafka producer to send messages
	producer messagepush.KafkaProducer
}

// gRPC client retry configuration string.
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

func NewDepositNotifier(cfg *DepositNotifierConfig, storage db.Storage, producer messagepush.KafkaProducer) (*DepositNotifier, error) {
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
		producer:  producer,
	}, nil
}

func (dn *DepositNotifier) Start(ctx context.Context) error {
	duration, err := time.ParseDuration(dn.cfg.Interval)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {

		case <-ctx.Done():
			log.Warn("Notifier is shutting down...")
			return nil

		case <-ticker.C:
			var deposits []*pgstorage.DepositToNotify
			// Pull the oldest records for each type for fairness.
			claimedDeposits, err := dn.storage.GetDepositsForNotification(ctx, pgstorage.CLAIMED, dn.cfg.Limit)
			if err != nil {
				log.Warnw("GetDepositsForNotification (claimed) FAILED", "err", err)
				continue
			}

			readyDeposits, err := dn.storage.GetDepositsForNotification(ctx, pgstorage.READY_FOR_CLAIM, dn.cfg.Limit)
			if err != nil {
				log.Warnw("GetDepositsForNotification (ready_for_claim) FAILED", "err", err)
				continue
			}

			deposits = append(deposits, claimedDeposits...)
			deposits = append(deposits, readyDeposits...)

			log.Infow("There are >= 1 deposits to notify", pgstorage.CLAIMED, len(claimedDeposits), pgstorage.READY_FOR_CLAIM, len(readyDeposits))

			// Nothing to notify, moving on...
			if len(deposits) == 0 {
				continue
			}

			for _, dep := range deposits {

				bridgeRes, err := dn.bridgeCli.GetBridge(ctx, &pb.GetBridgeRequest{
					DepositCnt: dep.DepositCount,
					NetId:      dep.NetworkID,
				})
				if err != nil {
					log.Warnf("Failed to get bridge. Cnt = %d, NetId = %d, %v", dep.DepositCount, dep.NetworkID, err)
					continue
				}
				d := bridgeRes.GetDeposit()

				switch dep.Txtype {
				case pgstorage.CLAIMED:
					// Skip if deposit (L1->L2) has not been claimed.
					if d.ClaimTxHash == "" {
						continue
					}

					claimedMsg := ClaimedMessage{
						Version: VERSION,
						TxType:  dep.Txtype,
						Deposit: DepositInfo{
							Count:  fmt.Sprint(d.DepositCnt),
							TxHash: d.TxHash,
						},
						Claim: ClaimedInfo{
							TxHash: d.ClaimTxHash,
						},
					}

					var msg string
					if msg, err = messagepush.Notify(dn.producer, claimedMsg); err != nil {
						log.Warnw("Failed to send notification for claimed message.", "err", err)
					}

					if err = dn.storage.UpdateDepositForNotification(ctx, dep.Id, []byte(msg)); err != nil {
						log.Warnw("Failed to update record for ready_for_claim message.", "err", err)
					}

				case pgstorage.READY_FOR_CLAIM:
					// Skip if (L2->L1) is not ready for claim
					if !d.ReadyForClaim {
						log.Debugw("L2 to L1 txn not ready for claim yet...", "Cnt", dep.DepositCount, "NetworkID", dep.NetworkID)
						continue
					}

					proofResp, err := dn.bridgeCli.GetProof(ctx, &pb.GetProofRequest{
						DepositCnt: dep.DepositCount,
						NetId:      dep.NetworkID,
					})
					if err != nil {
						log.Warnf("Failed to get merkle proof. Cnt = %d, NetId = %d, %v", dep.DepositCount, dep.NetworkID, err)
						continue
					}

					readyForClaimMsg := ReadyForClaimMessage{
						Version: VERSION,
						TxType:  dep.Txtype,
						Deposit: DepositInfo{
							Count:  fmt.Sprint(d.DepositCnt),
							TxHash: d.TxHash,
						},
						Claim: ToClaimInfo{
							// from "/merkle-proof" endpoint
							SmtProofLocalER:  proofResp.Proof.GetMerkleProof(),
							SmtProofRollupER: proofResp.Proof.GetRollupMerkleProof(),
							MainnetER:        proofResp.Proof.GetMainExitRoot(),
							RollupER:         proofResp.Proof.GetRollupExitRoot(),

							// from "/bridge" endpoint
							GlobalIndex:     d.GlobalIndex,
							OriginNetwork:   fmt.Sprint(d.OrigNet),
							OriginTokenAddr: d.OrigAddr,
							DestNetwork:     fmt.Sprint(d.DestNet),
							DestAddr:        d.DestAddr,
							Amount:          d.Amount,
							Metadata:        d.Metadata,
						},
					}

					var msg string
					if msg, err = messagepush.Notify(dn.producer, readyForClaimMsg); err != nil {
						log.Warnw("Failed to send notification for ready_for_claim message.", "err", err)
					}

					if err = dn.storage.UpdateDepositForNotification(ctx, dep.Id, []byte(msg)); err != nil {
						log.Warnw("Failed to update record for ready_for_claim message.", "err", err)
					}

				default:
					log.Error("Unknown txtype, not possible")
				}
			}
		}
	}
}
