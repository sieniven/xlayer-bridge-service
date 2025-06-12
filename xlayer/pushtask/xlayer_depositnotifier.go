package pushtask

import (
	"context"
	"time"

	"github.com/0xPolygonHermez/zkevm-bridge-service/db"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
)

type DepositNotifierConfig struct {
	// maximum number of notifications messages to push to Kafka
	Limit uint `mapstructure:"Limit"`
	// time string (1s, 1m)
	Interval string `mapstructure:"Interval"`
	// Kafka topic to push messages to
	Topic string `mapstructure:"Topic"`
}

type DepositNotifier struct {
	cfg *DepositNotifierConfig

	storage DepositNotifierStorage
}

func NewDepositNotifier(cfg *DepositNotifierConfig, storage db.Storage) *DepositNotifier {
	return &DepositNotifier{
		cfg:     cfg,
		storage: storage.(DepositNotifierStorage),
	}
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

	log.Info("Notifier duration: ", duration)

	ticker := time.NewTicker(duration)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			log.Info("DepositNotifier tick")

			deposits, err := dn.storage.GetDepositsForNotification(ctx, dn.cfg.Limit)
			if err != nil {
				log.Warnw("GetDepositsForNotification FAILED", "err", err)
				return err
			}

			for _, dep := range deposits {
				log.Infow("DEPOSIT", "deposit", dep)
			}

			// TODO: fetch up to `Limit` records from new table
			// TODO: for each record, make http request to get bridge info.
		}
	}
}
