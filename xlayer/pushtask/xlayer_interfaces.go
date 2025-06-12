package pushtask

import (
	"context"

	"github.com/0xPolygonHermez/zkevm-bridge-service/db/pgstorage"
)

type DepositNotifierStorage interface {
	GetDepositsForNotification(ctx context.Context, limit uint) ([]*pgstorage.DepositToNotify, error)
	UpdateDepositForNotification(ctx context.Context, id uint64) error
}
