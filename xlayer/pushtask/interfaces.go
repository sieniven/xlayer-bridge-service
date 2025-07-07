package pushtask

import (
	"context"

	"github.com/0xPolygonHermez/zkevm-bridge-service/etherman"
)

type DBStorage interface {
	GetNotReadyTransactionsWithBlockRange(ctx context.Context, networkID uint, minBlockNum, maxBlockNum uint64, limit, offset uint, dbTx interface{}) ([]*etherman.Deposit, error)
}
