package estimatetime

import (
	"context"

	"github.com/0xPolygonHermez/zkevm-bridge-service/etherman"
)

// Calculator provides methods to calculate the deposit estimate time by sampling recent deposits
type Calculator interface {
	Get(networkID uint) uint32
}

type DBStorage interface {
	GetLatestReadyDeposits(ctx context.Context, networkID uint, limit uint, dbTx interface{}) ([]*etherman.Deposit, error)
}
