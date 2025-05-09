package utils

import (
	apolloconfig "github.com/0xPolygonHermez/zkevm-bridge-service/config/apollo_xlayer"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
)

var (
	// L1TargetBlockConfirmations is the number of block confirmations need to wait for the transaction to be synced from L1 to L2
	L1TargetBlockConfirmations = apolloconfig.NewIntEntry[uint64]("l1TargetBlockConfirmations", 64) //nolint:gomnd
)

func init() {
	log.Info("L1TargetBlockConfirmations = ", L1TargetBlockConfirmations)
}
