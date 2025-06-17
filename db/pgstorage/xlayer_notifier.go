package pgstorage

import "github.com/0xPolygonHermez/zkevm-bridge-service/log"

const (
	// These are actual values used in `txtype` in
	// `sync.notification_tracker` table.
	CLAIMED         = "claimed"
	READY_FOR_CLAIM = "ready_for_claim"
)

var (
	L1netId uint32 = 0
)

func SetL1NetID(id uint32) {
	L1netId = id
	log.Infow("Deposit Notifier L1 Net ID", "netID", L1netId)
}
