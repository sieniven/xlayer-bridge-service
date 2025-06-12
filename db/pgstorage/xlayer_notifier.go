package pgstorage

const (
	// These are actual values used in `txtype` in
	// `sync.notification_tracker` table.
	CLAIMED         = "claimed"
	READY_FOR_CLAIM = "ready_for_claim"
)
