package config

type claimTxManCfg struct {
	// FreeGas enabled whether gas price is 0
	FreeGas bool `mapstructure:"FreeGas"`
	// OptClaim enabled store claimTx into storage every deposit
	OptClaim bool `mapstructure:"OptClaim"`
	// Number of DB claim records to query
	MonitorTxsLimit uint `mapstructure:"MonitorTxsLimit"`
}

type syncCfg struct {
	LargeTxUsdLimit uint64 `mapstructure:"LargeTxUsdLimit"`

	// When set to true, new deposits will also records into
	// notification tracking table. These records will be used
	// by PUSH service to check which deposits have notifications
	// been sent.
	EnableNotificationTracking bool `mapstructure:"EnableNotificationTracking"`

	// Used in synchronizer when EnableNotificationTracking is set true.
	// This is used to determine txtype (claimed/ready_for_claim) to be set for the deposit
	// based on direction of deposit (L1->L2, L2->L1). Default is 0.
	L1NetID uint32 `mapstructure:"L1NetID"`
}
