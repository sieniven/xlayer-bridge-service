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
}
