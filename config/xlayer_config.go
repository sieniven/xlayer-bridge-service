package config

import (
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"

	apolloconfig "github.com/0xPolygonHermez/zkevm-bridge-service/config/apollo_xlayer"
	businessconfig "github.com/0xPolygonHermez/zkevm-bridge-service/config/business_xlayer"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/coinmiddleware"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/iprestriction"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/messagepush"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/nacos"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/redisstorage"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/tokenlogoinfo"
)

type ethermanCfg struct {
	L1ChainId  uint   `mapstructure:"L1ChainId"`
	L2ChainIds []uint `mapstructure:"L2ChainIds"`
}

type serverCfg struct {
	// SentinelConfigFilePath is the file path to store the sentinel config
	SentinelConfigFilePath string              `mapstructure:"SentinelConfigFilePath"`
	Redis                  redisstorage.Config `apollo:"Redis"`
}

type metricsCfg struct {
	// Env is the environment label for the metrics, to separate mainnet and testnet metrics
	Env string `mapstructure:"Env"`
}

type claimTxManCfg struct {
	// FreeGas enabled whether gas price is 0
	FreeGas bool `mapstructure:"FreeGas"`
	// OptClaim enabled store claimTx into storage every deposit
	OptClaim bool `mapstructure:"OptClaim"`
}

// We will use this config to wrap the upstream config and load
// our XLayer specfic values in this struct.
type XLayerConfig struct {
	// All upstream structs are preserved
	UpstreamCfg *Config

	// Configs that overlapped with upstream configs.
	// Add new fields here instead of adding them to the upstream structs.
	Etherman       ethermanCfg
	BridgeServer   serverCfg
	Metrics        metricsCfg
	ClaimTxManager claimTxManCfg

	// Pure XLayer configs
	Apollo                 apolloconfig.Config
	NacosConfig            nacos.Config
	BusinessConfig         businessconfig.Config `apollo:"BusinessConfig"`
	IPRestriction          iprestriction.Config  `apollo:"IPRestriction"`
	TokenLogoServiceConfig tokenlogoinfo.Config  `apollo:"TokenLogoServiceConfig"`
	CoinKafkaConsumer      coinmiddleware.Config `apollo:"CoinKafkaConsumer"`
	MessagePushProducer    messagepush.Config    `apollo:"MessagePushProducer"`
}

// Load the same config file, but this time load all X Layer config here.
// The config toml file structure must remains the same.
func LoadXLayerCfg(upstreamCfg *Config) (*XLayerConfig, error) {
	var cfg XLayerConfig
	viper.SetConfigType("toml")
	err := viper.Unmarshal(&cfg, viper.DecodeHook(mapstructure.TextUnmarshallerHookFunc()))
	if err != nil {
		return nil, err
	}
	cfg.UpstreamCfg = upstreamCfg
	return &cfg, nil
}
