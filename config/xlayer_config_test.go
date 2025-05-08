package config

import (
	"fmt"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// Assert XLayer configs can be parsed into config.
func TestXLayerConfig(t *testing.T) {
	c, err := Load("./configs_xlayer/xlayer-test-config.local.toml", "")
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadXLayerCfg(c)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("upstream config:\n%+v\n\n", cfg.UpstreamCfg)

	fmt.Printf("Etherman.L1ChainId = %+v\n", cfg.Etherman.L1ChainId)
	assert.True(t, viper.IsSet("Etherman.L1ChainId"))
	fmt.Printf("Etherman.L2ChainIds = %+v\n", cfg.Etherman.L2ChainIds)
	assert.True(t, viper.IsSet("Etherman.L2ChainIds"))
	fmt.Printf("BridgeServer.SentinelConfigFilePath = %+v\n", cfg.BridgeServer.SentinelConfigFilePath)
	assert.True(t, viper.IsSet("BridgeServer.SentinelConfigFilePath"))
	fmt.Printf("BridgeServer.Redis = %+v\n", cfg.BridgeServer.Redis)
	assert.True(t, viper.IsSet("BridgeServer.Redis"))
	fmt.Printf("Metrics.Env = %+v\n", cfg.Metrics.Env)
	assert.True(t, viper.IsSet("Metrics.Env"), "metrics env not set")
	fmt.Printf("Apollo = %+v\n", cfg.Apollo)
	assert.True(t, viper.IsSet("Apollo"))
	fmt.Printf("NacosConfig = %+v\n", cfg.NacosConfig)
	assert.True(t, viper.IsSet("NacosConfig"))
	fmt.Printf("BusinessConfig = %+v\n", cfg.BusinessConfig)
	assert.True(t, viper.IsSet("BusinessConfig"), "business config not set")
	fmt.Printf("IPRestriction = %+v\n", cfg.IPRestriction)
	assert.True(t, viper.IsSet("IPRestriction"), "IP restriction not set")
	fmt.Printf("TokenLogoServiceConfig = %+v\n", cfg.TokenLogoServiceConfig)
	assert.True(t, viper.IsSet("TokenLogoServiceConfig"), "token Logo not set")
	fmt.Printf("CoinKafkaConsumer = %+v\n", cfg.CoinKafkaConsumer)
	assert.True(t, viper.IsSet("CoinKafkaConsumer"))
	fmt.Printf("MessagePushProducer = %+v\n", cfg.MessagePushProducer)
	assert.True(t, viper.IsSet("MessagePushProducer"))
	fmt.Printf("LargeTxUsdLimit = %+v\n", cfg.Synchronizer.LargeTxUsdLimit)
	assert.True(t, viper.IsSet("Synchronizer.LargeTxUsdLimit"))
}
