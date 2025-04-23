package config

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestXLayerConfig(t *testing.T) {
	c, err := Load("./xlayer-config.local.toml", "")
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, viper.IsSet("Etherman.L1URL") && c.Etherman.L1URL != "")
	assert.True(t, viper.IsSet("Etherman.L2URLs") && len(c.Etherman.L2URLs) > 0)

	cfg, err := LoadXLayerCfg(c)
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, viper.IsSet("Etherman.L1URL") && cfg.UpstreamCfg.Etherman.L1URL != "")
	assert.True(t, viper.IsSet("Etherman.L2URLs") && len(cfg.UpstreamCfg.Etherman.L2URLs) > 0)
	assert.True(t, viper.IsSet("Etherman.L1ChainId"))
	assert.True(t, viper.IsSet("Etherman.L2ChainIds"))
}
