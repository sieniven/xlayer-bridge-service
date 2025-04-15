package apolloconfig

import (
	"github.com/apolloconfig/agollo/v4"

	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
)

func getLogger() *log.Logger {
	return log.WithFields(loggerFieldKey, loggerFieldValue)
}

func SetLogger() {
	agollo.SetLogger(getLogger())
}
