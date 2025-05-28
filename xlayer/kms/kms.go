package kms

import (
	"fmt"
	"strings"

	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	kms "gitlab.okg.com/okcoin-commons/ok-kms-go-client/kms"
)

func GetDBPassword(dbPassword string) (string, error) {
	const encryptedPrefix = "{encrypt}"
	if strings.HasPrefix(dbPassword, encryptedPrefix) {
		if err := kms.Init(); err != nil {
			return "", fmt.Errorf("failed to init KMS: %w", err)
		}
		secretKey := strings.TrimPrefix(dbPassword, encryptedPrefix)
		realPass, err := kms.GetAwsSecretValue(secretKey)
		log.Info("REAL PASS:", realPass)
		if err != nil {
			return "", fmt.Errorf("failed to fetch DB pass from KMS: %w", err)
		}
		return realPass, nil
	}
	return dbPassword, nil // Return original password if not encrypted
}
