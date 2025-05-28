package kms

import (
	"fmt"
	"strings"

	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"github.com/okx/mock_kms/kms"
)

func GetDBPassword(dbPassword string) (string, error) {
	const encryptedPrefix = "{encrypt}"
	if strings.HasPrefix(dbPassword, encryptedPrefix) {
		if err := kms.Init(); err != nil {
			return "", fmt.Errorf("failed to init KMS: %w", err)
		}
		secretKey := strings.TrimPrefix(dbPassword, encryptedPrefix)
		realPass := kms.GetAwsSecretValue(secretKey)
		log.Info("REAL PASS:", realPass)
		if realPass == "" {
			return "", fmt.Errorf("Failed to fetch DB pass from KMS: password is empty.")
		}
		return realPass, nil
	}
	return dbPassword, nil // Return original password if not encrypted
}
