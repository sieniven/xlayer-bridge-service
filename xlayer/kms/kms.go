package kms

import (
	"fmt"
	"strings"

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
		if realPass == "" {
			return "", fmt.Errorf("Failed to fetch DB pass from KMS: password is empty.")
		}
		return realPass, nil
	}
	return dbPassword, nil // Return original password if not encrypted
}
