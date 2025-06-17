package pushtask

import (
	"context"

	"github.com/0xPolygonHermez/zkevm-bridge-service/db/pgstorage"
)

type DepositNotifierStorage interface {
	GetDepositsForNotification(ctx context.Context, txtype string, limit uint) ([]*pgstorage.DepositToNotify, error)
	UpdateDepositForNotification(ctx context.Context, id uint64, msg []byte) error
	SkipDepositForNotification(ctx context.Context, id uint64) error
}

type DepositInfo struct {
	Count  string `json:"count"`
	TxHash string `json:"txHash"`
}

type ClaimedInfo struct {
	TxHash string `json:"txHash"`
}

type ToClaimInfo struct {
	SmtProofLocalER  []string `json:"smtProofLocalExitRoot"`
	SmtProofRollupER []string `json:"smtProofRollupExitRoot"`
	GlobalIndex      string   `json:"globalIndex"`
	MainnetER        string   `json:"mainnetExitRoot"`
	RollupER         string   `json:"rollupExitRoot"`
	OriginNetwork    string   `json:"originNetwork"`
	OriginTokenAddr  string   `json:"originTokenAddress"`
	DestNetwork      string   `json:"destinationNetwork"`
	DestAddr         string   `json:"destinationAddress"`
	Amount           string   `json:"amount"`
	Metadata         string   `json:"metadata"`
}

type ClaimedMessage struct {
	Version string      `json:"version"`
	TxType  string      `json:"type"`
	Deposit DepositInfo `json:"deposit"`
	Claim   ClaimedInfo `json:"claim"`
}

type ReadyForClaimMessage struct {
	Version string      `json:"version"`
	TxType  string      `json:"type"`
	Deposit DepositInfo `json:"deposit"`
	Claim   ToClaimInfo `json:"claim"`
}
