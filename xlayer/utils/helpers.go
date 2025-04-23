package utils

import (
	"encoding/hex"

	"github.com/0xPolygonHermez/zkevm-bridge-service/bridgectrl/pb"
	"github.com/0xPolygonHermez/zkevm-bridge-service/etherman"
)

func EthermanDepositToPbTransaction(deposit *etherman.Deposit) *pb.Transaction {
	if deposit == nil {
		return nil
	}

	return &pb.Transaction{
		FromChain:        uint32(deposit.NetworkID),
		ToChain:          uint32(deposit.DestinationNetwork),
		BridgeToken:      deposit.OriginalAddress.Hex(),
		TokenAmount:      deposit.Amount.String(),
		Time:             uint64(deposit.Time.UnixMilli()),
		TxHash:           deposit.TxHash.String(),
		FromChainId:      GetChainIdByNetworkId(uint(deposit.NetworkID)),          // nolint:gosec
		ToChainId:        GetChainIdByNetworkId(uint(deposit.DestinationNetwork)), // nolint:gosec
		Id:               deposit.Id,
		Index:            uint64(deposit.DepositCount),
		Metadata:         "0x" + hex.EncodeToString(deposit.Metadata),
		DestAddr:         deposit.DestinationAddress.Hex(),
		LeafType:         uint32(deposit.LeafType),
		BlockNumber:      deposit.BlockNumber,
		DestContractAddr: deposit.DestContractAddress.Hex(),
		OriginalNetwork:  uint32(deposit.OriginalNetwork),
	}
}
