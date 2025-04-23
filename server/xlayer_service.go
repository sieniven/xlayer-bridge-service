package server

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"

	"github.com/0xPolygonHermez/zkevm-bridge-service/bridgectrl/pb"
	"github.com/0xPolygonHermez/zkevm-bridge-service/log"
	"github.com/0xPolygonHermez/zkevm-bridge-service/utils"
	"github.com/0xPolygonHermez/zkevm-bridge-service/utils/gerror"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/estimatetime"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/localcache"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/messagepush"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/pushtask"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/redisstorage"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/tokenlogoinfo"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/utils/messagebridge"

	ctmtypes "github.com/0xPolygonHermez/zkevm-bridge-service/claimtxman/types"
	apolloconfig "github.com/0xPolygonHermez/zkevm-bridge-service/config/apollo_xlayer"
	xlUtils "github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/utils"
)

const (
	mtHeight           = 32 // For sending mtProof to bridge contract, it requires constant-sized array...
	defaultMinDuration = 1
)

var (
	minReadyTimeLimitForWaitClaimSeconds = apolloconfig.NewIntEntry[int64]("api.minReadyTimeLimitForWaitClaim", 24*60*1000) //nolint:gomnd
)

// Put in global variables (separate from struct fields)
var (
	redis               redisstorage.RedisStorage
	mainCoinsCache      localcache.MainCoinsCache
	messagePushProducer messagepush.KafkaProducer
	nodeClientsMap      map[uint]*utils.Client
	authMap             map[uint]*bind.TransactOpts
)

func (s *bridgeService) WithRedisStorage(storage redisstorage.RedisStorage) *bridgeService {
	redis = storage
	return s
}

func (s *bridgeService) WithMainCoinsCache(cache localcache.MainCoinsCache) *bridgeService {
	mainCoinsCache = cache
	return s
}

func (s *bridgeService) WithMessagePushProducer(producer messagepush.KafkaProducer) *bridgeService {
	messagePushProducer = producer
	return s
}

func (s *bridgeService) SetupL2Clients(
	l2NodeClients []*utils.Client,
	l2Auths []*bind.TransactOpts,
	networks []uint32,
) *bridgeService {
	for i, network := range networks {
		if i > 0 { // Why skip first index?
			id := uint(network) // nolint:gosec
			nodeClientsMap[id] = l2NodeClients[i-1]
			authMap[id] = l2Auths[i-1]
		}
	}
	return s
}

func (s *bridgeService) GetSmtProof(ctx context.Context, req *pb.GetSmtProofRequest) (*pb.CommonProofResponse, error) {
	globalExitRoot, merkleProof, rollupMerkleProof, err := s.GetClaimProof(uint32(req.Index), req.FromChain, nil) // nolint:gosec
	if err != nil || len(merkleProof) != len(rollupMerkleProof) {
		log.Errorf("GetSmtProof err[%v] merkleProofLen[%v] rollupMerkleProofLen[%v]", err, len(merkleProof), len(rollupMerkleProof))
		return &pb.CommonProofResponse{
			Code: uint32(pb.ErrorCode_ERROR_DEFAULT),
			Msg:  err.Error(),
		}, nil
	}
	var (
		proof       []string
		rollupProof []string
	)
	for i := 0; i < len(merkleProof); i++ {
		proof = append(proof, "0x"+hex.EncodeToString(merkleProof[i][:]))
		rollupProof = append(rollupProof, "0x"+hex.EncodeToString(rollupMerkleProof[i][:]))
	}

	return &pb.CommonProofResponse{
		Code: uint32(pb.ErrorCode_ERROR_OK),
		Data: &pb.ProofDetail{
			SmtProof:        proof,
			RollupSmtProof:  rollupProof,
			MainnetExitRoot: globalExitRoot.ExitRoots[0].Hex(),
			RollupExitRoot:  globalExitRoot.ExitRoots[1].Hex(),
		},
	}, nil
}

// GetCoinPrice returns the price for each coin symbol in the request
// Bridge rest API endpoint
func (s *bridgeService) GetCoinPrice(ctx context.Context, req *pb.GetCoinPriceRequest) (*pb.CommonCoinPricesResponse, error) {
	// convert inner chainId to standard chain id
	for _, symbol := range req.SymbolInfos {
		symbol.ChainId = xlUtils.GetStandardChainIdByInnerId(symbol.ChainId)
	}
	priceList, err := redis.GetCoinPrice(ctx, req.SymbolInfos)
	if err != nil {
		log.Errorf("get coin price from redis failed for symbol: %v, error: %v", req.SymbolInfos, err)
		return &pb.CommonCoinPricesResponse{
			Code: uint32(pb.ErrorCode_ERROR_DEFAULT),
			Msg:  gerror.ErrInternalErrorForRpcCall.Error(),
		}, nil
	}
	// convert standard chainId to ok inner chainId
	for _, priceInfo := range priceList {
		priceInfo.ChainId = xlUtils.GetInnerChainIdByStandardId(priceInfo.ChainId)
	}
	return &pb.CommonCoinPricesResponse{
		Code: uint32(pb.ErrorCode_ERROR_OK),
		Data: priceList,
	}, nil
}

// GetMainCoins returns the info of the main coins in a network
// Bridge rest API endpoint
func (s *bridgeService) GetMainCoins(ctx context.Context, req *pb.GetMainCoinsRequest) (*pb.CommonCoinsResponse, error) {
	coins, err := mainCoinsCache.GetMainCoinsByNetwork(ctx, req.NetworkId)
	if err != nil {
		log.Errorf("get main coins from cache failed for net: %v, error: %v", req.NetworkId, err)
		return &pb.CommonCoinsResponse{
			Code: uint32(pb.ErrorCode_ERROR_DEFAULT),
			Msg:  gerror.ErrInternalErrorForRpcCall.Error(),
		}, nil
	}
	// use ok inner chain id
	for _, coinInfo := range coins {
		coinInfo.ChainId = xlUtils.GetInnerChainIdByStandardId(coinInfo.ChainId)
	}
	return &pb.CommonCoinsResponse{
		Code: uint32(pb.ErrorCode_ERROR_OK),
		Data: coins,
	}, nil
}

// GetPendingTransactions returns the pending transactions of an account
// Bridge rest API endpoint
func (s *bridgeService) GetPendingTransactions(ctx context.Context, req *pb.GetPendingTransactionsRequest) (*pb.CommonTransactionsResponse, error) {
	limit := req.Limit
	if limit == 0 {
		limit = s.defaultPageLimit
	}
	if limit > s.maxPageLimit {
		limit = s.maxPageLimit
	}

	deposits, err := s.storage.GetPendingTransactions(ctx, req.DestAddr, uint(limit+1), uint(req.Offset), messagebridge.GetContractAddressList(), nil)
	if err != nil {
		log.Errorf("get pending tx failed for address: %v, limit: %v, offset: %v, error: %v", req.DestAddr, limit, req.Offset, err)
		return &pb.CommonTransactionsResponse{
			Code: uint32(pb.ErrorCode_ERROR_DEFAULT),
			Msg:  gerror.ErrInternalErrorForRpcCall.Error(),
		}, nil
	}

	hasNext := len(deposits) > int(limit)
	if hasNext {
		deposits = deposits[:limit]
	}

	l1BlockNum, _ := redis.GetL1BlockNum(ctx)
	l2CommitBlockNum, _ := redis.GetCommitMaxBlockNum(ctx)
	l2AvgCommitDuration := pushtask.GetAvgCommitDuration(ctx, redis)
	l2AvgVerifyDuration := pushtask.GetAvgVerifyDuration(ctx, redis)
	currTime := time.Now()

	var pbTransactions []*pb.Transaction
	transactionMap := make(map[string][]*pb.Transaction)
	for _, deposit := range deposits {
		// replace contract address to real token address
		messagebridge.ReplaceDepositInfo(deposit, false)
		transaction := xlUtils.EthermanDepositToPbTransaction(deposit)
		transaction.EstimateTime = estimatetime.GetDefaultCalculator().Get(uint(deposit.NetworkID)) // nolint:gosec
		transaction.Status = uint32(pb.TransactionStatus_TX_CREATED)
		transaction.GlobalIndex = s.getGlobalIndex(deposit).String()
		if deposit.ReadyForClaim {
			transaction.Status = uint32(pb.TransactionStatus_TX_PENDING_USER_CLAIM)
			// For L1->L2, if backend is trying to auto-claim, set the status to 0 to block the user from manual-claim
			// When the auto-claim failed, set status to 1 to let the user claim manually through front-end
			if deposit.NetworkID == 0 {
				mTx, err := s.storage.GetClaimTxById(ctx, uint(deposit.DepositCount), nil)
				if err == nil && mTx.Status != ctmtypes.MonitoredTxStatusFailed {
					transaction.Status = uint32(pb.TransactionStatus_TX_PENDING_AUTO_CLAIM)
				}
			}
		} else {
			// For L1->L2, when ready_for_claim is false, but there have been more than 64 block confirmations,
			// should also display the status as "L2 executing" (pending auto claim)
			if deposit.NetworkID == 0 {
				if l1BlockNum-deposit.BlockNumber >= xlUtils.L1TargetBlockConfirmations.Get() {
					transaction.Status = uint32(pb.TransactionStatus_TX_PENDING_AUTO_CLAIM)
				}
			} else {
				if l2CommitBlockNum >= deposit.BlockNumber {
					transaction.Status = uint32(pb.TransactionStatus_TX_PENDING_VERIFICATION)
				}
				s.setDurationForL2Deposit(ctx, l2AvgCommitDuration, l2AvgVerifyDuration, currTime, transaction, deposit.Time)
			}
		}
		// chain id convert to ok inner chain id
		if transaction.FromChainId != 0 {
			transaction.FromChainId = uint32(xlUtils.GetInnerChainIdByStandardId(uint64(transaction.FromChainId)))
		}
		if transaction.ToChainId != 0 {
			transaction.ToChainId = uint32(xlUtils.GetInnerChainIdByStandardId(uint64(transaction.ToChainId)))
		}
		pbTransactions = append(pbTransactions, transaction)
		chainId := xlUtils.GetChainIdByNetworkId(uint(deposit.OriginalNetwork)) // nolint:gosec
		logoCacheKey := tokenlogoinfo.GetTokenLogoMapKey(transaction.GetBridgeToken(), chainId)
		transactionMap[logoCacheKey] = append(transactionMap[logoCacheKey], transaction)
	}
	tokenlogoinfo.FillLogoInfos(ctx, redis, transactionMap)
	return &pb.CommonTransactionsResponse{
		Code: uint32(pb.ErrorCode_ERROR_OK),
		Data: &pb.TransactionDetail{HasNext: hasNext, Transactions: pbTransactions},
	}, nil
}
