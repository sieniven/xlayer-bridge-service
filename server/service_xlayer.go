package server

import (
	"github.com/0xPolygonHermez/zkevm-bridge-service/utils"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/localcache"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/messagepush"
	"github.com/0xPolygonHermez/zkevm-bridge-service/xlayer/redisstorage"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
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
