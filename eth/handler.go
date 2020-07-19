// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/clique"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/eth/fetcher"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	softResponseLimit = 2 * 1024 * 1024 // Target maximum size of returned blocks, headers or node data.
	estHeaderRlpSize  = 500             // Approximate size of an RLP encoded block header

	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096

	// minimim number of peers to broadcast new blocks to
	minBroadcastPeers = 4

	// minimun number of peers to request data
	minRequestPeers = 2
)

var (
	daoChallengeTimeout = 15 * time.Second // Time allowance for a node to reply to the DAO handshake challenge
)

// errIncompatibleConfig is returned if the requested protocols and configs are
// not compatible (low protocol version restrictions and high requirements).
var errIncompatibleConfig = errors.New("incompatible configuration")

func errResp(code errCode, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

type ProtocolManager struct {
	numShard      uint64
	myshard       uint64
	networkID     uint64
	stateGasLimit uint64
	stateGasPrice *big.Int
	refAddress    common.Address

	fastSync  uint32 // Flag whether fast sync is enabled (gets disabled if we already have blocks)
	rFastSync uint32 // Flag whether reference chain fast sync enabled or not.
	acceptTxs uint32 // Flag whether we're considered synchronised (enables transaction processing)

	txpool      txPool
	blockchain  *core.BlockChain
	refchain    *core.BlockChain
	chainconfig *params.ChainConfig
	maxPeers    int

	logdir          string
	downloader      *downloader.Downloader
	rDownloader     *downloader.Downloader
	fetcher         *fetcher.Fetcher
	cousinPeers     map[uint64]*peerSet
	cousinPeerLock  sync.RWMutex
	shardAddMap     map[uint64]*big.Int
	shardAddMapLock sync.RWMutex

	SubProtocols []p2p.Protocol

	eventMux      *event.TypeMux
	rEventMux     *event.TypeMux
	txsCh         chan core.NewTxsEvent
	txsSub        event.Subscription
	minedBlockSub *event.TypeMuxSubscription
	refBlockSub   *event.TypeMuxSubscription

	// channels for fetcher, syncer, txsyncLoop
	newPeerCh   chan *peer
	txsyncCh    chan *txsync
	quitSync    chan struct{}
	rQuitSync   chan struct{}
	noMorePeers chan struct{}

	// wait group is used for graceful shutdowns during downloading
	// and processing
	wg sync.WaitGroup

	raftMode bool
	engine   consensus.Engine
}

// NewProtocolManager returns a new Ethereum sub protocol manager. The Ethereum sub protocol manages peers capable
// with the Ethereum network.
func NewProtocolManager(config *params.ChainConfig, mode downloader.SyncMode, numShard, myshard, networkID uint64, mux, rmux *event.TypeMux, txpool txPool, engine consensus.Engine, blockchain, refchain *core.BlockChain, refAddress common.Address, shardAddMap map[uint64]*big.Int, chaindb, refdb ethdb.Database, raftMode bool, logdir string) (*ProtocolManager, error) {
	// Create the protocol manager with the base fields
	manager := &ProtocolManager{
		numShard:      numShard,
		myshard:       myshard,
		stateGasLimit: uint64(80000),
		stateGasPrice: big.NewInt(0),
		networkID:     networkID,
		eventMux:      mux,
		rEventMux:     rmux,
		txpool:        txpool,
		blockchain:    blockchain,
		refchain:      refchain,
		refAddress:    refAddress,
		chainconfig:   config,
		cousinPeers:   make(map[uint64]*peerSet),
		shardAddMap:   make(map[uint64]*big.Int),
		newPeerCh:     make(chan *peer),
		noMorePeers:   make(chan struct{}),
		txsyncCh:      make(chan *txsync),
		quitSync:      make(chan struct{}),
		rQuitSync:     make(chan struct{}),
		raftMode:      raftMode,
		engine:        engine,
		logdir:        logdir,
	}

	// manager.shardAddMapLock.Lock()
	for shard, addr := range shardAddMap {
		manager.shardAddMap[shard] = addr
	}
	// manager.shardAddMapLock.Unlock()

	if handler, ok := manager.engine.(consensus.Handler); ok {
		handler.SetBroadcaster(manager)
	}
	// Figure out whether to allow fast sync or not
	if mode == downloader.FastSync && blockchain.CurrentBlock().NumberU64() > 0 {
		log.Warn("Blockchain not empty, fast sync disabled")
		mode = downloader.FullSync
	}
	if mode == downloader.FastSync {
		manager.fastSync = uint32(1)
		manager.rFastSync = uint32(1)
	}
	protocol := engine.Protocol()
	// Initiate a sub-protocol for every implemented version we can handle
	manager.SubProtocols = make([]p2p.Protocol, 0, len(protocol.Versions))
	for i, version := range protocol.Versions {
		// Skip protocol version if incompatible with the mode of operation
		if mode == downloader.FastSync && version < eth63 {
			continue
		}
		// Compatible; initialise the sub-protocol
		version := version // Closure for the run
		manager.SubProtocols = append(manager.SubProtocols, p2p.Protocol{
			Name:    protocol.Name,
			Version: version,
			Length:  protocol.Lengths[i],
			Run: func(p *p2p.Peer, shard uint64, rw p2p.MsgReadWriter) error {
				peer := manager.newPeer(shard, int(version), p, rw)
				select {
				case manager.newPeerCh <- peer:
					manager.wg.Add(1)
					defer manager.wg.Done()
					return manager.handle(peer)
				case <-manager.quitSync:
					return p2p.DiscQuitting
				}
			},
			NodeInfo: func() interface{} {
				return manager.NodeInfo()
			},
			CousinPeerInfo: func(id enode.ID) interface{} {
				manager.cousinPeerLock.RLock()
				defer manager.cousinPeerLock.RUnlock()
				for _, peers := range manager.cousinPeers {
					if p := peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
						return p.Info()
					}
				}
				return nil
			},
		})
	}
	if len(manager.SubProtocols) == 0 {
		return nil, errIncompatibleConfig
	}
	// Construct the different synchronisation mechanisms
	manager.downloader = downloader.New(false, mode, chaindb, manager.eventMux, blockchain, nil, manager.removePeer, myshard)
	manager.rDownloader = downloader.New(true, mode, refdb, manager.rEventMux, refchain, nil, manager.removePeer, myshard)

	validator := func(ref bool, header *types.Header) error {
		if ref {
			return engine.VerifyHeader(refchain, header, true)
		}
		return engine.VerifyHeader(blockchain, header, true)
	}
	heighter := func(ref bool) uint64 {
		if ref {
			return refchain.CurrentBlock().NumberU64()
		}
		return blockchain.CurrentBlock().NumberU64()
	}
	inserter := func(ref bool, blocks types.Blocks) (int, error) {
		// If fast sync is running, deny importing weird blocks
		if ref {
			if atomic.LoadUint32(&manager.rFastSync) == 1 {
				log.Warn("Discarded bad propagated reference block", "number", blocks[0].Number(), "hash", blocks[0].Hash())
				return 0, nil
			}
			return manager.refchain.InsertChain(blocks)
		} else {
			if atomic.LoadUint32(&manager.fastSync) == 1 {
				log.Warn("Discarded bad propagated block", "number", blocks[0].Number(), "hash", blocks[0].Hash())
				return 0, nil
			}
			atomic.StoreUint32(&manager.acceptTxs, 1) // Mark initial sync done on any fetcher import
			return manager.blockchain.InsertChain(blocks)
		}
	}
	manager.fetcher = fetcher.New(blockchain.GetBlockByHash, refchain.GetBlockByHash, validator, manager.BroadcastBlock, heighter, inserter, manager.removePeer, manager.myshard)

	return manager, nil
}

// GetShardID returns shard number of a peer.
func (pm *ProtocolManager) GetShardID(id string) uint64 {
	pm.cousinPeerLock.RLock()
	defer pm.cousinPeerLock.RUnlock()
	for shard, pset := range pm.cousinPeers {
		for peerID := range pset.peers {
			if id == peerID {
				return shard
			}
		}
	}
	return 4096
}

func (pm *ProtocolManager) removePeer(id string) {
	// Short circuit if the peer was already removed
	// @sourav, todo: here we are removing from our shard. Fix this
	shard := pm.GetShardID(id)
	pm.cousinPeerLock.RLock()
	if pm.cousinPeers[shard] == nil {
		pm.cousinPeers[shard] = newPeerSet()
	}
	peer := pm.cousinPeers[shard].Peer(id)
	if peer == nil {
		pm.cousinPeerLock.RUnlock()
		return
	}
	pm.cousinPeerLock.RUnlock()
	log.Debug("Removing Ethereum peer", "peer", id)

	// Unregister the peer from the downloader and Ethereum peer set
	pm.downloader.UnregisterPeer(id)
	pm.rDownloader.UnregisterPeer(id)
	// Todo rdwd
	pm.cousinPeerLock.Lock()
	if err := pm.cousinPeers[shard].Unregister(id); err != nil {
		log.Error("Peer removal failed", "peer", id, "err", err)
	}
	pm.cousinPeerLock.Unlock()
	// Hard disconnect at the networking layer
	if peer != nil {
		peer.Peer.Disconnect(p2p.DiscUselessPeer)
	}
}

func (pm *ProtocolManager) Start(maxPeers int) {
	pm.maxPeers = maxPeers

	// broadcast transactions
	pm.txsCh = make(chan core.NewTxsEvent, txChanSize)
	pm.txsSub = pm.txpool.SubscribeNewTxsEvent(pm.txsCh)
	go pm.txBroadcastLoop()

	if !pm.raftMode {
		// broadcast mined blocks
		pm.minedBlockSub = pm.eventMux.Subscribe(core.NewMinedBlockEvent{})
		pm.refBlockSub = pm.eventMux.Subscribe(core.NewRefBlockEvent{})
		go pm.minedBroadcastLoop()
		go pm.fetchForeignDataLoop()
	} else {
		// We set this immediately in raft mode to make sure the miner never drops
		// incoming txes. Raft mode doesn't use the fetcher or downloader, and so
		// this would never be set otherwise.
		atomic.StoreUint32(&pm.acceptTxs, 1)
	}

	// start sync handlers
	go pm.syncer()
	go pm.txsyncLoop()
}

func (pm *ProtocolManager) Stop() {
	log.Info("Stopping Ethereum protocol")

	pm.txsSub.Unsubscribe() // quits txBroadcastLoop
	if !pm.raftMode {
		pm.minedBlockSub.Unsubscribe() // quits blockBroadcastLoop
	}

	// Quit the sync loop.
	// After this send has completed, no new peers will be accepted.
	pm.noMorePeers <- struct{}{}

	// Quit fetcher, txsyncLoop.
	close(pm.quitSync)

	// Disconnect existing sessions.
	// This also closes the gate for any new registrations on the peer set.
	// sessions which are already established but not added to pm.peers yet
	// will exit when they try to register.
	pm.cousinPeerLock.Lock()
	for _, peers := range pm.cousinPeers {
		peers.Close()
	}
	pm.cousinPeerLock.Unlock()

	// Wait for all peer handler goroutines and the loops to come down.
	pm.wg.Wait()

	log.Info("Ethereum protocol stopped")
}

func (pm *ProtocolManager) newPeer(shard uint64, pv int, p *p2p.Peer, rw p2p.MsgReadWriter) *peer {
	return newPeer(shard, pv, p, newMeteredMsgWriter(rw))
}

// handle is the callback invoked to manage the life cycle of an eth peer. When
// this function terminates, the peer is disconnected.
func (pm *ProtocolManager) handle(p *peer) error {

	peerShard := p.Shard()
	// For each shard, put a threshold on the number of peers.
	pm.cousinPeerLock.Lock()
	if pm.cousinPeers[peerShard] == nil {
		pm.cousinPeers[peerShard] = newPeerSet()
	}
	pm.cousinPeerLock.Unlock()
	pm.cousinPeerLock.RLock()
	if pm.cousinPeers[peerShard].Len() >= pm.maxPeers && !p.Peer.Info().Network.Trusted {
		pm.cousinPeerLock.RUnlock()
		return p2p.DiscTooManyCousinPeers
	}
	pm.cousinPeerLock.RUnlock()
	p.Log().Debug("Peer connected", "shard", peerShard, "myshard", pm.myshard, "name", p.Name())

	// Execute the Ethereum handshake
	var (
		genesis = pm.blockchain.Genesis()
		head    = pm.blockchain.CurrentHeader()
		rHead   = pm.refchain.CurrentHeader()
		hash    = head.Hash()
		rHash   = rHead.Hash()
		number  = head.Number.Uint64()
		rNumber = rHead.Number.Uint64()
		td      = pm.blockchain.GetTd(hash, number)
		rTd     = pm.refchain.GetTd(rHash, rNumber)
	)
	if err := p.Handshake(pm.myshard, pm.networkID, td, rTd, hash, rHash, genesis.Hash()); err != nil {
		p.Log().Debug("Ethereum handshake failed", "err", err)
		return err
	}
	if rw, ok := p.rw.(*meteredMsgReadWriter); ok {
		rw.Init(p.version)
	}
	// Register the peer locally
	pm.cousinPeerLock.Lock()
	if err := pm.cousinPeers[peerShard].Register(p); err != nil {
		p.Log().Error("Ethereum peer registration failed", "err", err)
		pm.cousinPeerLock.Unlock()
		return err
	}
	pm.cousinPeerLock.Unlock()
	defer pm.removePeer(p.id)

	// Register the peer in the downloader. If the downloader considers it banned, we disconnect
	if err := pm.downloader.RegisterPeer(p.id, p.version, p); err != nil {
		return err
	}
	if err := pm.rDownloader.RegisterPeer(p.id, p.version, p); err != nil {
		return err
	}
	// Propagate existing transactions. new transactions appearing
	// after this will be sent via broadcasts.

	// @sourav: todo, if the peer is of different shard, avoid synchrosnising.

	if peerShard == pm.myshard {
		pm.syncTransactions(p)
	}

	// If we're DAO hard-fork aware, validate any remote peer with regard to the hard-fork
	if daoBlock := pm.chainconfig.DAOForkBlock; daoBlock != nil && (peerShard == pm.myshard || peerShard == uint64(0)) {
		// Request the peer's DAO fork header for extra-data validation
		if err := p.RequestHeadersByNumber(peerShard, daoBlock.Uint64(), 1, 0, false); err != nil {
			return err
		}
		// Start a timer to disconnect if the peer doesn't reply in time
		p.forkDrop = time.AfterFunc(daoChallengeTimeout, func() {
			p.Log().Debug("Timed out DAO fork-check, dropping")
			pm.removePeer(p.id)
		})
		// Make sure it's cleaned up if the peer dies off
		defer func() {
			if p.forkDrop != nil {
				p.forkDrop.Stop()
				p.forkDrop = nil
			}
		}()
	}
	// main loop. handle incoming messages.
	for {
		if err := pm.handleMsg(p); err != nil {
			p.Log().Warn("Ethereum message handling failed", "err", err)
			return err
		}
	}
}

// handleMsg is invoked whenever an inbound message is received from a remote
// peer. The remote connection is torn down upon returning any error.
func (pm *ProtocolManager) handleMsg(p *peer) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
	msg, err := p.rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Size > ProtocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	defer msg.Discard()

	if pm.raftMode {
		if msg.Code != TxMsg &&
			msg.Code != GetBlockHeadersMsg && msg.Code != BlockHeadersMsg &&
			msg.Code != GetBlockBodiesMsg && msg.Code != BlockBodiesMsg {

			log.Info("raft: ignoring message", "code", msg.Code)

			return nil
		}
	} else if handler, ok := pm.engine.(consensus.Handler); ok {
		pubKey := p.Node().Pubkey()
		addr := crypto.PubkeyToAddress(*pubKey)
		handled, err := handler.HandleMsg(addr, msg)
		if handled {
			return err
		}
	}

	// Handle the message depending on its contents
	switch {
	case msg.Code == StatusMsg:
		// Status messages should never arrive after the handshake
		return errResp(ErrExtraStatusMsg, "uncontrolled status message")

	// Block header query, collect the requested headers and reply
	case msg.Code == GetBlockHeadersMsg:
		// Decode the complex header query
		var query getBlockHeadersData
		if err := msg.Decode(&query); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		hashMode := query.Origin.Hash != (common.Hash{})
		ref := query.Shard == uint64(0) && pm.myshard > uint64(0)
		first := true
		maxNonCanonical := uint64(100)

		// Gather headers until the fetch or network limits is reached
		var (
			bytes   common.StorageSize
			headers []*types.Header
			unknown bool
			origin  *types.Header
		)

		for !unknown && len(headers) < int(query.Amount) && bytes < softResponseLimit && len(headers) < downloader.MaxHeaderFetch {
			// Retrieve the next header satisfying the query
			if hashMode {
				if ref {
					if first {
						first = false
						origin = pm.refchain.GetHeaderByHash(query.Origin.Hash)
						if origin != nil {
							query.Origin.Number = origin.Number.Uint64()
						}
					} else {
						origin = pm.refchain.GetHeader(query.Origin.Hash, query.Origin.Number)
					}
				} else {
					if first {
						first = false
						origin = pm.blockchain.GetHeaderByHash(query.Origin.Hash)
						if origin != nil {
							query.Origin.Number = origin.Number.Uint64()
						}
					} else {
						origin = pm.blockchain.GetHeader(query.Origin.Hash, query.Origin.Number)
					}
				}
			} else {
				if ref {
					origin = pm.refchain.GetHeaderByNumber(query.Origin.Number)
				} else {
					origin = pm.blockchain.GetHeaderByNumber(query.Origin.Number)
				}
			}

			if origin == nil {
				break
			}
			headers = append(headers, origin)
			bytes += estHeaderRlpSize

			// Advance to the next header of the query
			switch {
			case hashMode && query.Reverse:
				// Hash based traversal towards the genesis block
				ancestor := query.Skip + 1
				if ancestor == 0 {
					unknown = true
				} else {
					if ref {
						query.Origin.Hash, query.Origin.Number = pm.refchain.GetAncestor(query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical)
					} else {
						query.Origin.Hash, query.Origin.Number = pm.blockchain.GetAncestor(query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical)
					}
					unknown = (query.Origin.Hash == common.Hash{})
				}
			case hashMode && !query.Reverse:
				// Hash based traversal towards the leaf block
				var (
					current = origin.Number.Uint64()
					next    = current + query.Skip + 1
				)

				if next <= current {
					infos, _ := json.MarshalIndent(p.Peer.Info(), "", "  ")
					p.Log().Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next, "attacker", infos)
					unknown = true
				} else {
					if ref {
						if header := pm.refchain.GetHeaderByNumber(next); header != nil {
							nextHash := header.Hash()
							expOldHash, _ := pm.refchain.GetAncestor(nextHash, next, query.Skip+1, &maxNonCanonical)
							if expOldHash == query.Origin.Hash {
								query.Origin.Hash, query.Origin.Number = nextHash, next
							} else {
								unknown = true
							}
						} else {
							unknown = true
						}
					} else {
						if header := pm.blockchain.GetHeaderByNumber(next); header != nil {
							nextHash := header.Hash()
							expOldHash, _ := pm.blockchain.GetAncestor(nextHash, next, query.Skip+1, &maxNonCanonical)
							if expOldHash == query.Origin.Hash {
								query.Origin.Hash, query.Origin.Number = nextHash, next
							} else {
								unknown = true
							}
						} else {
							unknown = true
						}
					}
				}
			case query.Reverse:
				// Number based traversal towards the genesis block
				if query.Origin.Number >= query.Skip+1 {
					query.Origin.Number -= query.Skip + 1
				} else {
					unknown = true
				}

			case !query.Reverse:
				// Number based traversal towards the leaf block
				query.Origin.Number += query.Skip + 1
			}
		}
		return p.SendBlockHeaders(headers)

	case msg.Code == BlockHeadersMsg:
		// @sourav, todo: first check whether the message is for
		// reference chain or for mainchain. Add the block in
		// appropriate position.
		// A batch of headers arrived to one of our previous requests
		var headers []*types.Header
		if err := msg.Decode(&headers); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// If no headers were received, but we're expending a DAO fork check, maybe it's that
		if len(headers) == 0 && p.forkDrop != nil {
			// Possibly an empty reply to the fork header checks, sanity check TDs
			verifyDAO := true

			// If we already have a DAO header, we can check the peer's TD against it. If
			// the peer's ahead of this, it too must have a reply to the DAO check
			if daoHeader := pm.blockchain.GetHeaderByNumber(pm.chainconfig.DAOForkBlock.Uint64()); daoHeader != nil {
				if _, td := p.Head(false); td.Cmp(pm.blockchain.GetTd(daoHeader.Hash(), daoHeader.Number.Uint64())) >= 0 {
					verifyDAO = false
				}
			}

			if daoHeader := pm.refchain.GetHeaderByNumber(pm.chainconfig.DAOForkBlock.Uint64()); daoHeader != nil {
				if _, td := p.Head(true); td.Cmp(pm.refchain.GetTd(daoHeader.Hash(), daoHeader.Number.Uint64())) >= 0 {
					verifyDAO = false
				}
			}
			// If we're seemingly on the same chain, disable the drop timer
			if verifyDAO {
				p.Log().Debug("Seems to be on the same side of the DAO fork")
				p.forkDrop.Stop()
				p.forkDrop = nil
				return nil
			}
		}
		// Filter out any explicitly requested headers, deliver the rest to the downloader
		filter := len(headers) == 1

		if filter {
			// If it's a potential DAO fork check, validate against the rules
			if p.forkDrop != nil && pm.chainconfig.DAOForkBlock.Cmp(headers[0].Number) == 0 {
				// Disable the fork drop timer
				p.forkDrop.Stop()
				p.forkDrop = nil

				// Validate the header and either drop the peer or continue
				if err := misc.VerifyDAOHeaderExtraData(pm.chainconfig, headers[0]); err != nil {
					p.Log().Debug("Verified to be on the other side of the DAO fork, dropping")
					return err
				}
				p.Log().Debug("Verified to be on the same side of the DAO fork")
				return nil
			}
			// Irrelevant of the fork checks, send the header to the fetcher just in case
			headers = pm.fetcher.FilterHeaders(p.id, headers, time.Now())
		}
		if len(headers) > 0 || !filter {
			if err := pm.rDownloader.DeliverHeaders(p.id, headers); err != nil {
				log.Debug("Failed to deliver headers", "err", err)
			}

			if err := pm.downloader.DeliverHeaders(p.id, headers); err != nil {
				log.Debug("Failed to deliver headers", "err", err)
			}
		}

	case msg.Code == GetBlockBodiesMsg:
		// Decode the retrieval message
		var request getBlockBodiesData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		shard := request.Shard
		hashes := request.Hashes
		// Gather blocks until the fetch or network limits is reached
		var (
			bytes  int
			bodies []rlp.RawValue
		)
		ref := shard == uint64(0) && pm.myshard > uint64(0)
		for _, hash := range hashes {
			if ref {
				if data := pm.refchain.GetBodyRLP(hash); len(data) != 0 {
					bodies = append(bodies, data)
					bytes += len(data)
				}
			} else {
				if data := pm.blockchain.GetBodyRLP(hash); len(data) != 0 {
					bodies = append(bodies, data)
					bytes += len(data)
				}
			}
		}
		return p.SendBlockBodiesRLP(bodies)

	case msg.Code == BlockBodiesMsg:
		// A batch of block bodies arrived to one of our previous requests
		var request blockBodiesData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver them all to the downloader for queuing
		transactions := make([][]*types.Transaction, len(request))
		uncles := make([][]*types.Header, len(request))

		for i, body := range request {
			transactions[i] = body.Transactions
			uncles[i] = body.Uncles
		}
		// Filter out any explicitly requested bodies, deliver the rest to the downloader
		filter := len(transactions) > 0 || len(uncles) > 0
		if filter {
			transactions, uncles = pm.fetcher.FilterBodies(p.id, transactions, uncles, time.Now())
		}
		if len(transactions) > 0 || len(uncles) > 0 || !filter {
			if pm.myshard > uint64(0) {
				err := pm.rDownloader.DeliverBodies(p.id, transactions, uncles)
				if err != nil {
					log.Debug("Failed to deliver bodies", "err", err)
				}
			}
			err = pm.downloader.DeliverBodies(p.id, transactions, uncles)
			if err != nil {
				log.Debug("Failed to deliver bodies", "err", err)
			}
		}

	case p.version >= eth63 && msg.Code == GetNodeDataMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather state data until the fetch or network limits is reached
		var (
			hash  common.Hash
			bytes int
			data  [][]byte
		)
		for bytes < softResponseLimit && len(data) < downloader.MaxStateFetch {
			// Retrieve the hash of the next state entry
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested state entry, stopping if enough was found
			if entry, err := pm.blockchain.TrieNode(hash); err == nil {
				data = append(data, entry)
				bytes += len(entry)
			} else if entry, err = pm.refchain.TrieNode(hash); err == nil {
				data = append(data, entry)
				bytes += len(entry)
			}
		}
		return p.SendNodeData(data)

	case p.version >= eth63 && msg.Code == NodeDataMsg:
		// A batch of node state data arrived to one of our previous requests
		var data [][]byte
		if err := msg.Decode(&data); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver all to the downloader
		if err := pm.downloader.DeliverNodeData(p.id, data); err != nil {
			log.Debug("Failed to deliver node state data", "err", err)
		}

		// Deliver all to the downloader
		if err := pm.rDownloader.DeliverNodeData(p.id, data); err != nil {
			log.Debug("Failed to deliver reference node state data", "err", err)
		}

	case p.version >= eth63 && msg.Code == GetReceiptsMsg:
		var request getReceiptsData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}

		hashes := request.Hashes
		shard := request.Shard

		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather state data until the fetch or network limits is reached
		var (
			bytes    int
			receipts []rlp.RawValue
			results  types.Receipts
		)

		ref := shard == uint64(0) && pm.myshard > uint64(0)
		for _, hash := range hashes {
			if ref {
				results = pm.refchain.GetReceiptsByHash(hash)
				if results == nil {
					if header := pm.refchain.GetHeaderByHash(hash); header == nil || header.ReceiptHash != types.EmptyRootHash {
						continue
					}
				}
			} else {
				results = pm.blockchain.GetReceiptsByHash(hash)
				if results == nil {
					if header := pm.blockchain.GetHeaderByHash(hash); header == nil || header.ReceiptHash != types.EmptyRootHash {
						continue
					}
				}
			}

			if encoded, err := rlp.EncodeToBytes(results); err != nil {
				log.Error("Failed to encode receipt", "err", err)
			} else {
				receipts = append(receipts, encoded)
				bytes += len(encoded)
			}
		}
		return p.SendReceiptsRLP(receipts)

	case p.version >= eth63 && msg.Code == ReceiptsMsg:
		// A batch of receipts arrived to one of our previous requests
		var receipts [][]*types.Receipt
		if err := msg.Decode(&receipts); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver all to the downloader
		if err := pm.downloader.DeliverReceipts(p.id, receipts); err != nil {
			log.Debug("Failed to deliver receipts", "err", err)
		}

		// Deliver all to the downloader
		if err = pm.rDownloader.DeliverReceipts(p.id, receipts); err != nil {
			log.Debug("Failed to deliver receipts", "err", err)
		}

	case msg.Code == NewBlockHashesMsg:
		var announces newBlockHashesData
		if err := msg.Decode(&announces); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		// Mark the hashes as present at the remote node
		for _, block := range announces {
			p.MarkBlock(block.Ref, block.Hash)
		}
		// Schedule all the unknown hashes for retrieval
		unknown := make(newBlockHashesData, 0, len(announces))
		for _, block := range announces {
			if block.Ref {
				if !pm.refchain.HasBlock(block.Hash, block.Number) {
					unknown = append(unknown, block)
				}
			} else {
				if !pm.blockchain.HasBlock(block.Hash, block.Number) {
					unknown = append(unknown, block)
				}
			}
		}
		for _, block := range unknown {
			pm.fetcher.Notify(p.id, block.Ref, block.Hash, block.Number, time.Now(), p.RequestOneHeader, p.RequestBodies)
		}

	case msg.Code == NewBlockMsg:
		// Retrieve and decode the propagated block
		var request newBlockData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		request.Block.ReceivedAt = msg.ReceivedAt
		request.Block.ReceivedFrom = p
		ref := (request.Block.Shard() == uint64(0) && pm.myshard > uint64(0))

		// Mark the peer as owning the block and schedule it for import
		p.MarkBlock(ref, request.Block.Hash())
		pm.fetcher.Enqueue(p.id, request.Block)

		// Assuming the block is importable by the peer, but possibly not yet done so,
		// calculate the head hash and TD that the peer truly must have.
		var (
			trueHead = request.Block.ParentHash()
			trueTD   = new(big.Int).Sub(request.TD, request.Block.Difficulty())
		)
		// Update the peer's total difficulty if better than the previous
		if _, td := p.Head(ref); trueTD.Cmp(td) > 0 {
			p.SetHead(ref, trueHead, trueTD)

			// Schedule a sync if above ours. Note, this will not fire a sync for a gap of
			// a singe block (as the true TD is below the propagated block), however this
			// scenario should easily be covered by the fetcher.
			if ref {
				currentBlock := pm.refchain.CurrentBlock()
				currTD := pm.refchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64())
				if currTD != nil {
					if trueTD.Cmp(currTD) > 0 && p.Shard() == uint64(0) {
						go pm.synchronise(ref, p)
					}
				}
			} else {
				currentBlock := pm.blockchain.CurrentBlock()
				currTD := pm.blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64())
				if currTD != nil {
					if trueTD.Cmp(currTD) > 0 {
						go pm.synchronise(ref, p)
					}
				}
			}
		}

	case msg.Code == TxMsg:
		// Transactions arrived, make sure we have a valid and fresh chain to handle them
		if atomic.LoadUint32(&pm.acceptTxs) == 0 {
			break
		}
		// Transactions can be processed, parse all of them and deliver to the pool
		var txs []*types.Transaction
		if err := msg.Decode(&txs); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		for i, tx := range txs {
			// Validate and mark the remote transaction
			if tx == nil {
				return errResp(ErrDecode, "transaction %d is nil", i)
			}
			p.MarkTransaction(tx.Hash())
		}
		pm.txpool.AddRemotes(txs)

	case msg.Code == GetStateDataMsg:
		var request getStateData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		refNum := request.RefNum
		root := request.Root
		count := request.Count
		log.Debug("Received request from", "pshard", p.Shard(), "num", refNum, "root", root)

		results := pm.blockchain.StateData(root, request.Keys)
		if results != nil {
			err := p.SendDataResponse(refNum, count, root, results)
			if err != nil {
				log.Error("Error in send state data response!", "error", err)
			}
		}

	case msg.Code == StateDataMsg:
		var request stateData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		refNum := request.RefNum
		root := request.Root
		vals := request.Vals
		log.Debug("Received response from", "pshard", p.Shard(), "num", refNum, "root", root)

		dresp := pm.logdir + "dresp" // + strconv.Itoa(int(pm.myshard))
		drespf, err := os.OpenFile(dresp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Error("Can't open dresp file", "error", err)
		}
		fmt.Fprintln(drespf, refNum, p.Shard(), len(vals), root.Hex(), p.ID(), time.Now().Unix())
		drespf.Close()
		go pm.AddFetchedData(refNum, p.Shard(), vals)

	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
}

func (pm *ProtocolManager) Enqueue(id string, block *types.Block) {
	pm.fetcher.Enqueue(id, block)
}

// AddFetchedData fills the data cache with data downloaded from peer
func (pm *ProtocolManager) AddFetchedData(refNum, pshard uint64, vals []*types.KeyVal) {
	dc, _ := pm.blockchain.Dc(refNum)
	if dc != nil {
		dc.DataCacheMu.RLock()
		if !dc.ShardStatus[pshard] {
			dc.DataCacheMu.RUnlock()
			dc.AddData(pshard, vals)

			dc.DataCacheMu.RLock()
			status := dc.Status
			dc.DataCacheMu.RUnlock()
			log.Debug("Foreign Data added", "refnum", refNum, "shard", pshard, "status", status)
			if status {
				go pm.blockchain.PostForeignDataEvent(refNum)
			}
		} else {
			dc.DataCacheMu.RUnlock()
		}
	}
}

// FetchData requests data from appropriate shard
func (pm *ProtocolManager) FetchData(start, end uint64) {
	for refNum := start; refNum <= end; refNum++ {
		dc, status := pm.blockchain.Dc(refNum)
		if !status {
			for shard := range dc.ShardStatus {
				if shard != pm.myshard {
					dc.DataCacheMu.RLock()
					root := dc.Commits[shard].StateRoot
					dc.DataCacheMu.RUnlock()
					go pm.FetchDataShard(refNum, shard, root)
				}
			}
		}
	}
}

// FetchDataShard sends request for each data
func (pm *ProtocolManager) FetchDataShard(refNum, shard uint64, root common.Hash) {
	var (
		keys  []*types.CKeys
		count uint64
	)
	dc, _ := pm.blockchain.Dc(refNum)
	dc.DataCacheMu.RLock()
	for addr, lshard := range dc.AddrToShard {
		if lshard == shard {
			keys = append(keys, dc.Keyval[addr])
			count++
		}
	}
	dc.DataCacheMu.RUnlock()

	pm.cousinPeerLock.RLock()
	peers := pm.cousinPeers[shard].PeersWithoutRequest(refNum)
	pm.cousinPeerLock.RUnlock()
	// Send the data request to a sqrt(N) nodes, where N is the number of nodes
	// connected for the shard
	requestLen := int(math.Sqrt(float64(len(peers))))
	if requestLen < minRequestPeers {
		requestLen = minRequestPeers
	}

	if requestLen > len(peers) {
		requestLen = len(peers)
	}
	requests := peers[:uint64(requestLen)]
	dreq := pm.logdir + "dreq" // + strconv.Itoa(int(pm.myshard))
	dreqf, err := os.OpenFile(dreq, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open dreqFile", "err", err)
	}
	for _, peer := range requests {
		fmt.Fprintln(dreqf, refNum, shard, count, root.Hex(), peer.ID(), time.Now().Unix())
		peer.SendDataRequest(refNum, count, root, keys)
	}
	dreqf.Close()
	return
}

// BroadcastBlock will either propagate a block to a subset of it's peers, or
// will only announce it's availability (depending what's requested).
func (pm *ProtocolManager) BroadcastBlock(block *types.Block, propagate bool) {
	hash := block.Hash()

	// If propagation is requested, send to a subset of the peer
	if propagate {
		// Calculate the TD of the block (it's not imported yet, so block.Td is not valid)
		var (
			td     *big.Int
			parent *types.Block
		)

		// If the node belongs to a reference shard, broadcast its
		// block to a subset of all the shards. Otherwise, broadcast
		// to only your own shards.
		if pm.myshard == uint64(0) {
			if parent = pm.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1); parent != nil {
				td = new(big.Int).Add(block.Difficulty(), pm.blockchain.GetTd(block.ParentHash(), block.NumberU64()-1))
			} else {
				log.Error("Propagating dangling block", "number", block.Number(), "hash", hash)
				return
			}

			pm.cousinPeerLock.RLock()
			for i := range pm.cousinPeers {
				peers := pm.cousinPeers[i].PeersWithoutBlock(hash)
				// Send the block to a subset of our peers
				transferLen := int(math.Sqrt(float64(len(peers))))
				if transferLen < minBroadcastPeers {
					transferLen = minBroadcastPeers
				}
				if transferLen > len(peers) {
					transferLen = len(peers)
				}
				transfer := peers[:transferLen]
				for _, peer := range transfer {
					if i == pm.myshard {
						peer.AsyncSendNewBlock(false, block, td)
					} else {
						peer.AsyncSendNewBlock(true, block, td)
					}
				}
				log.Trace("Propagated block", "hash", hash, "recipients", len(transfer), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
			}
			pm.cousinPeerLock.RUnlock()
		} else {

			ref := block.Shard() == uint64(0)
			if ref {

				if parent = pm.refchain.GetBlock(block.ParentHash(), block.NumberU64()-1); parent != nil {
					td = new(big.Int).Add(block.Difficulty(), pm.refchain.GetTd(block.ParentHash(), block.NumberU64()-1))
				} else {
					log.Error("Propagating dangling block", "number", block.Number(), "hash", hash)
					return
				}

				pm.cousinPeerLock.RLock()
				for i := range pm.cousinPeers {
					peers := pm.cousinPeers[i].PeersWithoutBlock(hash)
					// Send the block to a subset of our peers
					transferLen := int(math.Sqrt(float64(len(peers))))
					if transferLen < minBroadcastPeers {
						transferLen = minBroadcastPeers
					}
					if transferLen > len(peers) {
						transferLen = len(peers)
					}
					transfer := peers[:transferLen]
					for _, peer := range transfer {
						if i != uint64(0) {
							peer.AsyncSendNewBlock(true, block, td)
						}
					}
					log.Trace("Propagated block", "hash", hash, "recipients", len(transfer), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
				}
				pm.cousinPeerLock.RUnlock()
			} else {

				if parent = pm.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1); parent != nil {
					parentTd := pm.blockchain.GetTd(block.ParentHash(), block.NumberU64()-1)
					if parentTd != nil {
						td = new(big.Int).Add(block.Difficulty(), parentTd)
					} else {
						return
					}
				} else {
					log.Error("Propagating dangling block", "number", block.Number(), "hash", hash)
					return
				}

				pm.cousinPeerLock.RLock()
				peers := pm.cousinPeers[pm.myshard].PeersWithoutBlock(hash)
				pm.cousinPeerLock.RUnlock()
				// Send the block to a subset of our peers
				transferLen := int(math.Sqrt(float64(len(peers))))
				if transferLen < minBroadcastPeers {
					transferLen = minBroadcastPeers
				}
				if transferLen > len(peers) {
					transferLen = len(peers)
				}
				transfer := peers[:transferLen]
				for _, peer := range transfer {
					peer.AsyncSendNewBlock(false, block, td)
				}
				log.Trace("Propagated block", "hash", hash, "recipients", len(transfer), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))

				shardByte := make([]byte, 32)
				blkNumByte := make([]byte, 32)
				refBlkNumByte := make([]byte, 32)
				binary.BigEndian.PutUint64(shardByte[24:], pm.myshard)
				binary.BigEndian.PutUint64(blkNumByte[24:], block.NumberU64())
				binary.BigEndian.PutUint64(refBlkNumByte[24:], block.RefNumberU64())

				var start int
				dataLen := 4*32 + 4
				data := make([]byte, dataLen)
				funcAddress, _ := hex.DecodeString("8a31297a")
				start += copy(data[start:], funcAddress)
				start += copy(data[start:], shardByte)
				start += copy(data[start:], blkNumByte)
				start += copy(data[start:], refBlkNumByte)
				start += copy(data[start:], block.Root().Bytes())

				// NewTransaction(txType, nonce, shard, to, amount, gasLimit, gasPrice, data)
				stateTx := types.NewTransaction(types.StateCommit, block.RefNumberU64()+block.NumberU64()-1, pm.myshard, pm.refAddress, big.NewInt(0), pm.stateGasLimit, pm.stateGasPrice, data)
				var txs []*types.Transaction
				txs = append(txs, stateTx)
				pm.cousinPeerLock.RLock()
				refPeers := pm.cousinPeers[uint64(0)].PeersWithoutTx(stateTx.Hash())
				pm.cousinPeerLock.RUnlock()

				// Send the block to a subset of our peers
				rTransferLen := int(math.Sqrt(float64(len(refPeers))))
				if rTransferLen < minBroadcastPeers {
					rTransferLen = minBroadcastPeers
				}
				if rTransferLen > len(refPeers) {
					rTransferLen = len(refPeers)
				}
				rTransfer := refPeers[:rTransferLen]
				for _, peer := range rTransfer {
					peer.AsyncSendTransactions(txs)
				}
				log.Debug("Propagated state committment", "number", block.Number(), "bh", block.Hash(), "th", stateTx.Hash(), "val", stateTx.Value().Uint64(), "data", hex.EncodeToString(data))
			}
		}
		return
	}
	// Otherwise if the block is indeed in out own chain, announce it

	ref := pm.myshard > uint64(0) && block.Shard() == uint64(0)
	if ref {
		if pm.refchain.HasBlock(hash, block.NumberU64()) {
			receipients := 0
			pm.cousinPeerLock.RLock()
			if pm.cousinPeers[pm.myshard] == nil {
				pm.cousinPeers[pm.myshard] = newPeerSet()
			}
			peers := pm.cousinPeers[pm.myshard].PeersWithoutBlock(hash)
			pm.cousinPeerLock.RUnlock()
			for _, peer := range peers {
				peer.AsyncSendNewBlockHash(true, block)
				receipients++
			}
			log.Trace("Propagated block", "hash", hash, "recipients", receipients, "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
		}
	} else {
		if pm.blockchain.HasBlock(hash, block.NumberU64()) {
			receipients := 0
			if pm.myshard == 0 {
				pm.cousinPeerLock.RLock()
				for i := range pm.cousinPeers {
					peers := pm.cousinPeers[i].PeersWithoutBlock(hash)
					// Send the block to a subset of our peers
					transferLen := int(math.Sqrt(float64(len(peers))))
					if transferLen < minBroadcastPeers {
						transferLen = minBroadcastPeers
					}
					if transferLen > len(peers) {
						transferLen = len(peers)
					}
					transfer := peers[:transferLen]
					for _, peer := range transfer {
						if i == pm.myshard {
							peer.AsyncSendNewBlockHash(false, block)
						} else {
							peer.AsyncSendNewBlockHash(true, block)
						}
					}
					log.Debug("Propagated block", "hash", hash, "recipients", len(transfer), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
				}
				pm.cousinPeerLock.RUnlock()
			} else {
				pm.cousinPeerLock.RLock()
				peers := pm.cousinPeers[pm.myshard].PeersWithoutBlock(hash)
				pm.cousinPeerLock.RUnlock()
				for _, peer := range peers {
					peer.AsyncSendNewBlockHash(false, block)
					receipients++
				}
				log.Debug("Propagated block", "hash", hash, "recipients", receipients, "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
			}
		}
	}
}

// BroadcastTxs will propagate a batch of transactions to all peers which are not known to
// already have the given transaction.
func (pm *ProtocolManager) BroadcastTxs(txs types.Transactions) {
	var txset = make(map[*peer]types.Transactions)

	// Broadcast transactions to a batch of peers not knowing about it
	// NOTE: Raft-based consensus currently assumes that geth broadcasts
	// transactions to all peers in the network. A previous comment here
	// indicated that this logic might change in the future to only send to a
	// subset of peers. If this change occurs upstream, a merge conflict should
	// arise here, and we should add logic to send to *all* peers in raft mode.

	for _, tx := range txs {
		pm.cousinPeerLock.RLock()
		if pm.cousinPeers[pm.myshard] == nil {
			pm.cousinPeers[pm.myshard] = newPeerSet()
		}
		peers := pm.cousinPeers[pm.myshard].PeersWithoutTx(tx.Hash())
		pm.cousinPeerLock.RUnlock()
		for _, peer := range peers {
			txset[peer] = append(txset[peer], tx)
		}
		log.Trace("Broadcast transaction", "hash", tx.Hash(), "recipients", len(peers))
	}
	// FIXME include this again: peers = peers[:int(math.Sqrt(float64(len(peers))))]
	for peer, txs := range txset {
		peer.AsyncSendTransactions(txs)
	}
}

// Mined broadcast loop
func (pm *ProtocolManager) minedBroadcastLoop() {
	// automatically stops if unsubscribe
	for obj := range pm.minedBlockSub.Chan() {
		if ev, ok := obj.Data.(core.NewMinedBlockEvent); ok {
			pm.BroadcastBlock(ev.Block, true)  // First propagate block to peers
			pm.BroadcastBlock(ev.Block, false) // Only then announce to the rest
		}
	}
}

func (pm *ProtocolManager) fetchForeignDataLoop() {
	for obj := range pm.refBlockSub.Chan() {
		if ev, ok := obj.Data.(core.NewRefBlockEvent); ok {
			pm.FetchData(ev.Start, ev.End)
		}
	}
}

func (pm *ProtocolManager) txBroadcastLoop() {
	for {
		select {
		case event := <-pm.txsCh:
			pm.BroadcastTxs(event.Txs)

		// Err() channel will be closed when unsubscribing.
		case <-pm.txsSub.Err():
			return
		}
	}
}

// NodeInfo represents a short summary of the Ethereum sub-protocol metadata
// known about the host peer.
type NodeInfo struct {
	Network    uint64              `json:"network"`    // Ethereum network ID (1=Frontier, 2=Morden, Ropsten=3, Rinkeby=4)
	Shard      uint64              `json:"shard"`      // Shard Id (0=Reference Shard, >0 for Others)
	Difficulty *big.Int            `json:"difficulty"` // Total difficulty of the host's blockchain
	Genesis    common.Hash         `json:"genesis"`    // SHA3 hash of the host's genesis block
	Config     *params.ChainConfig `json:"config"`     // Chain configuration for the fork rules
	Head       common.Hash         `json:"head"`       // SHA3 hash of the host's best owned block
	Consensus  string              `json:"consensus"`  // Consensus mechanism in use
}

// NodeInfo retrieves some protocol metadata about the running host node.
func (pm *ProtocolManager) NodeInfo() *NodeInfo {
	currentBlock := pm.blockchain.CurrentBlock()

	return &NodeInfo{
		Network:    pm.networkID,
		Shard:      pm.myshard,
		Difficulty: pm.blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64()),
		Genesis:    pm.blockchain.Genesis().Hash(),
		Config:     pm.blockchain.Config(),
		Head:       currentBlock.Hash(),
		Consensus:  pm.getConsensusAlgorithm(),
	}
}

func (pm *ProtocolManager) getConsensusAlgorithm() string {
	var consensusAlgo string
	if pm.raftMode { // raft does not use consensus interface
		consensusAlgo = "raft"
	} else {
		switch pm.engine.(type) {
		case consensus.Istanbul:
			consensusAlgo = "istanbul"
		case *clique.Clique:
			consensusAlgo = "clique"
		case *ethash.Ethash:
			consensusAlgo = "ethash"
		default:
			consensusAlgo = "unknown"
		}
	}
	return consensusAlgo
}

// FindPeers returns
func (pm *ProtocolManager) FindPeers(targets map[common.Address]bool) map[common.Address]consensus.Peer {
	m := make(map[common.Address]consensus.Peer)
	pm.cousinPeerLock.Lock()
	defer pm.cousinPeerLock.Unlock()
	if pm.cousinPeers[pm.myshard] == nil {
		return m
	}
	for _, p := range pm.cousinPeers[pm.myshard].Peers() {
		pubKey := p.Node().Pubkey()
		addr := crypto.PubkeyToAddress(*pubKey)
		if targets[addr] {
			m[addr] = p
		}
	}
	return m
}
