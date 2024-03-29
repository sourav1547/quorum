// Copyright 2014 The go-ethereum Authors
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

// Package core implements the Ethereum consensus protocol.
package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	mrand "math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	lru "github.com/hashicorp/golang-lru"
)

var (
	blockInsertTimer = metrics.NewRegisteredTimer("chain/inserts", nil)

	ErrNoGenesis = errors.New("Genesis not found in chain")
)

const (
	bodyCacheLimit      = 256
	blockCacheLimit     = 256
	receiptsCacheLimit  = 32
	maxFutureBlocks     = 256
	maxTimeFutureBlocks = 30
	badBlockLimit       = 10
	triesInMemory       = 128

	// BlockChainVersion ensures that an incompatible database forces a resync from scratch.
	BlockChainVersion = 3
)

// CacheConfig contains the configuration values for the trie caching/pruning
// that's resident in a blockchain.
type CacheConfig struct {
	Disabled      bool          // Whether to disable trie write caching (archive node)
	TrieNodeLimit int           // Memory limit (MB) at which to flush the current in-memory trie to disk
	TrieTimeLimit time.Duration // Time limit after which to flush the current in-memory trie to disk
}

// BlockChain represents the canonical chain given a database with a genesis
// block. The Blockchain manages chain imports, reverts, chain reorganisations.
//
// Importing blocks in to the block chain happens according to the set of rules
// defined by the two stage Validator. Processing of blocks is done using the
// Processor which processes the included transaction. The validation of the state
// is done in the second part of the Validator. Failing results in aborting of
// the import.
//
// The BlockChain also helps in returning blocks from **any** chain included
// in the database as well as blocks that represents the canonical chain. It's
// important to note that GetBlock can return any block and does not need to be
// included in the canonical one where as GetBlockByNumber always represents the
// canonical chain.
type BlockChain struct {
	myshard     uint64
	numShard    uint64
	ref         bool                // To indicate a reference chain
	chainConfig *params.ChainConfig // Chain & network configuration
	cacheConfig *CacheConfig        // Cache configuration for pruning

	commitAddress   common.Address                 // Address of state commitment transaction
	pendingCrossTxs map[uint64]types.CrossShardTxs // Pending Cross shard transactions
	commitments     map[uint64]*types.Commitments  // Known commitments for each shard
	foreignData     map[uint64]*types.DataCache    // Data of foreign shards
	myLatestCommit  *types.Commitment              // Latest committed block
	foreignDataMu   sync.RWMutex                   // Lock for foreign data
	foreignDataCh   chan struct{}

	logdir        string
	gLocked       *types.RWLock                      // Currently readLocked
	lockedAddrMap map[uint64]map[common.Address]bool // shard to addr map

	lastCommit map[uint64]*types.Commitment // To store the last rs block that includes a commit
	lastCtx    map[uint64]uint64            // to store whether a shard is touched by a ctx or not
	procCtxs   map[common.Hash]bool         // processed cross shard transaction

	db     ethdb.Database // Low level persistent database to store final content in
	triegc *prque.Prque   // Priority queue mapping block numbers to tries to gc
	gcproc time.Duration  // Accumulates canonical block processing for trie dumping

	hc              *HeaderChain
	rmLogsFeed      event.Feed
	chainFeed       event.Feed
	chainSideFeed   event.Feed
	chainHeadFeed   event.Feed
	commitHeadFeed  event.Feed
	foreignDataFeed event.Feed
	logsFeed        event.Feed
	scope           event.SubscriptionScope
	genesisBlock    *types.Block

	mu      sync.RWMutex // global mutex for locking chain operations
	chainmu sync.RWMutex // blockchain insertion lock
	procmu  sync.RWMutex // block processor lock
	ctxmu   sync.RWMutex // cross-shard trasnaction processor!

	nonce            uint64
	checkpoint       int          // checkpoint counts towards the new checkpoint
	currentBlock     atomic.Value // Current head of the block chain
	currentFastBlock atomic.Value // Current head of the fast-sync chain (may be above the block chain!)

	stateCache    state.Database // State database to reuse between imports (contains state cache)
	bodyCache     *lru.Cache     // Cache for the most recent block bodies
	bodyRLPCache  *lru.Cache     // Cache for the most recent block bodies in RLP encoded format
	receiptsCache *lru.Cache     // Cache for the most recent receipts per block
	blockCache    *lru.Cache     // Cache for the most recent entire blocks
	futureBlocks  *lru.Cache     // future blocks are blocks added for later processing

	quit    chan struct{} // blockchain quit channel
	running int32         // running must be called atomically
	// procInterrupt must be atomically called
	procInterrupt int32          // interrupt signaler for block processing
	wg            sync.WaitGroup // chain processing wait group for shutting down

	engine    consensus.Engine
	processor Processor // block processor interface
	validator Validator // block and state validator interface
	vmConfig  vm.Config

	badBlocks      *lru.Cache              // Bad block cache
	shouldPreserve func(*types.Block) bool // Function used to determine whether should preserve the given block.

	privateStateCache state.Database // Private state database to reuse between imports (contains state cache)
}

// NewBlockChain returns a fully initialised block chain using information
// available in the database. It initialises the default Ethereum Validator and
// Processor.
func NewBlockChain(db ethdb.Database, cacheConfig *CacheConfig, chainConfig *params.ChainConfig, engine consensus.Engine, vmConfig vm.Config, shouldPreserve func(block *types.Block) bool, ref bool, shard, numShard uint64, commitments map[uint64]*types.Commitments, pendingCrossTxs map[uint64]types.CrossShardTxs, myLatestCommit *types.Commitment, foreignData map[uint64]*types.DataCache, foreignDataMu sync.RWMutex, gLocked *types.RWLock, lastCommit map[uint64]*types.Commitment, lastCtx map[uint64]uint64, lockedAddrMap map[uint64]map[common.Address]bool, logdir string) (*BlockChain, error) {
	if cacheConfig == nil {
		cacheConfig = &CacheConfig{
			TrieNodeLimit: 256,
			TrieTimeLimit: 5 * time.Minute,
		}
	}
	bodyCache, _ := lru.New(bodyCacheLimit)
	bodyRLPCache, _ := lru.New(bodyCacheLimit)
	receiptsCache, _ := lru.New(receiptsCacheLimit)
	blockCache, _ := lru.New(blockCacheLimit)
	futureBlocks, _ := lru.New(maxFutureBlocks)
	badBlocks, _ := lru.New(badBlockLimit)

	bc := &BlockChain{
		myshard:           shard,
		numShard:          numShard,
		ref:               ref,
		chainConfig:       chainConfig,
		cacheConfig:       cacheConfig,
		db:                db,
		triegc:            prque.New(nil),
		stateCache:        state.NewDatabase(db),
		quit:              make(chan struct{}),
		shouldPreserve:    shouldPreserve,
		bodyCache:         bodyCache,
		bodyRLPCache:      bodyRLPCache,
		receiptsCache:     receiptsCache,
		blockCache:        blockCache,
		futureBlocks:      futureBlocks,
		engine:            engine,
		vmConfig:          vmConfig,
		badBlocks:         badBlocks,
		privateStateCache: state.NewDatabase(db),
		pendingCrossTxs:   pendingCrossTxs,
		commitments:       commitments,
		foreignData:       foreignData,
		foreignDataMu:     foreignDataMu,
		foreignDataCh:     make(chan struct{}),
		myLatestCommit:    myLatestCommit,
		gLocked:           gLocked,
		lastCommit:        lastCommit,
		lastCtx:           lastCtx,
		procCtxs:          make(map[common.Hash]bool),
		lockedAddrMap:     lockedAddrMap,
		logdir:            logdir,
	}
	bc.SetValidator(NewBlockValidator(chainConfig, bc, engine))
	bc.SetProcessor(NewStateProcessor(chainConfig, bc, engine))

	var err error
	bc.hc, err = NewHeaderChain(db, chainConfig, engine, bc.getProcInterrupt)
	if err != nil {
		return nil, err
	}
	bc.genesisBlock = bc.GetBlockByNumber(0)
	if bc.genesisBlock == nil {
		return nil, ErrNoGenesis
	}

	refNum := uint64(0)
	genRoot := bc.genesisBlock.Root()
	genHash := bc.genesisBlock.Hash()
	bc.gLocked.Mu.Lock()
	if bc.myshard == 0 && !bc.ref {
		// Initialize commit for every shard!
		for i := uint64(1); i < numShard; i++ {
			bc.lastCtx[i] = refNum
		}
		for i := uint64(1); i < numShard; i++ {
			bc.lastCommit[i] = &types.Commitment{
				Shard:     shard,
				BlockNum:  uint64(0),
				RefNum:    uint64(0),
				StateRoot: genRoot,
				BHash:     genHash,
			}
		}
	} else {
		if bc.ref {
			// Initialize foreign data and commitments for each shard
			// I am adding this only in ref chain of each worker shard
			// as the variables are shard among both nodes!
			bc.foreignData[refNum] = types.NewDataCache(refNum, true)
			bc.commitments[refNum] = types.NewCommitments()
			for shard := uint64(0); shard < numShard; shard++ {
				commit := &types.Commitment{
					Shard:     shard,
					BlockNum:  uint64(0),
					RefNum:    uint64(0),
					StateRoot: genRoot,
					BHash:     genHash,
				}
				bc.commitments[refNum].AddCommit(shard, commit)
			}
			// Set my latest commit root to be root of genesis block!
			bc.myLatestCommit.StateRoot = bc.genesisBlock.Root()
		}
	}
	bc.gLocked.Mu.Unlock()

	if err := bc.loadLastState(); err != nil {
		return nil, err
	}
	// Check the current state of the block hashes and make sure that we do not have any of the bad blocks in our chain
	for hash := range BadHashes {
		if header := bc.GetHeaderByHash(hash); header != nil {
			// get the canonical block corresponding to the offending header's number
			headerByNumber := bc.GetHeaderByNumber(header.Number.Uint64())
			// make sure the headerByNumber (if present) is in our current canonical chain
			if headerByNumber != nil && headerByNumber.Hash() == header.Hash() {
				log.Error("Found bad hash, rewinding chain", "number", header.Number, "hash", header.ParentHash)
				bc.SetHead(header.Number.Uint64() - 1)
				log.Error("Chain rewind was successful, resuming normal operation")
			}
		}
	}
	// Take ownership of this particular state
	go bc.update()
	return bc, nil
}

// CommitNum Returns latest committed block!
func (bc *BlockChain) CommitNum() uint64 {
	return bc.myLatestCommit.BlockNum
}

// CtxExist returns whether a reference number has cross-shard
// transaction or not!
func (bc *BlockChain) CtxExist(num uint64) bool {
	ctx, cok := bc.pendingCrossTxs[num]
	if !cok {
		return false
	}
	return ctx.TxCount() > 0
}

// CrossTxs returns cross-shard transaciton at any height
func (bc *BlockChain) CrossTxs(work uint64) types.CrossShardTxs {
	return bc.pendingCrossTxs[work]
}

// CrossTxsLocked returns pending cross txs
func (bc *BlockChain) CrossTxsLocked(work uint64) types.CrossShardTxs {
	return bc.pendingCrossTxs[work]
}

// MyShard retuns shard of a blockchain
func (bc *BlockChain) MyShard() uint64 {
	return bc.myshard
}

// NumShard returns number of shard in the system
func (bc *BlockChain) NumShard() uint64 {
	return bc.numShard
}

func (bc *BlockChain) getProcInterrupt() bool {
	return atomic.LoadInt32(&bc.procInterrupt) == 1
}

// CommitAddress Returns the address of the state commitment transaction
func (bc *BlockChain) CommitAddress() common.Address {
	return bc.commitAddress
}

// SetCommitAddress Sets the commit address of the chain.
func (bc *BlockChain) SetCommitAddress(addr common.Address) {
	bc.commitAddress = addr
}

// Dc returns committed data and its current status!
func (bc *BlockChain) Dc(rnum uint64) (*types.DataCache, bool) {
	bc.foreignDataMu.RLock()
	defer bc.foreignDataMu.RUnlock()
	if dc, dok := bc.foreignData[rnum]; dok {
		return dc, dc.Status
	}
	return nil, false
}

// IsProcessed returns whether a transaction has been already processed or not
func (bc *BlockChain) IsProcessed(hash common.Hash) bool {
	bc.ctxmu.RLock()
	defer bc.ctxmu.RUnlock()
	_, tok := bc.procCtxs[hash]
	return tok
}

// IsProcessedLocked returns whether a trasnaction is already processed or not!
func (bc *BlockChain) IsProcessedLocked(thash common.Hash) bool {
	// This function assumes the gLocked.Mu is alread held
	_, tok := bc.procCtxs[thash]
	return tok
}

// AddProcessed a new trasnaction to the processed
func (bc *BlockChain) AddProcessed(thash common.Hash) {
	// This method assumes that gLocked.Mu is already held
	bc.ctxmu.Lock()
	defer bc.ctxmu.Unlock()
	bc.procCtxs[thash] = false
}

// loadLastState loads the last known chain state from the database. This method
// assumes that the chain manager mutex is held.
func (bc *BlockChain) loadLastState() error {
	// Restore the last known head block
	head := rawdb.ReadHeadBlockHash(bc.db)
	if head == (common.Hash{}) {
		// Corrupt or empty database, init from scratch
		log.Warn("Empty database, resetting chain")
		return bc.Reset()
	}
	// Make sure the entire head block is available
	currentBlock := bc.GetBlockByHash(head)
	if currentBlock == nil {
		// Corrupt or empty database, init from scratch
		log.Warn("Head block missing, resetting chain", "hash", head)
		return bc.Reset()
	}
	// Make sure the state associated with the block is available
	if _, err := state.New(currentBlock.Root(), bc.stateCache); err != nil {
		// Dangling block without a state associated, init from scratch
		log.Warn("Head state missing, repairing chain", "number", currentBlock.Number(), "hash", currentBlock.Hash())
		if err := bc.repair(&currentBlock); err != nil {
			return err
		}
	}

	// Quorum
	if _, err := state.New(GetPrivateStateRoot(bc.db, currentBlock.Root()), bc.privateStateCache); err != nil {
		log.Warn("Head private state missing, resetting chain", "number", currentBlock.Number(), "hash", currentBlock.Hash())
		return bc.Reset()
	}
	// /Quorum

	// Everything seems to be fine, set as the head block
	bc.currentBlock.Store(currentBlock)

	// Restore the last known head header
	currentHeader := currentBlock.Header()
	if head := rawdb.ReadHeadHeaderHash(bc.db); head != (common.Hash{}) {
		if header := bc.GetHeaderByHash(head); header != nil {
			currentHeader = header
		}
	}
	bc.hc.SetCurrentHeader(currentHeader)

	// Restore the last known head fast block
	bc.currentFastBlock.Store(currentBlock)
	if head := rawdb.ReadHeadFastBlockHash(bc.db); head != (common.Hash{}) {
		if block := bc.GetBlockByHash(head); block != nil {
			bc.currentFastBlock.Store(block)
		}
	}

	// Issue a status log for the user
	currentFastBlock := bc.CurrentFastBlock()

	headerTd := bc.GetTd(currentHeader.Hash(), currentHeader.Number.Uint64())
	blockTd := bc.GetTd(currentBlock.Hash(), currentBlock.NumberU64())
	fastTd := bc.GetTd(currentFastBlock.Hash(), currentFastBlock.NumberU64())

	log.Debug("Loaded most recent local header", "number", currentHeader.Number, "hash", currentHeader.Hash(), "td", headerTd, "age", common.PrettyAge(time.Unix(currentHeader.Time.Int64(), 0)))
	log.Debug("Loaded most recent local full block", "number", currentBlock.Number(), "hash", currentBlock.Hash(), "td", blockTd, "age", common.PrettyAge(time.Unix(currentBlock.Time().Int64(), 0)))
	log.Debug("Loaded most recent local fast block", "number", currentFastBlock.Number(), "hash", currentFastBlock.Hash(), "td", fastTd, "age", common.PrettyAge(time.Unix(currentFastBlock.Time().Int64(), 0)))

	return nil
}

// SetHead rewinds the local chain to a new head. In the case of headers, everything
// above the new head will be deleted and the new one set. In the case of blocks
// though, the head may be further rewound if block bodies are missing (non-archive
// nodes after a fast sync).
func (bc *BlockChain) SetHead(head uint64) error {
	log.Warn("Rewinding blockchain", "target", head)

	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Rewind the header chain, deleting all block bodies until then
	delFn := func(db rawdb.DatabaseDeleter, hash common.Hash, num uint64) {
		rawdb.DeleteBody(db, hash, num)
	}
	bc.hc.SetHead(head, delFn)
	currentHeader := bc.hc.CurrentHeader()

	// Clear out any stale content from the caches
	bc.bodyCache.Purge()
	bc.bodyRLPCache.Purge()
	bc.receiptsCache.Purge()
	bc.blockCache.Purge()
	bc.futureBlocks.Purge()

	// Rewind the block chain, ensuring we don't end up with a stateless head block
	if currentBlock := bc.CurrentBlock(); currentBlock != nil && currentHeader.Number.Uint64() < currentBlock.NumberU64() {
		bc.currentBlock.Store(bc.GetBlock(currentHeader.Hash(), currentHeader.Number.Uint64()))
	}
	if currentBlock := bc.CurrentBlock(); currentBlock != nil {
		if _, err := state.New(currentBlock.Root(), bc.stateCache); err != nil {
			// Rewound state missing, rolled back to before pivot, reset to genesis
			bc.currentBlock.Store(bc.genesisBlock)
		}
	}
	// Rewind the fast block in a simpleton way to the target head
	if currentFastBlock := bc.CurrentFastBlock(); currentFastBlock != nil && currentHeader.Number.Uint64() < currentFastBlock.NumberU64() {
		bc.currentFastBlock.Store(bc.GetBlock(currentHeader.Hash(), currentHeader.Number.Uint64()))
	}
	// If either blocks reached nil, reset to the genesis state
	if currentBlock := bc.CurrentBlock(); currentBlock == nil {
		bc.currentBlock.Store(bc.genesisBlock)
	}
	if currentFastBlock := bc.CurrentFastBlock(); currentFastBlock == nil {
		bc.currentFastBlock.Store(bc.genesisBlock)
	}
	currentBlock := bc.CurrentBlock()
	currentFastBlock := bc.CurrentFastBlock()

	rawdb.WriteHeadBlockHash(bc.db, currentBlock.Hash())
	rawdb.WriteHeadFastBlockHash(bc.db, currentFastBlock.Hash())

	return bc.loadLastState()
}

// FastSyncCommitHead sets the current head block to the one defined by the hash
// irrelevant what the chain contents were prior.
func (bc *BlockChain) FastSyncCommitHead(hash common.Hash) error {
	// Make sure that both the block as well at its state trie exists
	block := bc.GetBlockByHash(hash)
	if block == nil {
		return fmt.Errorf("non existent block [%x…]", hash[:4])
	}
	if _, err := trie.NewSecure(block.Root(), bc.stateCache.TrieDB(), 0); err != nil {
		return err
	}
	// If all checks out, manually set the head block
	bc.mu.Lock()
	bc.currentBlock.Store(block)
	bc.mu.Unlock()

	log.Info("Committed new head block", "number", block.Number(), "hash", hash)
	return nil
}

// GasLimit returns the gas limit of the current HEAD block.
func (bc *BlockChain) GasLimit() uint64 {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	if bc.Config().IsQuorum {
		return math.MaxBig256.Uint64() // HACK(joel) a very large number
	} else {
		return bc.CurrentBlock().GasLimit()
	}
}

// CurrentBlock retrieves the current head block of the canonical chain. The
// block is retrieved from the blockchain's internal cache.
func (bc *BlockChain) CurrentBlock() *types.Block {
	return bc.currentBlock.Load().(*types.Block)
}

// CurrentFastBlock retrieves the current fast-sync head block of the canonical
// chain. The block is retrieved from the blockchain's internal cache.
func (bc *BlockChain) CurrentFastBlock() *types.Block {
	return bc.currentFastBlock.Load().(*types.Block)
}

// SetProcessor sets the processor required for making state modifications.
func (bc *BlockChain) SetProcessor(processor Processor) {
	bc.procmu.Lock()
	defer bc.procmu.Unlock()
	bc.processor = processor
}

// SetValidator sets the validator which is used to validate incoming blocks.
func (bc *BlockChain) SetValidator(validator Validator) {
	bc.procmu.Lock()
	defer bc.procmu.Unlock()
	bc.validator = validator
}

// Validator returns the current validator.
func (bc *BlockChain) Validator() Validator {
	bc.procmu.RLock()
	defer bc.procmu.RUnlock()
	return bc.validator
}

// Processor returns the current processor.
func (bc *BlockChain) Processor() Processor {
	bc.procmu.RLock()
	defer bc.procmu.RUnlock()
	return bc.processor
}

// State returns a new mutable state based on the current HEAD block.
func (bc *BlockChain) State() (*state.StateDB, *state.StateDB, error) {
	return bc.StateAt(bc.CurrentBlock().Root())
}

// StateAt returns a new mutable state based on a particular point in time.
func (bc *BlockChain) StateAt(root common.Hash) (*state.StateDB, *state.StateDB, error) {
	publicStateDb, publicStateDbErr := state.New(root, bc.stateCache)
	if publicStateDbErr != nil {
		return nil, nil, publicStateDbErr
	}
	privateStateDb, privateStateDbErr := state.New(GetPrivateStateRoot(bc.db, root), bc.privateStateCache)
	if privateStateDbErr != nil {
		return nil, nil, privateStateDbErr
	}

	return publicStateDb, privateStateDb, nil
}

// StateData returns data at a particular root
func (bc *BlockChain) StateData(root common.Hash, keys []*types.CKeys) []*types.KeyVal {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	pstate, err := state.New(root, bc.stateCache)
	if err != nil {
		log.Error("Error in fetching state", "error", err)
		return nil
	}
	if pstate == nil {
		log.Error("State not found at", "root", root)
		return nil
	}

	var keyVals []*types.KeyVal
	for _, keyList := range keys {
		addr := keyList.Addr
		keyVal := &types.KeyVal{Addr: addr, Nonce: pstate.GetNonce(addr), Balance: pstate.GetBalance(addr).Uint64(), Data: []common.Hash{}}
		for _, key := range keyList.Keys {
			val := pstate.GetState(addr, key)
			keyVal.Data = append(keyVal.Data, val)
		}
		keyVals = append(keyVals, keyVal)
	}
	return keyVals
}

// PrivateStateAt returns private state
func (bc *BlockChain) PrivateStateAt(root common.Hash) (*state.StateDB, error) {
	privateStateDb, err := state.New(GetPrivateStateRoot(bc.db, root), bc.privateStateCache)
	if err != nil {
		return nil, err
	}
	return privateStateDb, nil
}

// Reset purges the entire blockchain, restoring it to its genesis state.
func (bc *BlockChain) Reset() error {
	return bc.ResetWithGenesisBlock(bc.genesisBlock)
}

// ResetWithGenesisBlock purges the entire blockchain, restoring it to the
// specified genesis state.
func (bc *BlockChain) ResetWithGenesisBlock(genesis *types.Block) error {
	// Dump the entire block chain and purge the caches
	if err := bc.SetHead(0); err != nil {
		return err
	}
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Prepare the genesis block and reinitialise the chain
	if err := bc.hc.WriteTd(genesis.Hash(), genesis.NumberU64(), genesis.Difficulty()); err != nil {
		log.Crit("Failed to write genesis block TD", "err", err)
	}
	rawdb.WriteBlock(bc.db, genesis)

	bc.genesisBlock = genesis
	bc.insert(bc.genesisBlock)
	bc.currentBlock.Store(bc.genesisBlock)
	bc.hc.SetGenesis(bc.genesisBlock.Header())
	bc.hc.SetCurrentHeader(bc.genesisBlock.Header())
	bc.currentFastBlock.Store(bc.genesisBlock)

	return nil
}

// repair tries to repair the current blockchain by rolling back the current block
// until one with associated state is found. This is needed to fix incomplete db
// writes caused either by crashes/power outages, or simply non-committed tries.
//
// This method only rolls back the current block. The current header and current
// fast block are left intact.
func (bc *BlockChain) repair(head **types.Block) error {
	for {
		// Abort if we've rewound to a head block that does have associated state
		if _, err := state.New((*head).Root(), bc.stateCache); err == nil {
			log.Info("Rewound blockchain to past state", "number", (*head).Number(), "hash", (*head).Hash())
			return nil
		}
		// Otherwise rewind one block and recheck state availability there
		(*head) = bc.GetBlock((*head).ParentHash(), (*head).NumberU64()-1)
	}
}

// Export writes the active chain to the given writer.
func (bc *BlockChain) Export(w io.Writer) error {
	return bc.ExportN(w, uint64(0), bc.CurrentBlock().NumberU64())
}

// ExportN writes a subset of the active chain to the given writer.
func (bc *BlockChain) ExportN(w io.Writer, first uint64, last uint64) error {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	if first > last {
		return fmt.Errorf("export failed: first (%d) is greater than last (%d)", first, last)
	}
	log.Info("Exporting batch of blocks", "count", last-first+1)

	start, reported := time.Now(), time.Now()
	for nr := first; nr <= last; nr++ {
		block := bc.GetBlockByNumber(nr)
		if block == nil {
			return fmt.Errorf("export failed on #%d: not found", nr)
		}
		if err := block.EncodeRLP(w); err != nil {
			return err
		}
		if time.Since(reported) >= statsReportLimit {
			log.Info("Exporting blocks", "exported", block.NumberU64()-first, "elapsed", common.PrettyDuration(time.Since(start)))
			reported = time.Now()
		}
	}

	return nil
}

// insert injects a new head block into the current block chain. This method
// assumes that the block is indeed a true head. It will also reset the head
// header and the head fast sync block to this very same block if they are older
// or if they are on a different side chain.
//
// Note, this function assumes that the `mu` mutex is held!
func (bc *BlockChain) insert(block *types.Block) {
	// If the block is on a side chain or an unknown one, force other heads onto it too
	updateHeads := rawdb.ReadCanonicalHash(bc.db, block.NumberU64()) != block.Hash()

	// Add the block to the canonical chain number scheme and mark as the head
	rawdb.WriteCanonicalHash(bc.db, block.Hash(), block.NumberU64())
	rawdb.WriteHeadBlockHash(bc.db, block.Hash())

	bc.currentBlock.Store(block)

	// If the block is better than our head or is on a different chain, force update heads
	if updateHeads {
		bc.hc.SetCurrentHeader(block.Header())
		rawdb.WriteHeadFastBlockHash(bc.db, block.Hash())

		bc.currentFastBlock.Store(block)
	}
}

// Genesis retrieves the chain's genesis block.
func (bc *BlockChain) Genesis() *types.Block {
	return bc.genesisBlock
}

// GetBody retrieves a block body (transactions and uncles) from the database by
// hash, caching it if found.
func (bc *BlockChain) GetBody(hash common.Hash) *types.Body {
	// Short circuit if the body's already in the cache, retrieve otherwise
	if cached, ok := bc.bodyCache.Get(hash); ok {
		body := cached.(*types.Body)
		return body
	}
	number := bc.hc.GetBlockNumber(hash)
	if number == nil {
		return nil
	}
	body := rawdb.ReadBody(bc.db, hash, *number)
	if body == nil {
		return nil
	}
	// Cache the found body for next time and return
	bc.bodyCache.Add(hash, body)
	return body
}

// GetBodyRLP retrieves a block body in RLP encoding from the database by hash,
// caching it if found.
func (bc *BlockChain) GetBodyRLP(hash common.Hash) rlp.RawValue {
	// Short circuit if the body's already in the cache, retrieve otherwise
	if cached, ok := bc.bodyRLPCache.Get(hash); ok {
		return cached.(rlp.RawValue)
	}
	number := bc.hc.GetBlockNumber(hash)
	if number == nil {
		return nil
	}
	body := rawdb.ReadBodyRLP(bc.db, hash, *number)
	if len(body) == 0 {
		return nil
	}
	// Cache the found body for next time and return
	bc.bodyRLPCache.Add(hash, body)
	return body
}

// HasBlock checks if a block is fully present in the database or not.
func (bc *BlockChain) HasBlock(hash common.Hash, number uint64) bool {
	if bc.blockCache.Contains(hash) {
		return true
	}
	return rawdb.HasBody(bc.db, hash, number)
}

// HasState checks if state trie is fully present in the database or not.
func (bc *BlockChain) HasState(hash common.Hash) bool {
	_, err := bc.stateCache.OpenTrie(hash)
	return err == nil
}

// HasBlockAndState checks if a block and associated state trie is fully present
// in the database or not, caching it if present.
func (bc *BlockChain) HasBlockAndState(hash common.Hash, number uint64) bool {
	// Check first that the block itself is known
	block := bc.GetBlock(hash, number)
	if block == nil {
		return false
	}
	return bc.HasState(block.Root())
}

// GetBlock retrieves a block from the database by hash and number,
// caching it if found.
func (bc *BlockChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	// Short circuit if the block's already in the cache, retrieve otherwise
	if block, ok := bc.blockCache.Get(hash); ok {
		return block.(*types.Block)
	}
	block := rawdb.ReadBlock(bc.db, hash, number)
	if block == nil {
		return nil
	}
	// Cache the found block for next time and return
	bc.blockCache.Add(block.Hash(), block)
	return block
}

// GetBlockByHash retrieves a block from the database by hash, caching it if found.
func (bc *BlockChain) GetBlockByHash(hash common.Hash) *types.Block {
	number := bc.hc.GetBlockNumber(hash)
	if number == nil {
		return nil
	}
	return bc.GetBlock(hash, *number)
}

// GetBlockByNumber retrieves a block from the database by number, caching it
// (associated with its hash) if found.
func (bc *BlockChain) GetBlockByNumber(number uint64) *types.Block {
	hash := rawdb.ReadCanonicalHash(bc.db, number)
	if hash == (common.Hash{}) {
		return nil
	}
	return bc.GetBlock(hash, number)
}

// GetGenesisHash returns hash of the geneis block
func (bc *BlockChain) GetGenesisHash() common.Hash {
	return rawdb.ReadCanonicalHash(bc.db, uint64(0))
}

// GetReceiptsByHash retrieves the receipts for all transactions in a given block.
func (bc *BlockChain) GetReceiptsByHash(hash common.Hash) types.Receipts {
	if receipts, ok := bc.receiptsCache.Get(hash); ok {
		return receipts.(types.Receipts)
	}

	number := rawdb.ReadHeaderNumber(bc.db, hash)
	if number == nil {
		return nil
	}

	receipts := rawdb.ReadReceipts(bc.db, hash, *number)
	bc.receiptsCache.Add(hash, receipts)
	return receipts
}

// GetBlocksFromHash returns the block corresponding to hash and up to n-1 ancestors.
// [deprecated by eth/62]
func (bc *BlockChain) GetBlocksFromHash(hash common.Hash, n int) (blocks []*types.Block) {
	number := bc.hc.GetBlockNumber(hash)
	if number == nil {
		return nil
	}
	for i := 0; i < n; i++ {
		block := bc.GetBlock(hash, *number)
		if block == nil {
			break
		}
		blocks = append(blocks, block)
		hash = block.ParentHash()
		*number--
	}
	return
}

// GetUnclesInChain retrieves all the uncles from a given block backwards until
// a specific distance is reached.
func (bc *BlockChain) GetUnclesInChain(block *types.Block, length int) []*types.Header {
	uncles := []*types.Header{}
	for i := 0; block != nil && i < length; i++ {
		uncles = append(uncles, block.Uncles()...)
		block = bc.GetBlock(block.ParentHash(), block.NumberU64()-1)
	}
	return uncles
}

// TrieNode retrieves a blob of data associated with a trie node (or code hash)
// either from ephemeral in-memory cache, or from persistent storage.
func (bc *BlockChain) TrieNode(hash common.Hash) ([]byte, error) {
	return bc.stateCache.TrieDB().Node(hash)
}

// Stop stops the blockchain service. If any imports are currently in progress
// it will abort them using the procInterrupt.
func (bc *BlockChain) Stop() {
	if !atomic.CompareAndSwapInt32(&bc.running, 0, 1) {
		return
	}
	// Unsubscribe all subscriptions registered from blockchain
	bc.scope.Close()
	close(bc.quit)
	atomic.StoreInt32(&bc.procInterrupt, 1)

	bc.wg.Wait()

	// Ensure the state of a recent block is also stored to disk before exiting.
	// We're writing three different states to catch different restart scenarios:
	//  - HEAD:     So we don't need to reprocess any blocks in the general case
	//  - HEAD-1:   So we don't do large reorgs if our HEAD becomes an uncle
	//  - HEAD-127: So we have a hard limit on the number of blocks reexecuted
	if !bc.cacheConfig.Disabled {
		triedb := bc.stateCache.TrieDB()

		for _, offset := range []uint64{0, 1, triesInMemory - 1} {
			if number := bc.CurrentBlock().NumberU64(); number > offset {
				recent := bc.GetBlockByNumber(number - offset)

				log.Info("Writing cached state to disk", "block", recent.Number(), "hash", recent.Hash(), "root", recent.Root())
				if err := triedb.Commit(recent.Root(), true); err != nil {
					log.Error("Failed to commit recent state trie", "err", err)
				}
			}
		}
		for !bc.triegc.Empty() {
			triedb.Dereference(bc.triegc.PopItem().(common.Hash))
		}
		if size, _ := triedb.Size(); size != 0 {
			log.Error("Dangling trie nodes after full cleanup")
		}
	}
	log.Info("Blockchain manager stopped")
}

func (bc *BlockChain) procFutureBlocks() {
	blocks := make([]*types.Block, 0, bc.futureBlocks.Len())
	for _, hash := range bc.futureBlocks.Keys() {
		if block, exist := bc.futureBlocks.Peek(hash); exist {
			blocks = append(blocks, block.(*types.Block))
		}
	}
	if len(blocks) > 0 {
		types.BlockBy(types.Number).Sort(blocks)

		// Insert one by one as chain insertion needs contiguous ancestry between blocks
		for i := range blocks {
			bc.InsertChain(blocks[i : i+1])
		}
	}
}

// WriteStatus status of write
type WriteStatus byte

const (
	NonStatTy WriteStatus = iota
	CanonStatTy
	SideStatTy
)

// Rollback is designed to remove a chain of links from the database that aren't
// certain enough to be valid.
func (bc *BlockChain) Rollback(chain []common.Hash) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	for i := len(chain) - 1; i >= 0; i-- {
		hash := chain[i]

		currentHeader := bc.hc.CurrentHeader()
		if currentHeader.Hash() == hash {
			bc.hc.SetCurrentHeader(bc.GetHeader(currentHeader.ParentHash, currentHeader.Number.Uint64()-1))
		}
		if currentFastBlock := bc.CurrentFastBlock(); currentFastBlock.Hash() == hash {
			newFastBlock := bc.GetBlock(currentFastBlock.ParentHash(), currentFastBlock.NumberU64()-1)
			bc.currentFastBlock.Store(newFastBlock)
			rawdb.WriteHeadFastBlockHash(bc.db, newFastBlock.Hash())
		}
		if currentBlock := bc.CurrentBlock(); currentBlock.Hash() == hash {
			newBlock := bc.GetBlock(currentBlock.ParentHash(), currentBlock.NumberU64()-1)
			bc.currentBlock.Store(newBlock)
			rawdb.WriteHeadBlockHash(bc.db, newBlock.Hash())
		}
	}
}

// SetReceiptsData computes all the non-consensus fields of the receipts
func SetReceiptsData(config *params.ChainConfig, block *types.Block, receipts types.Receipts) error {
	signer := types.MakeSigner(config, block.Number())

	transactions, logIndex := block.Transactions(), uint(0)
	if len(transactions) != len(receipts) {
		return errors.New("transaction and receipt count mismatch")
	}

	for j := 0; j < len(receipts); j++ {
		// The transaction hash can be retrieved from the transaction itself
		receipts[j].TxHash = transactions[j].Hash()

		// The contract address can be derived from the transaction itself
		if transactions[j].To() == nil {
			// Deriving the signer is expensive, only do if it's actually needed
			from, _ := types.Sender(signer, transactions[j])
			receipts[j].ContractAddress = crypto.CreateAddress(from, transactions[j].Nonce())
		}
		// The used gas can be calculated based on previous receipts
		if j == 0 {
			receipts[j].GasUsed = receipts[j].CumulativeGasUsed
		} else {
			receipts[j].GasUsed = receipts[j].CumulativeGasUsed - receipts[j-1].CumulativeGasUsed
		}
		// The derived log fields can simply be set from the block and transaction
		for k := 0; k < len(receipts[j].Logs); k++ {
			receipts[j].Logs[k].BlockNumber = block.NumberU64()
			receipts[j].Logs[k].BlockHash = block.Hash()
			receipts[j].Logs[k].TxHash = receipts[j].TxHash
			receipts[j].Logs[k].TxIndex = uint(j)
			receipts[j].Logs[k].Index = logIndex
			logIndex++
		}
	}
	return nil
}

// InsertReceiptChain attempts to complete an already existing header chain with
// transaction and receipt data.
func (bc *BlockChain) InsertReceiptChain(blockChain types.Blocks, receiptChain []types.Receipts) (int, error) {
	bc.wg.Add(1)
	defer bc.wg.Done()

	// Do a sanity check that the provided chain is actually ordered and linked
	for i := 1; i < len(blockChain); i++ {
		if blockChain[i].NumberU64() != blockChain[i-1].NumberU64()+1 || blockChain[i].ParentHash() != blockChain[i-1].Hash() {
			log.Error("Non contiguous receipt insert", "number", blockChain[i].Number(), "hash", blockChain[i].Hash(), "parent", blockChain[i].ParentHash(),
				"prevnumber", blockChain[i-1].Number(), "prevhash", blockChain[i-1].Hash())
			return 0, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, blockChain[i-1].NumberU64(),
				blockChain[i-1].Hash().Bytes()[:4], i, blockChain[i].NumberU64(), blockChain[i].Hash().Bytes()[:4], blockChain[i].ParentHash().Bytes()[:4])
		}
	}

	var (
		stats = struct{ processed, ignored int32 }{}
		start = time.Now()
		bytes = 0
		batch = bc.db.NewBatch()
	)
	for i, block := range blockChain {
		receipts := receiptChain[i]
		// Short circuit insertion if shutting down or processing failed
		if atomic.LoadInt32(&bc.procInterrupt) == 1 {
			return 0, nil
		}
		// Short circuit if the owner header is unknown
		if !bc.HasHeader(block.Hash(), block.NumberU64()) {
			return i, fmt.Errorf("containing header #%d [%x…] unknown", block.Number(), block.Hash().Bytes()[:4])
		}
		// Skip if the entire data is already known
		if bc.HasBlock(block.Hash(), block.NumberU64()) {
			stats.ignored++
			continue
		}
		// Compute all the non-consensus fields of the receipts
		if err := SetReceiptsData(bc.chainConfig, block, receipts); err != nil {
			return i, fmt.Errorf("failed to set receipts data: %v", err)
		}
		// Write all the data out into the database
		rawdb.WriteBody(batch, block.Hash(), block.NumberU64(), block.Body())
		rawdb.WriteReceipts(batch, block.Hash(), block.NumberU64(), receipts)
		rawdb.WriteTxLookupEntries(batch, block)

		stats.processed++

		if batch.ValueSize() >= ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return 0, err
			}
			bytes += batch.ValueSize()
			batch.Reset()
		}
	}
	if batch.ValueSize() > 0 {
		bytes += batch.ValueSize()
		if err := batch.Write(); err != nil {
			return 0, err
		}
	}

	// Update the head fast sync block if better
	bc.mu.Lock()
	head := blockChain[len(blockChain)-1]
	if td := bc.GetTd(head.Hash(), head.NumberU64()); td != nil { // Rewind may have occurred, skip in that case
		currentFastBlock := bc.CurrentFastBlock()
		if bc.GetTd(currentFastBlock.Hash(), currentFastBlock.NumberU64()).Cmp(td) < 0 {
			rawdb.WriteHeadFastBlockHash(bc.db, head.Hash())
			bc.currentFastBlock.Store(head)
		}
	}
	bc.mu.Unlock()

	context := []interface{}{
		"count", stats.processed, "elapsed", common.PrettyDuration(time.Since(start)),
		"number", head.Number(), "hash", head.Hash(), "age", common.PrettyAge(time.Unix(head.Time().Int64(), 0)),
		"size", common.StorageSize(bytes),
	}
	if stats.ignored > 0 {
		context = append(context, []interface{}{"ignored", stats.ignored}...)
	}
	log.Info("Imported new block receipts", context...)

	return 0, nil
}

var lastWrite uint64

// WriteBlockWithoutState writes only the block and its metadata to the database,
// but does not write any state. This is used to construct competing side forks
// up to the point where they exceed the canonical total difficulty.
func (bc *BlockChain) WriteBlockWithoutState(block *types.Block, td *big.Int) (err error) {
	bc.wg.Add(1)
	defer bc.wg.Done()

	if err := bc.hc.WriteTd(block.Hash(), block.NumberU64(), td); err != nil {
		return err
	}
	rawdb.WriteBlock(bc.db, block)

	return nil
}

// WriteBlockWithState writes the block and all associated state to the database.
func (bc *BlockChain) WriteBlockWithState(block *types.Block, receipts []*types.Receipt, state, privateState *state.StateDB) (status WriteStatus, err error) {
	bc.wg.Add(1)
	defer bc.wg.Done()

	// Calculate the total difficulty of the block
	ptd := bc.GetTd(block.ParentHash(), block.NumberU64()-1)
	if ptd == nil {
		return NonStatTy, consensus.ErrUnknownAncestor
	}
	// Make sure no inconsistent state is leaked during insertion
	bc.mu.Lock()
	defer bc.mu.Unlock()

	currentBlock := bc.CurrentBlock()
	localTd := bc.GetTd(currentBlock.Hash(), currentBlock.NumberU64())
	externTd := new(big.Int).Add(block.Difficulty(), ptd)

	// Irrelevant of the canonical status, write the block itself to the database
	if err := bc.hc.WriteTd(block.Hash(), block.NumberU64(), externTd); err != nil {
		return NonStatTy, err
	}
	rawdb.WriteBlock(bc.db, block)

	root, err := state.Commit(bc.chainConfig.IsEIP158(block.Number()))

	if err != nil {
		return NonStatTy, err
	}
	triedb := bc.stateCache.TrieDB()

	// Explicit commit for privateStateTriedb to handle Raft db issues
	if privateState != nil {
		privateRoot, err := privateState.Commit(bc.chainConfig.IsEIP158(block.Number()))
		if err != nil {
			return NonStatTy, err
		}
		privateTriedb := bc.privateStateCache.TrieDB()
		if err := privateTriedb.Commit(privateRoot, false); err != nil {
			return NonStatTy, err
		}
	}

	// If we're running an archive node, always flush
	if bc.cacheConfig.Disabled {
		if err := triedb.Commit(root, false); err != nil {
			return NonStatTy, err
		}

	} else {
		// Full but not archive node, do proper garbage collection
		triedb.Reference(root, common.Hash{}) // metadata reference to keep trie alive
		bc.triegc.Push(root, -int64(block.NumberU64()))

		if current := block.NumberU64(); current > triesInMemory {
			// If we exceeded our memory allowance, flush matured singleton nodes to disk
			var (
				nodes, imgs = triedb.Size()
				limit       = common.StorageSize(bc.cacheConfig.TrieNodeLimit) * 1024 * 1024
			)
			if nodes > limit || imgs > 4*1024*1024 {
				triedb.Cap(limit - ethdb.IdealBatchSize)
			}
			// Find the next state trie we need to commit
			header := bc.GetHeaderByNumber(current - triesInMemory)
			chosen := header.Number.Uint64()

			// If we exceeded out time allowance, flush an entire trie to disk
			if bc.gcproc > bc.cacheConfig.TrieTimeLimit {
				// If we're exceeding limits but haven't reached a large enough memory gap,
				// warn the user that the system is becoming unstable.
				if chosen < lastWrite+triesInMemory && bc.gcproc >= 2*bc.cacheConfig.TrieTimeLimit {
					log.Info("State in memory for too long, committing", "time", bc.gcproc, "allowance", bc.cacheConfig.TrieTimeLimit, "optimum", float64(chosen-lastWrite)/triesInMemory)
				}
				// Flush an entire trie and restart the counters
				triedb.Commit(header.Root, true)
				lastWrite = chosen
				bc.gcproc = 0
			}
			// Garbage collect anything below our required write retention
			for !bc.triegc.Empty() {
				root, number := bc.triegc.Pop()
				if uint64(-number) > chosen {
					bc.triegc.Push(root, number)
					break
				}
				triedb.Dereference(root.(common.Hash))
			}
		}
	}

	// Write other block data using a batch.
	batch := bc.db.NewBatch()
	rawdb.WriteReceipts(batch, block.Hash(), block.NumberU64(), receipts)

	// If the total difficulty is higher than our known, add it to the canonical chain
	// Second clause in the if statement reduces the vulnerability to selfish mining.
	// Please refer to http://www.cs.cornell.edu/~ie53/publications/btcProcFC.pdf
	reorg := externTd.Cmp(localTd) > 0
	currentBlock = bc.CurrentBlock()
	if !reorg && externTd.Cmp(localTd) == 0 {
		// Split same-difficulty blocks by number, then preferentially select
		// the block generated by the local miner as the canonical block.
		if block.NumberU64() < currentBlock.NumberU64() {
			reorg = true
		} else if block.NumberU64() == currentBlock.NumberU64() {
			var currentPreserve, blockPreserve bool
			if bc.shouldPreserve != nil {
				currentPreserve, blockPreserve = bc.shouldPreserve(currentBlock), bc.shouldPreserve(block)
			}
			reorg = !currentPreserve && (blockPreserve || mrand.Float64() < 0.5)
		}
	}
	if reorg {
		// Reorganise the chain if the parent is not the head block
		if block.ParentHash() != currentBlock.Hash() {
			if err := bc.reorg(currentBlock, block); err != nil {
				return NonStatTy, err
			}
		}
		// Write the positional metadata for transaction/receipt lookups and preimages
		rawdb.WriteTxLookupEntries(batch, block)
		rawdb.WritePreimages(batch, state.Preimages())

		status = CanonStatTy
	} else {
		status = SideStatTy
	}
	if err := batch.Write(); err != nil {
		return NonStatTy, err
	}

	// Set new head.
	if status == CanonStatTy {
		bc.insert(block)
	}
	bc.futureBlocks.Remove(block.Hash())
	return status, nil
}

// InsertChain attempts to insert the given batch of blocks in to the canonical
// chain or, otherwise, create a fork. If an error is returned it will return
// the index number of the failing block as well an error describing what went
// wrong.
//
// After insertion is done, all accumulated events will be fired.
func (bc *BlockChain) InsertChain(chain types.Blocks) (int, error) {
	n, events, logs, err := bc.insertChain(chain)
	bc.PostChainEvents(events, logs)
	return n, err
}

// Given a slice of public receipts and an overlapping (smaller) slice of
// private receipts, return a new slice where the default for each location is
// the public receipt but we take the private receipt in each place we have
// one.
func mergeReceipts(pub, priv types.Receipts) types.Receipts {
	m := make(map[common.Hash]*types.Receipt)
	for _, receipt := range pub {
		m[receipt.TxHash] = receipt
	}
	for _, receipt := range priv {
		m[receipt.TxHash] = receipt
	}

	ret := make(types.Receipts, 0, len(pub))
	for _, pubReceipt := range pub {
		ret = append(ret, m[pubReceipt.TxHash])
	}

	return ret
}

// insertChain will execute the actual chain insertion and event aggregation. The
// only reason this method exists as a separate one is to make locking cleaner
// with deferred statements.
func (bc *BlockChain) insertChain(chain types.Blocks) (int, []interface{}, []*types.Log, error) {
	// Sanity check that we have something meaningful to import
	if len(chain) == 0 {
		return 0, nil, nil, nil
	}
	// Do a sanity check that the provided chain is actually ordered and linked
	for i := 1; i < len(chain); i++ {
		if chain[i].NumberU64() != chain[i-1].NumberU64()+1 || chain[i].ParentHash() != chain[i-1].Hash() {
			// Chain broke ancestry, log a message (programming error) and skip insertion
			log.Error("Non contiguous block insert", "number", chain[i].Number(), "hash", chain[i].Hash(),
				"parent", chain[i].ParentHash(), "prevnumber", chain[i-1].Number(), "prevhash", chain[i-1].Hash())

			return 0, nil, nil, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, chain[i-1].NumberU64(),
				chain[i-1].Hash().Bytes()[:4], i, chain[i].NumberU64(), chain[i].Hash().Bytes()[:4], chain[i].ParentHash().Bytes()[:4])
		}
	}
	// Pre-checks passed, start the full block imports
	bc.wg.Add(1)
	defer bc.wg.Done()

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	// A queued approach to delivering events. This is generally
	// faster than direct delivery and requires much less mutex
	// acquiring.
	var (
		stats         = insertStats{startTime: mclock.Now()}
		events        = make([]interface{}, 0, len(chain))
		lastCanon     *types.Block
		coalescedLogs []*types.Log
	)
	// Start the parallel header verifier
	headers := make([]*types.Header, len(chain))
	seals := make([]bool, len(chain))

	for i, block := range chain {
		headers[i] = block.Header()
		seals[i] = true
	}
	abort, results := bc.engine.VerifyHeaders(bc, headers, seals)
	defer close(abort)

	// Start a parallel signature recovery (signer will fluke on fork transition, minimal perf loss)
	senderCacher.recoverFromBlocks(types.MakeSigner(bc.chainConfig, chain[0].Number()), chain)

	// Iterate over the blocks and insert when the verifier permits
	for i, block := range chain {
		// If the chain is terminating, stop processing blocks
		if atomic.LoadInt32(&bc.procInterrupt) == 1 {
			log.Debug("Premature abort during blocks processing")
			// QUORUM
			if bc.chainConfig.IsQuorum && bc.chainConfig.Istanbul == nil && bc.chainConfig.Clique == nil {
				// Only returns an error for raft mode
				return i, events, coalescedLogs, ErrAbortBlocksProcessing
			}
			// END QUORUM
			break
		}
		// If the header is a banned one, straight out abort
		if BadHashes[block.Hash()] {
			bc.reportBlock(block, nil, ErrBlacklistedHash)
			return i, events, coalescedLogs, ErrBlacklistedHash
		}
		// Wait for the block's verification to complete
		bstart := time.Now()

		err := <-results
		if err == nil {
			err = bc.Validator().ValidateBody(block)
		}
		switch {
		case err == ErrKnownBlock:
			// Block and state both already known. However if the current block is below
			// this number we did a rollback and we should reimport it nonetheless.
			if bc.CurrentBlock().NumberU64() >= block.NumberU64() {
				stats.ignored++
				continue
			}

		case err == consensus.ErrFutureBlock:
			// Allow up to MaxFuture second in the future blocks. If this limit is exceeded
			// the chain is discarded and processed at a later time if given.
			max := big.NewInt(time.Now().Unix() + maxTimeFutureBlocks)
			if block.Time().Cmp(max) > 0 && !bc.chainConfig.IsQuorum {
				return i, events, coalescedLogs, fmt.Errorf("future block: %v > %v", block.Time(), max)
			}
			bc.futureBlocks.Add(block.Hash(), block)
			stats.queued++
			continue

		case err == consensus.ErrUnknownAncestor && bc.futureBlocks.Contains(block.ParentHash()):
			bc.futureBlocks.Add(block.Hash(), block)
			stats.queued++
			continue

		case err == consensus.ErrPrunedAncestor:
			// Block competing with the canonical chain, store in the db, but don't process
			// until the competitor TD goes above the canonical TD
			currentBlock := bc.CurrentBlock()
			localTd := bc.GetTd(currentBlock.Hash(), currentBlock.NumberU64())
			externTd := new(big.Int).Add(bc.GetTd(block.ParentHash(), block.NumberU64()-1), block.Difficulty())
			if localTd.Cmp(externTd) > 0 {
				if err = bc.WriteBlockWithoutState(block, externTd); err != nil {
					return i, events, coalescedLogs, err
				}
				continue
			}
			// Competitor chain beat canonical, gather all blocks from the common ancestor
			var winner []*types.Block

			parent := bc.GetBlock(block.ParentHash(), block.NumberU64()-1)
			for !bc.HasState(parent.Root()) {
				winner = append(winner, parent)
				parent = bc.GetBlock(parent.ParentHash(), parent.NumberU64()-1)
			}
			for j := 0; j < len(winner)/2; j++ {
				winner[j], winner[len(winner)-1-j] = winner[len(winner)-1-j], winner[j]
			}
			// Import all the pruned blocks to make the state available
			bc.chainmu.Unlock()
			_, evs, logs, err := bc.insertChain(winner)
			bc.chainmu.Lock()
			events, coalescedLogs = evs, logs

			if err != nil {
				return i, events, coalescedLogs, err
			}

		case err != nil:
			bc.reportBlock(block, nil, err)
			return i, events, coalescedLogs, err
		}
		// Create a new statedb using the parent block and report an
		// error if it fails.
		var parent *types.Block
		if i == 0 {
			parent = bc.GetBlock(block.ParentHash(), block.NumberU64()-1)
		} else {
			parent = chain[i-1]
		}

		// alias state.New because we introduce a variable named state on the next line
		stateNew := state.New

		state, err := state.New(parent.Root(), bc.stateCache)
		if err != nil {
			return i, events, coalescedLogs, err
		}

		// Quorum
		privateStateRoot := GetPrivateStateRoot(bc.db, parent.Root())
		privateState, err := stateNew(privateStateRoot, bc.privateStateCache)
		if err != nil {
			return i, events, coalescedLogs, err
		}
		// /Quorum

		startRef := bc.GetBlockByHash(block.ParentHash()).RefNumberU64() + uint64(1)
		currRef := block.RefNumberU64()
		if !bc.ref && bc.myshard > uint64(0) {
			refNum := startRef
			for refNum <= currRef {
				_, status := bc.Dc(refNum)
				if !status {
					select {
					case <-bc.foreignDataCh:
						continue
					}
				}
				refNum++
			}
		}
		// Process block using the parent state as reference point.
		receipts, privateReceipts, logs, usedGas, err := bc.processor.Process(block, startRef, currRef, state, privateState, bc.vmConfig)
		if err != nil {
			bc.reportBlock(block, receipts, err)
			return i, events, coalescedLogs, err
		}
		// Validate the state using the default validator
		err = bc.Validator().ValidateState(block, parent, state, receipts, usedGas)
		if err != nil {
			bc.reportBlock(block, receipts, err)
			return i, events, coalescedLogs, err
		}

		// Quorum
		// Write private state changes to database
		if privateStateRoot, err = privateState.Commit(bc.Config().IsEIP158(block.Number())); err != nil {
			return i, events, coalescedLogs, err
		}
		if err := WritePrivateStateRoot(bc.db, block.Root(), privateStateRoot); err != nil {
			return i, events, coalescedLogs, err
		}
		allReceipts := mergeReceipts(receipts, privateReceipts)
		// /Quorum

		proctime := time.Since(bstart)

		// Write the block to the chain and get the status.
		status, err := bc.WriteBlockWithState(block, allReceipts, state, privateState)
		if err != nil {
			return i, events, coalescedLogs, err
		}
		if err := WritePrivateBlockBloom(bc.db, block.NumberU64(), privateReceipts); err != nil {
			return i, events, coalescedLogs, err
		}
		switch status {
		case CanonStatTy:
			log.Debug("Inserted new block", "number", block.Number(), "hash", block.Hash(), "uncles", len(block.Uncles()),
				"txs", len(block.Transactions()), "gas", block.GasUsed(), "elapsed", common.PrettyDuration(time.Since(bstart)))

			coalescedLogs = append(coalescedLogs, logs...)
			blockInsertTimer.UpdateSince(bstart)
			if !bc.ref {
				events = append(events, ChainEvent{block, block.Hash(), logs})
			}
			lastCanon = block

			// Only count canonical blocks for GC processing time
			bc.gcproc += proctime

			// Initializing the address of the state commitment transaction
			if block.NumberU64() == uint64(1) {
				if len(block.Transactions()) > 0 {
					bc.commitAddress = receipts[0].ContractAddress
				}
			}

			// Update the reference status of shards i.e., update which shards committed
			// new block and for which shard new cross-shard transactions were added!
			if bc.myshard == uint64(0) {
				bc.UpdateRefStatus(block, receipts)
			} else {
				// Parse transactions in reference chain to check for new state commitments
				if bc.ref {
					bc.ParseBlock(block, receipts)
				} else {
					bc.LogData(false, block, receipts)
				}
			}

		case SideStatTy:
			log.Debug("Inserted forked block", "number", block.Number(), "hash", block.Hash(), "diff", block.Difficulty(), "elapsed",
				common.PrettyDuration(time.Since(bstart)), "txs", len(block.Transactions()), "gas", block.GasUsed(), "uncles", len(block.Uncles()))

			blockInsertTimer.UpdateSince(bstart)
			if !bc.ref {
				events = append(events, ChainSideEvent{block})
			}
		}
		stats.processed++
		stats.usedGas += usedGas

		cache, _ := bc.stateCache.TrieDB().Size()
		stats.report(chain, i, cache)
	}
	// Append a single chain head event if we've progressed the chain
	if lastCanon != nil && bc.CurrentBlock().Hash() == lastCanon.Hash() {
		events = append(events, ChainHeadEvent{lastCanon})
	}

	return 0, events, coalescedLogs, nil
}

func (bc *BlockChain) addNewLocks(allKeys map[uint64][]*types.CKeys) {
	// This function assumes that the bc.gLocked.Mu is already held
	var addr common.Address
	for shard, sKeys := range allKeys {
		if _, sok := bc.lockedAddrMap[shard]; !sok {
			bc.lockedAddrMap[shard] = make(map[common.Address]bool)
		}
		// for every contract of a shard
		for _, cKeys := range sKeys {
			addr = cKeys.Addr
			if _, aok := bc.gLocked.Locks[addr]; !aok {
				bc.gLocked.Locks[addr] = types.NewCLock(addr)
			}
			if _, aok := bc.lockedAddrMap[shard][addr]; !aok {
				bc.lockedAddrMap[shard][addr] = false
			}
			// Add lock to all keys
			for _, key := range cKeys.Keys {
				if _, kok := bc.gLocked.Locks[addr].Keys[key]; !kok {
					bc.gLocked.Locks[addr].Keys[key] = 0
				}
				bc.gLocked.Locks[addr].Keys[key] = bc.gLocked.Locks[addr].Keys[key] + 1
			}
			// Mark write locks
			for _, key := range cKeys.WKeys {
				bc.gLocked.Locks[addr].Keys[key] = -1
			}
		}
	}
}

// LogData logs data of local blocks!
func (bc *BlockChain) LogData(self bool, block *types.Block, receipts types.Receipts) {
	// Logging local transaction!
	ltdata := bc.logdir + "ltdata"
	ltdataf, err := os.OpenFile(ltdata, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open ltdata file", "error", err)
	}
	defer ltdataf.Close()
	// Cross-shard Local data
	csltime := bc.logdir + "csltime"
	csltimef, err := os.OpenFile(csltime, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open csltime  file", "error", err)
	}
	defer csltimef.Close()
	// Parsing transactions!
	bNum := block.NumberU64()
	rNum := block.RefNumberU64()
	txs := block.Transactions()
	bHash := block.Hash().Hex()
	var txLen = 0
	for i, tx := range txs {
		receipt := receipts[i]
		txLen++
		fmt.Fprintln(ltdataf, bNum, bHash, rNum, tx.Hash().Hex(), tx.TxType(), receipt.Status, receipt.GasUsed, time.Now().Unix())
		if tx.TxType() == types.CrossShardLocal {
			fmt.Fprintln(csltimef, bNum, tx.Hash().Hex(), time.Now().Unix())
		}
	}
	// Logging information about the block!
	lbtime := bc.logdir + "lbtime"
	lbtimef, err := os.OpenFile(lbtime, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open lbitme file!", "error", err)
	}
	fmt.Fprintln(lbtimef, bNum, rNum, block.Hash().Hex(), block.Root().Hex(), block.GasUsed(), txLen, self, time.Now().Unix())
	lbtimef.Close()
}

// CheckGLock checks whether the global lock is held or not!
func (bc *BlockChain) CheckGLock(addr common.Address, addrKeys map[common.Hash]bool) bool {
	// This function assumes that bc.gLocked.Mu is already held!
	gLockedKeys := bc.gLocked.Locks[addr].Keys
	for key, kval := range addrKeys {
		gval, gok := gLockedKeys[key]
		// Globally locked and current write locked or globally write locked!
		if gok && (kval || gval < 0) {
			return true
		}
	}
	return false
}

// UpdateRefStatus updates current reference statsus
func (bc *BlockChain) UpdateRefStatus(block *types.Block, receipts types.Receipts) {
	bc.gLocked.Mu.Lock()
	defer bc.gLocked.Mu.Unlock()
	var (
		elemSize  = 32
		u64Offset = 24
		bNum      = block.NumberU64()
		receipt   *types.Receipt
		rStatus   bool
		tStatus   bool
		txType    uint64
		eventOut  uint64
	)
	tdata := bc.logdir + "tdata"
	tdataf, err := os.OpenFile(tdata, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open tdata file", "error", err)
	}
	defer tdataf.Close()
	ctxtime := bc.logdir + "ctxtime"
	ctxtimef, err := os.OpenFile(ctxtime, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open tdata file", "error", err)
	}
	defer ctxtimef.Close()
	// state commitment time!
	sctime := bc.logdir + "sctime"
	sctimef, err := os.OpenFile(sctime, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open tdata file", "error", err)
	}
	defer sctimef.Close()
	// Parsing trasnaction
	txs := block.Transactions()
	for i, tx := range txs {
		receipt = receipts[i]
		rStatus = receipt.Status == uint64(1)
		txType = tx.TxType()
		// Tranaction data file
		fmt.Fprintln(tdataf, bNum, tx.Hash().Hex(), txType, rStatus, receipt.GasUsed, time.Now().Unix())

		tStatus = false
		if rStatus && receipt.Logs != nil {
			if txType == types.CrossShard || txType == types.StateCommit {
				eventOut = binary.BigEndian.Uint64(receipt.Logs[0].Data[u64Offset:])
				tStatus = eventOut == uint64(1)
			} else {
				log.Debug("Not a relavent transaction", "status", rStatus, "txType", txType)
			}
		} else {
			log.Debug("Not parsing transaction", "rs", rStatus, "txType", txType, "logs", receipt.Logs)
		}

		if tStatus {
			if txType == types.CrossShard {
				// Marking the trasnaction as processed
				bc.AddProcessed(tx.Hash())
				// Updating latest cross-shard transaction for a shard
				data := tx.Data()[4:]
				_, shards, _ := types.DecodeCrossTx(uint64(0), data)
				for _, shard := range shards {
					bc.lastCtx[shard] = bNum
				}
				// Updating global locks based on the new cross-shard transaction
				numShards := len(shards)
				index := (2+1+numShards)*elemSize + elemSize + 2
				allKeys, _, _ := types.GetAllRWSet(uint16(numShards), data[index:])
				bc.addNewLocks(allKeys)
				// Logging data!
				// Cross-shard transaction file
				fmt.Fprintln(ctxtimef, bNum, tx.Hash().Hex(), numShards, time.Now().Unix())
			} else if txType == types.StateCommit {
				// Extracting data
				shard, commit, report, root, bHash := types.DecodeStateCommit(tx)
				// Unlocking keys due to state commit
				lockedAddrs, sok := bc.lockedAddrMap[shard]
				log.Debug("Unlocking locked keys!", "len", len(lockedAddrs))
				if sok && len(lockedAddrs) > 0 {
					for addr := range lockedAddrs {
						delete(bc.gLocked.Locks, addr)
					}
					delete(bc.lockedAddrMap, shard)
				}
				// Updating the latest commit of a shard
				lcommit := bc.lastCommit[shard]
				if report >= lcommit.RefNum {
					bc.lastCommit[shard] = &types.Commitment{Shard: shard, BlockNum: commit, RefNum: report, StateRoot: root, BHash: bHash} // Update last commit of a shard!
					fmt.Fprintln(sctimef, shard, commit, report, root.Hex(), bHash.Hex(), tx.Hash().Hex(), time.Now().Unix())
				}
			}
		}
	}
	// Logging summary of the block
	txLen := len(txs)
	rtime := bc.logdir + "rtime"
	rtimef, err := os.OpenFile(rtime, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open rtime file", "error", err)
	}
	fmt.Fprintln(rtimef, bNum, txLen, block.Hash().Hex(), block.Root().Hex(), block.GasLimit(), block.GasUsed(), time.Now().Unix())
	rtimef.Close()
}

// ParseBlock function extracts necessary information from a reference block
func (bc *BlockChain) ParseBlock(block *types.Block, receipts types.Receipts) {
	bc.gLocked.Mu.Lock()
	defer bc.gLocked.Mu.Unlock()
	elemSize := 32
	u64Offset := 24
	myshard := bc.MyShard()
	refNum := block.NumberU64()
	status := true

	if _, ok := bc.commitments[refNum]; !ok && refNum > uint64(0) {
		bc.commitments[refNum] = types.NewCommitments()
		bc.commitments[refNum].CopyCommits(bc.numShard, bc.commitments[refNum-1])
	}
	// Transactional information!
	tdata := bc.logdir + "tdata"
	tdataf, err := os.OpenFile(tdata, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open tdata file", "error", err)
	}
	defer tdataf.Close()
	// Cross-shard transaction file
	ctxtime := bc.logdir + "ctxtime"
	ctxtimef, err := os.OpenFile(ctxtime, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open tdata file", "error", err)
	}
	defer ctxtimef.Close()
	// state commitment time!
	sctime := bc.logdir + "sctime"
	sctimef, err := os.OpenFile(sctime, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open tdata file", "error", err)
	}
	defer sctimef.Close()
	// Parsing transaction!
	txs := block.Transactions()
	for i, tx := range txs {
		// Checking execution status of the transaction
		receipt := receipts[i]
		rStatus := receipt.Status == uint64(1)
		txType := tx.TxType()
		// Tranaction data file
		fmt.Fprintln(tdataf, refNum, tx.Hash().Hex(), txType, rStatus, receipt.GasUsed, time.Now().Unix())

		txStatus := false
		var eventOutput uint64
		if rStatus && receipt.Logs != nil {
			if txType == types.CrossShard || txType == types.StateCommit {
				eventOutput = binary.BigEndian.Uint64(receipt.Logs[0].Data[u64Offset:])
				txStatus = eventOutput == uint64(1)
			} else {
				log.Debug("rStatus passed", "txType", txType)
				continue
			}
		} else {
			log.Debug("Not parsing transaction", "rs", rStatus, "txType", txType, "logs", receipt.Logs)
		}

		if txStatus {
			if tx.TxType() == types.CrossShard {
				data := tx.Data()[4:]
				_, shardsInvolved, involved := types.DecodeCrossTx(myshard, data)
				if involved {
					status = false
					numShards := len(shardsInvolved)
					startIndex := (2+1+numShards)*elemSize + elemSize // Last 32 bytes to avoid string length
					crossTx := types.ParseCrossTxData(uint16(numShards), data[2+startIndex:])
					crossTx.BlockNum = block.Number()
					log.Debug("New cross shard transaction added!", "bn", refNum, "shards", shardsInvolved)

					if _, ok := bc.pendingCrossTxs[refNum]; !ok {
						bc.pendingCrossTxs[refNum] = types.NewCrossShardTxs()
					}
					bc.pendingCrossTxs[refNum].AddTransaction(uint64(i), crossTx)
					// Logging data!
					fmt.Fprintln(ctxtimef, refNum, tx.Hash().Hex(), crossTx.Tx.Hash().Hex(), numShards, time.Now().Unix())
				}
			} else if tx.TxType() == types.StateCommit {
				shard, commit, report, root, bHash := types.DecodeStateCommit(tx)
				if shard == bc.myshard {
					bc.myLatestCommit.Update(commit, report, root, bHash)
					log.Info("Updated Latest commit", "commit", commit, "report", report, "reporting", refNum, "root", root, "bHash", bHash)
					bc.CleanPendingTx(report)
				} else {
					tcommit := &types.Commitment{Shard: shard, BlockNum: commit, RefNum: report, StateRoot: root, BHash: bHash}
					bc.commitments[refNum].AddCommit(shard, tcommit)
					log.Debug("New commit added for ", "shard", shard, "committed", commit, "reporting", refNum, "root", root)
				}
				fmt.Fprintln(sctimef, shard, commit, report, root.Hex(), bHash.Hex(), tx.Hash().Hex(), time.Now().Unix())
			}
		} else {
			log.Info("Unsuccesful transaction execution!", "status", receipt.Status, "event", eventOutput, "txType", tx.TxType(), "hash", tx.Hash())
		}
	}
	bc.foreignDataMu.Lock()
	if _, ok := bc.foreignData[refNum]; !ok {
		bc.foreignData[refNum] = types.NewDataCache(refNum, status)
	}
	bc.foreignDataMu.Unlock()
	if _, ok := bc.pendingCrossTxs[refNum]; ok {
		status := bc.foreignData[refNum].InitKeys(bc.myshard, bc.pendingCrossTxs[refNum], bc.commitments[refNum])
		if status {
			go bc.PostForeignDataEvent(refNum)
		}
	}
	rtime := bc.logdir + "rtime"
	rtimef, err := os.OpenFile(rtime, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error("Can't open rtime file", "error", err)
	}
	txLen := len(txs)
	fmt.Fprintln(rtimef, refNum, txLen, block.Hash().Hex(), block.Root().Hex(), block.GasLimit(), block.GasUsed(), time.Now().Unix())
	rtimef.Close()
}

// CleanPendingTx removes commited cross-shard transactions
func (bc *BlockChain) CleanPendingTx(blockNum uint64) {
	for num := range bc.commitments {
		if num < blockNum {
			if _, ok := bc.commitments[num]; ok {
				delete(bc.commitments, num)
				log.Debug("Cleaning old commitments for", "rbn", num)
			}
		}
	}

	for num := range bc.pendingCrossTxs {
		if num < blockNum {
			if _, ok := bc.pendingCrossTxs[num]; ok {
				delete(bc.pendingCrossTxs, num)
				log.Debug("Cleaning cross shard transactions for", "rbn", num)
			}
		}
	}
}

// insertStats tracks and reports on block insertion.
type insertStats struct {
	queued, processed, ignored int
	usedGas                    uint64
	lastIndex                  int
	startTime                  mclock.AbsTime
}

// statsReportLimit is the time limit during import and export after which we
// always print out progress. This avoids the user wondering what's going on.
const statsReportLimit = 8 * time.Second

// report prints statistics if some number of blocks have been processed
// or more than a few seconds have passed since the last message.
func (st *insertStats) report(chain []*types.Block, index int, cache common.StorageSize) {
	// Fetch the timings for the batch
	var (
		now     = mclock.Now()
		elapsed = time.Duration(now) - time.Duration(st.startTime)
	)
	// If we're at the last block of the batch or report period reached, log
	if index == len(chain)-1 || elapsed >= statsReportLimit {
		var (
			end = chain[index]
			txs = countTransactions(chain[st.lastIndex : index+1])
		)
		context := []interface{}{
			"blocks", st.processed, "txs", txs, "mgas", float64(st.usedGas) / 1000000,
			"elapsed", common.PrettyDuration(elapsed), "mgasps", float64(st.usedGas) * 1000 / float64(elapsed),
			"number", end.Number(), "shard", end.Shard(), "hash", end.Hash(),
		}
		if timestamp := time.Unix(end.Time().Int64(), 0); time.Since(timestamp) > time.Minute {
			context = append(context, []interface{}{"age", common.PrettyAge(timestamp)}...)
		}
		context = append(context, []interface{}{"cache", cache}...)

		if st.queued > 0 {
			context = append(context, []interface{}{"queued", st.queued}...)
		}
		if st.ignored > 0 {
			context = append(context, []interface{}{"ignored", st.ignored}...)
		}
		log.Info("Imported new chain segment", context...)

		*st = insertStats{startTime: now, lastIndex: index + 1}
	}
}

func countTransactions(chain []*types.Block) (c int) {
	for _, b := range chain {
		c += len(b.Transactions())
	}
	return c
}

// reorgs takes two blocks, an old chain and a new chain and will reconstruct the blocks and inserts them
// to be part of the new canonical chain and accumulates potential missing transactions and post an
// event about them
func (bc *BlockChain) reorg(oldBlock, newBlock *types.Block) error {
	var (
		newChain    types.Blocks
		oldChain    types.Blocks
		commonBlock *types.Block
		deletedTxs  types.Transactions
		deletedLogs []*types.Log
		// collectLogs collects the logs that were generated during the
		// processing of the block that corresponds with the given hash.
		// These logs are later announced as deleted.
		collectLogs = func(hash common.Hash) {
			// Coalesce logs and set 'Removed'.
			number := bc.hc.GetBlockNumber(hash)
			if number == nil {
				return
			}
			receipts := rawdb.ReadReceipts(bc.db, hash, *number)
			for _, receipt := range receipts {
				for _, log := range receipt.Logs {
					del := *log
					del.Removed = true
					deletedLogs = append(deletedLogs, &del)
				}
			}
		}
	)

	// first reduce whoever is higher bound
	if oldBlock.NumberU64() > newBlock.NumberU64() {
		// reduce old chain
		for ; oldBlock != nil && oldBlock.NumberU64() != newBlock.NumberU64(); oldBlock = bc.GetBlock(oldBlock.ParentHash(), oldBlock.NumberU64()-1) {
			oldChain = append(oldChain, oldBlock)
			deletedTxs = append(deletedTxs, oldBlock.Transactions()...)

			collectLogs(oldBlock.Hash())
		}
	} else {
		// reduce new chain and append new chain blocks for inserting later on
		for ; newBlock != nil && newBlock.NumberU64() != oldBlock.NumberU64(); newBlock = bc.GetBlock(newBlock.ParentHash(), newBlock.NumberU64()-1) {
			newChain = append(newChain, newBlock)
		}
	}
	if oldBlock == nil {
		return fmt.Errorf("Invalid old chain")
	}
	if newBlock == nil {
		return fmt.Errorf("Invalid new chain")
	}

	for {
		if oldBlock.Hash() == newBlock.Hash() {
			commonBlock = oldBlock
			break
		}

		oldChain = append(oldChain, oldBlock)
		newChain = append(newChain, newBlock)
		deletedTxs = append(deletedTxs, oldBlock.Transactions()...)
		collectLogs(oldBlock.Hash())

		oldBlock, newBlock = bc.GetBlock(oldBlock.ParentHash(), oldBlock.NumberU64()-1), bc.GetBlock(newBlock.ParentHash(), newBlock.NumberU64()-1)
		if oldBlock == nil {
			return fmt.Errorf("Invalid old chain")
		}
		if newBlock == nil {
			return fmt.Errorf("Invalid new chain")
		}
	}
	// Ensure the user sees large reorgs
	if len(oldChain) > 0 && len(newChain) > 0 {
		logFn := log.Info
		if len(oldChain) > 63 {
			logFn = log.Warn
		}
		logFn("Chain split detected", "number", commonBlock.Number(), "hash", commonBlock.Hash(),
			"drop", len(oldChain), "dropfrom", oldChain[0].Hash(), "add", len(newChain), "addfrom", newChain[0].Hash())
	} else {
		log.Error("Impossible reorg, please file an issue", "oldnum", oldBlock.Number(), "oldhash", oldBlock.Hash(), "newnum", newBlock.Number(), "newhash", newBlock.Hash())
	}
	// Insert the new chain, taking care of the proper incremental order
	var addedTxs types.Transactions
	for i := len(newChain) - 1; i >= 0; i-- {
		// insert the block in the canonical way, re-writing history
		bc.insert(newChain[i])
		// write lookup entries for hash based transaction/receipt searches
		rawdb.WriteTxLookupEntries(bc.db, newChain[i])
		addedTxs = append(addedTxs, newChain[i].Transactions()...)
	}
	// calculate the difference between deleted and added transactions
	diff := types.TxDifference(deletedTxs, addedTxs)
	// When transactions get deleted from the database that means the
	// receipts that were created in the fork must also be deleted
	batch := bc.db.NewBatch()
	for _, tx := range diff {
		rawdb.DeleteTxLookupEntry(batch, tx.Hash())
	}
	batch.Write()

	if len(deletedLogs) > 0 {
		go bc.rmLogsFeed.Send(RemovedLogsEvent{deletedLogs})
	}
	if len(oldChain) > 0 {
		go func() {
			for _, block := range oldChain {
				bc.chainSideFeed.Send(ChainSideEvent{Block: block})
			}
		}()
	}

	return nil
}

// Ref returns whether a chain is a reference chain or not.
func (bc *BlockChain) Ref() bool {
	return bc.ref
}

// PostForeignDataEvent posts after downloading foreign data
func (bc *BlockChain) PostForeignDataEvent(refNum uint64) {
	bc.foreignDataFeed.Send(ForeignDataEvent{})
	select {
	case bc.foreignDataCh <- struct{}{}:
	default:
	}
}

// PostChainEvents iterates over the events generated by a chain insertion and
// posts them into the event feed.
// TODO: Should not expose PostChainEvents. The chain events should be posted in WriteBlock.
func (bc *BlockChain) PostChainEvents(events []interface{}, logs []*types.Log) {
	// post event logs for further processing
	if logs != nil {
		bc.logsFeed.Send(logs)
	}
	for _, event := range events {
		switch ev := event.(type) {
		case ChainEvent:
			bc.chainFeed.Send(ev)

		case ChainHeadEvent:
			bc.chainHeadFeed.Send(ev)

		case ChainSideEvent:
			bc.chainSideFeed.Send(ev)
		}
	}
}

func (bc *BlockChain) update() {
	futureTimer := time.NewTicker(5 * time.Second)
	defer futureTimer.Stop()
	for {
		select {
		case <-futureTimer.C:
			bc.procFutureBlocks()
		case <-bc.quit:
			return
		}
	}
}

// BadBlocks returns a list of the last 'bad blocks' that the client has seen on the network
func (bc *BlockChain) BadBlocks() []*types.Block {
	blocks := make([]*types.Block, 0, bc.badBlocks.Len())
	for _, hash := range bc.badBlocks.Keys() {
		if blk, exist := bc.badBlocks.Peek(hash); exist {
			block := blk.(*types.Block)
			blocks = append(blocks, block)
		}
	}
	return blocks
}

// HasBadBlock returns whether the block with the hash is a bad block. dep: Istanbul
func (bc *BlockChain) HasBadBlock(hash common.Hash) bool {
	return bc.badBlocks.Contains(hash)
}

// addBadBlock adds a bad block to the bad-block LRU cache
func (bc *BlockChain) addBadBlock(block *types.Block) {
	bc.badBlocks.Add(block.Hash(), block)
}

// reportBlock logs a bad block error.
func (bc *BlockChain) reportBlock(block *types.Block, receipts types.Receipts, err error) {
	bc.addBadBlock(block)

	var receiptString string
	for _, receipt := range receipts {
		receiptString += fmt.Sprintf("\t%v\n", receipt)
	}
	log.Error(fmt.Sprintf(`
########## BAD BLOCK #########
Chain config: %v

Number: %v
Hash: 0x%x
%v

Error: %v
##############################
`, bc.chainConfig, block.Number(), block.Hash(), receiptString, err))
}

// InsertHeaderChain attempts to insert the given header chain in to the local
// chain, possibly creating a reorg. If an error is returned, it will return the
// index number of the failing header as well an error describing what went wrong.
//
// The verify parameter can be used to fine tune whether nonce verification
// should be done or not. The reason behind the optional check is because some
// of the header retrieval mechanisms already need to verify nonces, as well as
// because nonces can be verified sparsely, not needing to check each.
func (bc *BlockChain) InsertHeaderChain(chain []*types.Header, checkFreq int) (int, error) {
	start := time.Now()
	if i, err := bc.hc.ValidateHeaderChain(chain, checkFreq); err != nil {
		return i, err
	}

	// Make sure only one thread manipulates the chain at once
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	bc.wg.Add(1)
	defer bc.wg.Done()

	whFunc := func(header *types.Header) error {
		bc.mu.Lock()
		defer bc.mu.Unlock()

		_, err := bc.hc.WriteHeader(header)
		return err
	}

	return bc.hc.InsertHeaderChain(chain, whFunc, start)
}

// writeHeader writes a header into the local chain, given that its parent is
// already known. If the total difficulty of the newly inserted header becomes
// greater than the current known TD, the canonical chain is re-routed.
//
// Note: This method is not concurrent-safe with inserting blocks simultaneously
// into the chain, as side effects caused by reorganisations cannot be emulated
// without the real blocks. Hence, writing headers directly should only be done
// in two scenarios: pure-header mode of operation (light clients), or properly
// separated header/block phases (non-archive clients).
func (bc *BlockChain) writeHeader(header *types.Header) error {
	bc.wg.Add(1)
	defer bc.wg.Done()

	bc.mu.Lock()
	defer bc.mu.Unlock()

	_, err := bc.hc.WriteHeader(header)
	return err
}

// CurrentHeader retrieves the current head header of the canonical chain. The
// header is retrieved from the HeaderChain's internal cache.
func (bc *BlockChain) CurrentHeader() *types.Header {
	return bc.hc.CurrentHeader()
}

// GetTd retrieves a block's total difficulty in the canonical chain from the
// database by hash and number, caching it if found.
func (bc *BlockChain) GetTd(hash common.Hash, number uint64) *big.Int {
	return bc.hc.GetTd(hash, number)
}

// GetTdByHash retrieves a block's total difficulty in the canonical chain from the
// database by hash, caching it if found.
func (bc *BlockChain) GetTdByHash(hash common.Hash) *big.Int {
	return bc.hc.GetTdByHash(hash)
}

// GetHeader retrieves a block header from the database by hash and number,
// caching it if found.
func (bc *BlockChain) GetHeader(hash common.Hash, number uint64) *types.Header {
	return bc.hc.GetHeader(hash, number)
}

// GetHeaderByHash retrieves a block header from the database by hash, caching it if
// found.
func (bc *BlockChain) GetHeaderByHash(hash common.Hash) *types.Header {
	return bc.hc.GetHeaderByHash(hash)
}

// HasHeader checks if a block header is present in the database or not, caching
// it if present.
func (bc *BlockChain) HasHeader(hash common.Hash, number uint64) bool {
	return bc.hc.HasHeader(hash, number)
}

// GetBlockHashesFromHash retrieves a number of block hashes starting at a given
// hash, fetching towards the genesis block.
func (bc *BlockChain) GetBlockHashesFromHash(hash common.Hash, max uint64) []common.Hash {
	return bc.hc.GetBlockHashesFromHash(hash, max)
}

// GetAncestor retrieves the Nth ancestor of a given block. It assumes that either the given block or
// a close ancestor of it is canonical. maxNonCanonical points to a downwards counter limiting the
// number of blocks to be individually checked before we reach the canonical chain.
//
// Note: ancestor == 0 returns the same block, 1 returns its parent and so on.
func (bc *BlockChain) GetAncestor(hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	return bc.hc.GetAncestor(hash, number, ancestor, maxNonCanonical)
}

// GetHeaderByNumber retrieves a block header from the database by number,
// caching it (associated with its hash) if found.
func (bc *BlockChain) GetHeaderByNumber(number uint64) *types.Header {
	return bc.hc.GetHeaderByNumber(number)
}

// Config retrieves the blockchain's chain configuration.
func (bc *BlockChain) Config() *params.ChainConfig { return bc.chainConfig }

// Engine retrieves the blockchain's consensus engine.
func (bc *BlockChain) Engine() consensus.Engine { return bc.engine }

// SubscribeRemovedLogsEvent registers a subscription of RemovedLogsEvent.
func (bc *BlockChain) SubscribeRemovedLogsEvent(ch chan<- RemovedLogsEvent) event.Subscription {
	return bc.scope.Track(bc.rmLogsFeed.Subscribe(ch))
}

// SubscribeChainEvent registers a subscription of ChainEvent.
func (bc *BlockChain) SubscribeChainEvent(ch chan<- ChainEvent) event.Subscription {
	return bc.scope.Track(bc.chainFeed.Subscribe(ch))
}

// SubscribeChainHeadEvent registers a subscription of ChainHeadEvent.
func (bc *BlockChain) SubscribeChainHeadEvent(ch chan<- ChainHeadEvent) event.Subscription {
	return bc.scope.Track(bc.chainHeadFeed.Subscribe(ch))
}

// SubscribeForeignDataEvent registers a foriegn data signal
func (bc *BlockChain) SubscribeForeignDataEvent(ch chan<- ForeignDataEvent) event.Subscription {
	return bc.scope.Track(bc.foreignDataFeed.Subscribe(ch))
}

// SubscribeChainSideEvent registers a subscription of ChainSideEvent.
func (bc *BlockChain) SubscribeChainSideEvent(ch chan<- ChainSideEvent) event.Subscription {
	return bc.scope.Track(bc.chainSideFeed.Subscribe(ch))
}

// SubscribeLogsEvent registers a subscription of []*types.Log.
func (bc *BlockChain) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
	return bc.scope.Track(bc.logsFeed.Subscribe(ch))
}
