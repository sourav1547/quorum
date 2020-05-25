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
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
)

var (
	errClosed            = errors.New("peer set is closed")
	errShardClosed       = errors.New("shard peer set is closed")
	errAlreadyRegistered = errors.New("peer is already registered")
	errNotRegistered     = errors.New("peer is not registered")
)

const (
	maxKnownTxs    = 32768 // Maximum transactions hashes to keep in the known list (prevent DOS)
	maxKnownBlocks = 1024  // Maximum block hashes to keep in the known list (prevent DOS)

	// maxQueuedTxs is the maximum number of transaction lists to queue up before
	// dropping broadcasts. This is a sensitive number as a transaction list might
	// contain a single transaction, or thousands.
	maxQueuedTxs = 128

	// maxQueuedProps is the maximum number of block propagations to queue up before
	// dropping broadcasts. There's not much point in queueing stale blocks, so a few
	// that might cover uncles should be enough.
	maxQueuedProps = 4

	// maxQueuedAnns is the maximum number of block announcements to queue up before
	// dropping broadcasts. Similarly to block propagations, there's no point to queue
	// above some healthy uncle limit, so use that.
	maxQueuedAnns = 4

	handshakeTimeout = 5 * time.Second
)

// PeerInfo represents a short summary of the Ethereum sub-protocol metadata known
// about a connected peer.
type PeerInfo struct {
	Shard       uint64   `json:"shard"`      // Shard number of the peer
	Version     int      `json:"version"`    // Ethereum protocol version negotiated
	Difficulty  *big.Int `json:"difficulty"` // Total difficulty of the peer's blockchain
	RDifficulty *big.Int `json:"rdifficulty"`
	Head        string   `json:"head"` // SHA3 hash of the peer's best owned block
	RHead       string   `json:"rhead"`
}

// propEvent is a block propagation, waiting for its turn in the broadcast queue.
type propEvent struct {
	ref   bool
	block *types.Block
	td    *big.Int
}

type peer struct {
	id string

	*p2p.Peer
	rw p2p.MsgReadWriter

	shard    uint64
	version  int         // Protocol version negotiated
	forkDrop *time.Timer // Timed connection dropper if forks aren't validated in time

	head  common.Hash
	rHead common.Hash
	td    *big.Int
	rTd   *big.Int
	lock  sync.RWMutex
	rlock sync.RWMutex

	knownTxs     mapset.Set                // Set of transaction hashes known to be known by this peer
	knownBlocks  mapset.Set                // Set of block hashes known to be known by this peer
	rKnownBlocks mapset.Set                // Set of Reference block hashes known by this peer
	queuedTxs    chan []*types.Transaction // Queue of transactions to broadcast to the peer
	queuedProps  chan *propEvent           // Queue of blocks to broadcast to the peer
	queuedAnns   chan *types.Block         // Queue of blocks to announce to the peer
	term         chan struct{}             // Termination channel to stop the broadcaster
}

func newPeer(shard uint64, version int, p *p2p.Peer, rw p2p.MsgReadWriter) *peer {
	return &peer{
		Peer:         p,
		rw:           rw,
		shard:        shard,
		version:      version,
		id:           fmt.Sprintf("%x", p.ID().Bytes()[:8]),
		knownTxs:     mapset.NewSet(),
		knownBlocks:  mapset.NewSet(),
		rKnownBlocks: mapset.NewSet(),
		queuedTxs:    make(chan []*types.Transaction, maxQueuedTxs),
		queuedProps:  make(chan *propEvent, maxQueuedProps),
		queuedAnns:   make(chan *types.Block, maxQueuedAnns),
		term:         make(chan struct{}),
	}
}

// broadcast is a write loop that multiplexes block propagations, announcements
// and transaction broadcasts into the remote peer. The goal is to have an async
// writer that does not lock up node internals.
func (p *peer) broadcast() {
	for {
		select {
		case txs := <-p.queuedTxs:
			if err := p.SendTransactions(txs); err != nil {
				return
			}
			p.Log().Trace("Broadcast transactions", "count", len(txs))

		case prop := <-p.queuedProps:
			if err := p.SendNewBlock(prop.ref, prop.block, prop.td); err != nil {
				return
			}
			p.Log().Trace("Propagated block", "number", prop.block.Number(), "hash", prop.block.Hash(), "td", prop.td)

		case block := <-p.queuedAnns:
			ref := block.Shard() == uint64(0) && p.Shard() > uint64(0)
			if err := p.SendNewBlockHashes(ref, []common.Hash{block.Hash()}, []uint64{block.NumberU64()}); err != nil {
				return
			}

		case <-p.term:
			return
		}
	}
}

// close signals the broadcast goroutine to terminate.
func (p *peer) close() {
	close(p.term)
}

// Info gathers and returns a collection of metadata known about a peer.
func (p *peer) Info() *PeerInfo {
	hash, td := p.Head(false)
	rHash, rTd := p.Head(true)

	return &PeerInfo{
		Shard:       p.shard,
		Version:     p.version,
		Difficulty:  td,
		RDifficulty: rTd,
		RHead:       rHash.Hex(),
		Head:        hash.Hex(),
	}
}

func (p *peer) Shard() uint64 {
	return p.shard
}

// Head retrieves a copy of the current head hash and total difficulty of the
// peer.
// @sourav, todo: fix this
func (p *peer) Head(ref bool) (hash common.Hash, td *big.Int) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if ref {
		copy(hash[:], p.rHead[:])
		return hash, new(big.Int).Set(p.rTd)
	} else {
		copy(hash[:], p.head[:])
		return hash, new(big.Int).Set(p.td)
	}
}

// SetHead updates the head hash and total difficulty of the peer.
func (p *peer) SetHead(ref bool, hash common.Hash, td *big.Int) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if ref {
		copy(p.rHead[:], hash[:])
		p.rTd.Set(td)
	} else {
		copy(p.head[:], hash[:])
		p.td.Set(td)
	}
}

// MarkBlock marks a block as known for the peer, ensuring that the block will
// never be propagated to this particular peer.
func (p *peer) MarkBlock(ref bool, hash common.Hash) {
	if ref {
		// If we reached the memory allowance, drop a previously known block hash
		for p.rKnownBlocks.Cardinality() >= maxKnownBlocks {
			p.rKnownBlocks.Pop()
		}
		p.rKnownBlocks.Add(hash)
	} else {
		// If we reached the memory allowance, drop a previously known block hash
		for p.knownBlocks.Cardinality() >= maxKnownBlocks {
			p.knownBlocks.Pop()
		}
		p.knownBlocks.Add(hash)
	}
}

// MarkTransaction marks a transaction as known for the peer, ensuring that it
// will never be propagated to this particular peer.
func (p *peer) MarkTransaction(hash common.Hash) {
	// If we reached the memory allowance, drop a previously known transaction hash
	for p.knownTxs.Cardinality() >= maxKnownTxs {
		p.knownTxs.Pop()
	}
	p.knownTxs.Add(hash)
}

// Send writes an RLP-encoded message with the given code.
// data should encode as an RLP list.
func (p *peer) Send(msgcode uint64, data interface{}) error {
	return p2p.Send(p.rw, msgcode, data)
}

// SendTransactions sends transactions to the peer and includes the hashes
// in its transaction hash set for future reference.
func (p *peer) SendTransactions(txs types.Transactions) error {
	for _, tx := range txs {
		p.knownTxs.Add(tx.Hash())
	}
	return p2p.Send(p.rw, TxMsg, txs)
}

// AsyncSendTransactions queues list of transactions propagation to a remote
// peer. If the peer's broadcast queue is full, the event is silently dropped.
func (p *peer) AsyncSendTransactions(txs []*types.Transaction) {
	select {
	case p.queuedTxs <- txs:
		for _, tx := range txs {
			p.knownTxs.Add(tx.Hash())
		}
	default:
		p.Log().Debug("Dropping transaction propagation", "count", len(txs))
	}
}

// SendNewBlockHashes announces the availability of a number of blocks through
// a hash notification.
func (p *peer) SendNewBlockHashes(ref bool, hashes []common.Hash, numbers []uint64) error {
	if ref {
		for _, hash := range hashes {
			p.rKnownBlocks.Add(hash)
		}
	} else {
		for _, hash := range hashes {
			p.knownBlocks.Add(hash)
		}
	}
	request := make(newBlockHashesData, len(hashes))
	for i := 0; i < len(hashes); i++ {
		request[i].Ref = ref
		request[i].Hash = hashes[i]
		request[i].Number = numbers[i]
	}
	return p2p.Send(p.rw, NewBlockHashesMsg, request)
}

// AsyncSendNewBlockHash queues the availability of a block for propagation to a
// remote peer. If the peer's broadcast queue is full, the event is silently
// dropped.
func (p *peer) AsyncSendNewBlockHash(ref bool, block *types.Block) {
	select {
	case p.queuedAnns <- block:
		if ref {
			p.rKnownBlocks.Add(block.Hash())
		} else {
			p.knownBlocks.Add(block.Hash())
		}
	default:
		p.Log().Debug("Dropping block announcement", "number", block.NumberU64(), "hash", block.Hash())
	}
}

// SendNewBlock propagates an entire block to a remote peer.
func (p *peer) SendNewBlock(ref bool, block *types.Block, td *big.Int) error {
	if ref {
		p.rKnownBlocks.Add(block.Hash())
	} else {
		p.knownBlocks.Add(block.Hash())
	}
	return p2p.Send(p.rw, NewBlockMsg, []interface{}{block, td})
}

// AsyncSendNewBlock queues an entire block for propagation to a remote peer. If
// the peer's broadcast queue is full, the event is silently dropped.
func (p *peer) AsyncSendNewBlock(ref bool, block *types.Block, td *big.Int) {
	select {
	case p.queuedProps <- &propEvent{ref: ref, block: block, td: td}:
		if ref {
			p.rKnownBlocks.Add(block.Hash())
		} else {
			p.knownBlocks.Add(block.Hash())
		}
	default:
		p.Log().Debug("Dropping block propagation", "number", block.NumberU64(), "hash", block.Hash())
	}
}

// SendDataRequest sends a data request to remote peer
func (p *peer) SendDataRequest(refNum, count uint64, root common.Hash, keys []*types.CKeys) error {
	return p2p.Send(p.rw, GetStateDataMsg, &getStateData{Root: root, RefNum: refNum, Count: count, Keys: keys})
}

// SendDataResponse sends data
func (p *peer) SendDataResponse(refNum, count uint64, root common.Hash, vals []*types.KeyVal) error {
	return p2p.Send(p.rw, StateDataMsg, &stateData{Root: root, RefNum: refNum, Count: count, Vals: vals})
}

// SendBlockHeaders sends a batch of block headers to the remote peer.
func (p *peer) SendBlockHeaders(headers []*types.Header) error {
	return p2p.Send(p.rw, BlockHeadersMsg, headers)
}

// SendBlockBodies sends a batch of block contents to the remote peer.
func (p *peer) SendBlockBodies(bodies []*blockBody) error {
	return p2p.Send(p.rw, BlockBodiesMsg, blockBodiesData(bodies))
}

// SendBlockBodiesRLP sends a batch of block contents to the remote peer from
// an already RLP encoded format.
func (p *peer) SendBlockBodiesRLP(bodies []rlp.RawValue) error {
	return p2p.Send(p.rw, BlockBodiesMsg, bodies)
}

// SendNodeDataRLP sends a batch of arbitrary internal data, corresponding to the
// hashes requested.
func (p *peer) SendNodeData(data [][]byte) error {
	return p2p.Send(p.rw, NodeDataMsg, data)
}

// SendReceiptsRLP sends a batch of transaction receipts, corresponding to the
// ones requested from an already RLP encoded format.
func (p *peer) SendReceiptsRLP(receipts []rlp.RawValue) error {
	return p2p.Send(p.rw, ReceiptsMsg, receipts)
}

// RequestOneHeader is a wrapper around the header query functions to fetch a
// single header. It is used solely by the fetcher.
func (p *peer) RequestOneHeader(shard uint64, hash common.Hash) error {
	p.Log().Debug("Fetching single header", "hash", hash)
	return p2p.Send(p.rw, GetBlockHeadersMsg, &getBlockHeadersData{Origin: hashOrNumber{Hash: hash}, Amount: uint64(1), Skip: uint64(0), Reverse: false, Shard: shard})
}

// RequestHeadersByHash fetches a batch of blocks' headers corresponding to the
// specified header query, based on the hash of an origin block.
func (p *peer) RequestHeadersByHash(shard uint64, origin common.Hash, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromhash", origin, "skip", skip, "reverse", reverse)
	return p2p.Send(p.rw, GetBlockHeadersMsg, &getBlockHeadersData{Origin: hashOrNumber{Hash: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse, Shard: shard})
}

// RequestHeadersByNumber fetches a batch of blocks' headers corresponding to the
// specified header query, based on the number of an origin block.
func (p *peer) RequestHeadersByNumber(shard uint64, origin uint64, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromnum", origin, "skip", skip, "reverse", reverse)
	return p2p.Send(p.rw, GetBlockHeadersMsg, &getBlockHeadersData{Origin: hashOrNumber{Number: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse, Shard: shard})
}

// RequestBodies fetches a batch of blocks' bodies corresponding to the hashes
// specified.
func (p *peer) RequestBodies(shard uint64, hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of block bodies", "count", len(hashes))
	return p2p.Send(p.rw, GetBlockBodiesMsg, &getBlockBodiesData{Hashes: hashes, Shard: shard})
}

// RequestNodeData fetches a batch of arbitrary data from a node's known state
// data, corresponding to the specified hashes.
func (p *peer) RequestNodeData(hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of state data", "count", len(hashes))
	return p2p.Send(p.rw, GetNodeDataMsg, hashes)
}

// RequestReceipts fetches a batch of transaction receipts from a remote node.
func (p *peer) RequestReceipts(shard uint64, hashes []common.Hash) error {
	p.Log().Debug("Fetching batch of receipts", "count", len(hashes))
	return p2p.Send(p.rw, GetReceiptsMsg, getReceiptsData{Hashes: hashes, Shard: shard})
}

// Handshake executes the eth protocol handshake, negotiating version number,
// network IDs, difficulties, head and genesis blocks.
func (p *peer) Handshake(shard, network uint64, td, rTd *big.Int, head, rHead common.Hash, genesis common.Hash) error {
	// Send out own handshake in a new thread
	errc := make(chan error, 2)
	var status statusData // safe to read after two values have been received from errc

	go func() {
		errc <- p2p.Send(p.rw, StatusMsg, &statusData{
			ProtocolVersion: uint32(p.version),
			ShardId:         shard,
			NetworkId:       network,
			TD:              td,
			RTD:             rTd,
			CurrentBlock:    head,
			RCurrentBlock:   rHead,
			GenesisBlock:    genesis,
		})
	}()
	go func() {
		errc <- p.readStatus(network, &status, genesis)
	}()
	timeout := time.NewTimer(handshakeTimeout)
	defer timeout.Stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				return err
			}
		case <-timeout.C:
			return p2p.DiscReadTimeout
		}
	}

	if shard == status.ShardId {
		p.td, p.head = status.TD, status.CurrentBlock
		if shard > uint64(0) {
			p.rTd, p.rHead = status.RTD, status.RCurrentBlock
		}
	} else {
		if status.ShardId == uint64(0) {
			p.rTd, p.rHead = status.TD, status.CurrentBlock
		} else {
			p.rTd, p.rHead = status.RTD, status.RCurrentBlock
		}
	}
	return nil
}

func (p *peer) readStatus(network uint64, status *statusData, genesis common.Hash) (err error) {
	msg, err := p.rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Code != StatusMsg {
		return errResp(ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, StatusMsg)
	}
	if msg.Size > ProtocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	// Decode the handshake and make sure everything matches
	if err := msg.Decode(&status); err != nil {
		return errResp(ErrDecode, "msg %v: %v", msg, err)
	}
	if status.GenesisBlock != genesis {
		return errResp(ErrGenesisBlockMismatch, "%x (!= %x)", status.GenesisBlock[:8], genesis[:8])
	}
	if status.NetworkId != network {
		return errResp(ErrNetworkIdMismatch, "%d (!= %d)", status.NetworkId, network)
	}
	if int(status.ProtocolVersion) != p.version {
		return errResp(ErrProtocolVersionMismatch, "%d (!= %d)", status.ProtocolVersion, p.version)
	}
	return nil
}

// String implements fmt.Stringer.
func (p *peer) String() string {
	return fmt.Sprintf("Peer %s [%s]", p.id,
		fmt.Sprintf("eth/%2d", p.version),
	)
}

// peerSet represents the collection of active peers currently participating in
// the Ethereum sub-protocol.
type peerSet struct {
	peers  map[string]*peer
	lock   sync.RWMutex
	closed bool
}

// shardPeerSet represents on the collection of active peers
// type shardPeerSet struct {
// 	shardPeers map[uint64]*peerSet
// }

// // return new shard peers set
// func newShardPeerSet() *shardPeerSet {
// 	return &shardPeerSet{
// 		shardPeers: make(map[uint64]*peerSet),
// 	}
// }

// newPeerSet creates a new peer set to track the active participants.
func newPeerSet() *peerSet {
	return &peerSet{
		peers: make(map[string]*peer),
		lock:  sync.RWMutex{},
	}
}

// Register injects a new peer into the working set, or returns an error if the
// peer is already known. If a new peer it registered, its broadcast loop is also
// started.
func (ps *peerSet) Register(p *peer) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if ps.closed {
		return errClosed
	}
	if _, ok := ps.peers[p.id]; ok {
		return errAlreadyRegistered
	}
	ps.peers[p.id] = p
	go p.broadcast()

	return nil
}

// func (sps *shardPeerSet) Register(shard uint64, p *peer) error {
// 	return sps.shardPeers[shard].Register(p)
// }

// Unregister removes a remote peer from the active set, disabling any further
// actions to/from that particular entity.
func (ps *peerSet) Unregister(id string) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	p, ok := ps.peers[id]
	if !ok {
		return errNotRegistered
	}
	delete(ps.peers, id)
	p.close()

	return nil
}

// func (sps *shardPeerSet) Unregister(shard uint64, id string) error {
// 	return sps.shardPeers[shard].Unregister(id)
// }

// // Return all registered peers for a shard
// func (sps *shardPeerSet) Peers(shard uint64) map[string]*peer {
// 	return sps.shardPeers[shard].Peers()
// }

// Peers returns all registered peers
func (ps *peerSet) Peers() map[string]*peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	set := make(map[string]*peer)
	for id, p := range ps.peers {
		set[id] = p
	}
	return set
}

// Retrieve a peer from a specific shard
// func (sps *shardPeerSet) Peer(shard uint64, id string) *peer {
// 	return sps.shardPeers[shard].Peer(id)
// }

// Peer retrieves the registered peer with the given id.
func (ps *peerSet) Peer(id string) *peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return ps.peers[id]
}

// Current number of peers per shard
// func (sps *shardPeerSet) Len(shard uint64) int {
// 	return sps.shardPeers[shard].Len()
// }

// Total number of peers accross all shard
// func (sps *shardPeerSet) TotalLen() int {
// 	total := 0
// 	for _, v := range sps.shardPeers {
// 		total = total + v.Len()
// 	}
// 	return total
// }

// Len returns if the current number of peers in the set.
func (ps *peerSet) Len() int {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return len(ps.peers)
}

// PeersWithoutBlock retrieves a list of peers that do not have a given block in
// their set of known hashes.
func (ps *peerSet) PeersWithoutBlock(hash common.Hash) []*peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*peer, 0, len(ps.peers))
	for _, p := range ps.peers {
		if !p.knownBlocks.Contains(hash) {
			list = append(list, p)
		}
	}
	return list
}

// PeersWithoutRequest retrieves a list of peers to whom the
// the data request are not sent yet
// @sourav, todo: as of now we are returning the entire
// list of peers, we have to fix this later!
func (ps *peerSet) PeersWithoutRequest(number uint64) []*peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*peer, 0, len(ps.peers))
	for _, p := range ps.peers {
		list = append(list, p)
	}
	return list
}

// PeersWithoutTx retrieves a list of peers that do not have a given transaction
// in their set of known hashes.
func (ps *peerSet) PeersWithoutTx(hash common.Hash) []*peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	list := make([]*peer, 0, len(ps.peers))
	for _, p := range ps.peers {
		if !p.knownTxs.Contains(hash) {
			list = append(list, p)
		}
	}
	return list
}

// BestPeer retrieves the known peer with the currently highest total difficulty.
func (ps *peerSet) BestPeer(ref bool) *peer {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	var (
		bestPeer *peer
		bestTd   *big.Int
	)
	for _, p := range ps.peers {
		if _, td := p.Head(ref); bestPeer == nil || td.Cmp(bestTd) > 0 {
			bestPeer, bestTd = p, td
		}
	}
	return bestPeer
}

// Close disconnects all peers.
// No new peers can be registered after Close has returned.
func (ps *peerSet) Close() {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	for _, p := range ps.peers {
		p.Disconnect(p2p.DiscQuitting)
	}
	ps.closed = true
}

// func (sps *shardPeerSet) Close(shard uint64) {
// 	sps.shardPeers[shard].Close()
// }

// func (sps *shardPeerSet) CloseAll() {
// 	for _, v := range sps.shardPeers {
// 		v.Close()
// 	}
// }
