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

package core

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, start, end uint64, statedb, privateState *state.StateDB, cfg vm.Config) (types.Receipts, types.Receipts, []*types.Log, uint64, error) {

	var (
		receipts types.Receipts
		usedGas  = new(uint64)
		header   = block.Header()
		allLogs  []*types.Log
		gp       = new(GasPool).AddGas(block.GasLimit())
		dc       *types.DataCache
		curr     = start
		txType   uint64

		privateReceipts types.Receipts
	)
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		txType = tx.TxType()
		statedb.Prepare(tx.Hash(), block.Hash(), i)
		privateState.Prepare(tx.Hash(), block.Hash(), i)

		snap := statedb.Snapshot()
		psnap := privateState.Snapshot()

		if txType != types.CrossShardLocal {
			dc = nil
		} else {
			for curr <= end {
				found := false
				// @sourav, todo: Add locks for pendingCrossTxs map
				for _, ctx := range p.bc.pendingCrossTxs[curr].Txs {
					if tx.Hash() == ctx.Tx.Hash() {
						p.bc.foreignDataMu.RLock()
						dc = p.bc.foreignData[curr]
						p.bc.foreignDataMu.RUnlock()
						found = true
						log.Debug("Cross shard Transaction with", "hash", tx.Hash(), "refNum", curr)
						break
					}
				}
				if found {
					break
				}
				curr++
			}
		}

		// s1 := statedb.Copy()
		receipt, privateReceipt, _, err := ApplyTransaction(p.config, p.bc, nil, gp, dc, statedb, privateState, header, tx, usedGas, cfg)
		// s2 := statedb.Copy()
		if txType == types.CrossShardLocal && err != nil {
			statedb.RevertToSnapshot(snap)
			privateState.RevertToSnapshot(psnap)
			log.Warn("Skipping transaction", "thash", tx.Hash(), "from", tx.From(), "error", err)

			// Creating a dummy receipt
			root := statedb.IntermediateRoot(false)
			receipt = types.NewReceipt(root.Bytes(), false, *usedGas)
			receipt.TxHash = tx.Hash()
			receipt.GasUsed = tx.Gas()
			// Set the receipt logs and create a bloom for filtering
			receipt.Logs = statedb.GetLogs(tx.Hash())
			receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		} else {
			if err != nil {
				return nil, nil, nil, 0, err
			}
		}

		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)

		// if the private receipt is nil this means the tx was public
		// and we do not need to apply the additional logic.
		if privateReceipt != nil {
			privateReceipts = append(privateReceipts, privateReceipt)
			allLogs = append(allLogs, privateReceipt.Logs...)
		}

		// if header.Shard > uint64(0) {
		// 	log.Info("@ds Process Tx ", "s1", s1.IntermediateRoot(false), "s2", s2.IntermediateRoot(false))
		// }
	}
	// Update locked status
	if p.bc.myshard == uint64(0) {
		var (
			txType      uint64
			index       = 4
			receipt     *types.Receipt
			status      bool             // receipt status
			lockedAddrs []common.Address // locked address of a shard
			shards      []uint64         // shards involved in a cross shard tx
			numShard    int
			shard       uint64
			sok         bool
			elemSize    = 32
		)
		for i, tx := range block.Transactions() {
			txType = tx.TxType()
			receipt = receipts[i]
			status = receipt.Status == uint64(1)
			if status && txType == types.StateCommit {
				shard, _, _ = types.DecodeStateCommit(tx)

				p.bc.lockedAddrMapMu.RLock()
				lockedAddrs, sok = p.bc.lockedAddrMap[shard]
				p.bc.lockedAddrMapMu.RUnlock()

				// Continue if the shard do not exists in the lockedAddrMap
				if !sok || len(lockedAddrs) == 0 {
					continue
				}

				p.bc.lockedAddrMu.Lock()
				for _, addr := range lockedAddrs {
					delete(p.bc.lockedAddr, addr)
				}
				p.bc.lockedAddrMu.Unlock()

				// Unlock all keys of the shard
				p.bc.lockedAddrMapMu.Lock()
				delete(p.bc.lockedAddrMap, shard)
				p.bc.lockedAddrMapMu.Unlock()

			} else if status && txType == types.CrossShard {
				data := tx.Data()[4:]
				shards, _ = types.DecodeCrossTx(uint64(0), data)
				numShard = len(shards)
				index = (2+1+numShard)*elemSize + elemSize + 2
				// Fetch all read-write keys of a transaction
				allKyes, _, _ := types.GetAllRWSet(uint16(numShard), data[index:])
				// Update the global locked keys and lockedAddrMap
				p.bc.addNewLocks(allKyes)
			} else {
				log.Warn("Skipping transaction", "hash", tx.Hash(), "status", status, "type", txType)
			}
			if txType == types.CrossShard {
				p.bc.procCtxsMu.Lock()
				p.bc.procCtxs[tx.Hash()] = false
				p.bc.procCtxsMu.Unlock()
			}
		}
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	// s3 := statedb.Copy()
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles(), receipts)
	// s4 := statedb.Copy()
	// if header.Shard > uint64(0) {
	// 	log.Info("@ds Process before finalize", "s3", s3.IntermediateRoot(false), "s4", s4.IntermediateRoot(false))
	// }
	return receipts, privateReceipts, allLogs, *usedGas, nil
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc *BlockChain, author *common.Address, gp *GasPool, dc *types.DataCache, statedb, privateState *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, *types.Receipt, uint64, error) {
	if !config.IsQuorum || !tx.IsPrivate() {
		privateState = statedb
	}

	if config.IsQuorum && tx.GasPrice() != nil && tx.GasPrice().Cmp(common.Big0) > 0 {
		return nil, nil, 0, ErrInvalidGasPrice
	}

	// Updating the address of the transaction
	if tx.TxType() == types.StateCommit {
		commitAddress := bc.CommitAddress()
		tx.SetRecipient(&commitAddress)
	}
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, nil, 0, err
	}
	// Create a new context to be used in the EVM environment
	context := NewEVMContext(msg, header, bc, author)
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	vmenv := vm.NewEVM(context, dc, statedb, privateState, config, cfg)

	// Apply the transaction to the current state (included in the env)
	_, gas, failed, err := ApplyMessage(vmenv, msg, gp)
	if err != nil {
		return nil, nil, 0, err
	}

	// s1 := statedb.Copy()
	// Update the state with pending changes
	var root []byte
	if config.IsByzantium(header.Number) {
		// if header.Shard > uint64(0) {
		// 	log.Info("config.IsByzantium")
		// }
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	*usedGas += gas
	// s2 := statedb.Copy()

	// if header.Shard > uint64(0) {
	// 	log.Info("@ds Apply transaction", "s1", s1.IntermediateRoot(false), "s2", s2.IntermediateRoot(false))
	// }
	// If this is a private transaction, the public receipt should always
	// indicate success.
	publicFailed := !(config.IsQuorum && tx.IsPrivate()) && failed

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	// based on the eip phase, we're passing wether the root touch-delete accounts.
	receipt := types.NewReceipt(root, publicFailed, *usedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = gas
	// if the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = statedb.GetLogs(tx.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})

	var privateReceipt *types.Receipt
	if config.IsQuorum && tx.IsPrivate() {
		var privateRoot []byte
		if config.IsByzantium(header.Number) {
			privateState.Finalise(false)
		} else {
			privateRoot = privateState.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
		}
		privateReceipt = types.NewReceipt(privateRoot, failed, *usedGas)
		privateReceipt.TxHash = tx.Hash()
		privateReceipt.GasUsed = gas
		if msg.To() == nil {
			privateReceipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
		}

		privateReceipt.Logs = privateState.GetLogs(tx.Hash())
		privateReceipt.Bloom = types.CreateBloom(types.Receipts{privateReceipt})
	}

	return receipt, privateReceipt, gas, err
}
