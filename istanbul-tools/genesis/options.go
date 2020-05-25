// Copyright 2017 AMIS Technologies
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

package genesis

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"

	"github.com/jpmorganchase/istanbul-tools/cmd/istanbul/extra"
)

type Option func(*core.Genesis)

func Validators(addrs ...common.Address) Option {
	return func(genesis *core.Genesis) {
		extraData, err := extra.Encode("0x00", addrs)
		if err != nil {
			log.Error("Failed to encode extra data", "err", err)
			return
		}
		genesis.ExtraData = hexutil.MustDecode(extraData)
	}
}

func GasLimit(limit uint64) Option {
	return func(genesis *core.Genesis) {
		genesis.GasLimit = limit
	}
}

func Alloc(addrs []common.Address, balance *big.Int, numShard uint64) Option {
	return func(genesis *core.Genesis) {
		alloc := make(map[common.Address]core.GenesisAccount)
		for _, addr := range addrs {
			alloc[addr] = core.GenesisAccount{Balance: balance}
		}

		if numShard == uint64(1) {
			numShard = 10
		}
		// To allocate balance to shard addresses
		seed := "6462C73A8D4913910C5AAA748EA82CD67EB4B73D"
		bigSeed := new(big.Int)
		bigSeed, _ = bigSeed.SetString(seed, 16)
		for i := uint64(0); i < numShard; i++ {
			addr := new(big.Int).SetUint64(i)
			addr.Add(addr, bigSeed)
			alloc[common.BigToAddress(addr)] = core.GenesisAccount{Balance: balance}
		}

		genesis.Alloc = alloc
	}
}
