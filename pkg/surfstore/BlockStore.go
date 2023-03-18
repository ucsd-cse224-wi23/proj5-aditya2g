package surfstore

import (
	context "context"
	"fmt"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	mtx      sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	var bk Block
	bs.mtx.Lock()
	block, ok := bs.BlockMap[blockHash.Hash]
	bs.mtx.Unlock()
	if !ok {
		return &bk, fmt.Errorf("No block found with given hash")
	} else {
		return block, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hashVal := GetBlockHashString(block.BlockData)
	bs.mtx.Lock()
	bs.BlockMap[hashVal] = block
	bs.mtx.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var validHashes []string
	for _, hashVal := range blockHashesIn.Hashes {
		bs.mtx.Lock()
		_, ok := bs.BlockMap[hashVal]
		bs.mtx.Unlock()
		if ok {
			validHashes = append(validHashes, hashVal)
		}
	}
	return &BlockHashes{Hashes: validHashes}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	var validHashes []string
	for hashVal, _ := range bs.BlockMap {
		validHashes = append(validHashes, hashVal)
	}
	return &BlockHashes{Hashes: validHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
