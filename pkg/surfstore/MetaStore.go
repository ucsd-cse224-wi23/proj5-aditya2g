package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	mtx                sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	fileMetaData := m.FileMetaMap
	return &FileInfoMap{FileInfoMap: fileMetaData}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fileName := fileMetaData.Filename
	fileVersion := fileMetaData.Version
	m.mtx.Lock()
	data, ok := m.FileMetaMap[fileName]
	// File Not present
	if !ok {
		m.FileMetaMap[fileName] = fileMetaData

	} else {
		// Check for version
		if data.Version+1 == fileVersion {
			m.FileMetaMap[fileName] = fileMetaData
		} else {
			// Version Mismatch Return -1
			fileVersion = -1
		}
	}
	m.mtx.Unlock()
	return &Version{Version: fileVersion}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := make(map[string]*BlockHashes)
	for _, blockHash := range blockHashesIn.Hashes {
		serverName := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		bh, ok := blockStoreMap[serverName]
		if ok {
			bh.Hashes = append(bh.Hashes, blockHash)
		} else {
			var hashes []string
			hashes = append(hashes, blockHash)
			blockStoreMap[serverName] = &BlockHashes{Hashes: hashes}
		}
	}
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	blockStoreAddrs := m.BlockStoreAddrs
	return &BlockStoreAddrs{BlockStoreAddrs: blockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
