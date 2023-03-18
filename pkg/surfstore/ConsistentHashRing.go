package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	serverHashes := []string{}
	for h := range c.ServerMap {
		serverHashes = append(serverHashes, h)
	}
	sort.Strings(serverHashes)

	for i := 0; i < len(serverHashes); i++ {
		if serverHashes[i] > blockId {
			return c.ServerMap[serverHashes[i]]
		}
	}
	return c.ServerMap[serverHashes[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	serverMap := make(map[string]string)
	var c = ConsistentHashRing{
		ServerMap: serverMap,
	}

	for _, serverAddr := range serverAddrs {
		serverHash := c.Hash("blockstore" + serverAddr)
		c.ServerMap[serverHash] = serverAddr
	}
	return &c
}
