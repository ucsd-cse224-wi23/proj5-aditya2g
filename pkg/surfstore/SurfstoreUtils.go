package surfstore

import (
	"database/sql"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	reflect "reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error reading base dir", err)
	}

	// Checking if the db exists or not
	var dbFound bool = true
	metaFilePath, _ := filepath.Abs(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME))
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		dbFound = false
	}

	// If db doesn't exist create one
	if !dbFound {
		metaFilePath, _ := filepath.Abs(ConcatPath(client.BaseDir, DEFAULT_META_FILENAME))
		db, err := sql.Open("sqlite3", metaFilePath)
		if err != nil {
			log.Println("Error opening database", err)
		}
		stmt, err := db.Prepare(createTable)
		if err != nil {
			log.Println("Error creating table in database", err)
		}
		stmt.Exec()
	}

	// Read the local index
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Error retrieving data from local db", err)
	}

	fileHashMap := make(map[string][]string)
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		} else {
			// Compute their hash locally and store in map for comparison with db
			var numBlocks int = int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))
			fileHashMap[file.Name()] = GetHashList(ConcatPath(client.BaseDir, file.Name()), numBlocks, client.BlockSize)
		}

		// Check if the given file exists in local index already
		data, ok := localIndex[file.Name()]
		// File not present
		if !ok {
			localIndex[file.Name()] = &FileMetaData{Filename: file.Name(), Version: int32(VERSION_INDEX), BlockHashList: fileHashMap[file.Name()]}
		} else {
			// File is present
			ok = reflect.DeepEqual(data.BlockHashList, fileHashMap[file.Name()])
			if !ok {
				// Increase the version number and update hash
				localIndex[file.Name()].Version = localIndex[file.Name()].Version + 1
				localIndex[file.Name()].BlockHashList = fileHashMap[file.Name()]
			}
		}
	}

	// Newly added/updated files are added to localIndex
	// Updating the hash and version for files which have been removed from the baseDir
	for fileName, data := range localIndex {
		_, ok := fileHashMap[fileName]
		if !ok {
			if len(data.BlockHashList) != 1 || data.BlockHashList[0] != "0" {
				data.Version++
				data.BlockHashList = []string{TOMBSTONE_HASHVALUE}
			}
		}
	}

	remoteIndex := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remoteIndex)

	var blockAddrs []string
	err = client.GetBlockStoreAddrs(&blockAddrs)
	if err != nil {
		log.Println("BlockStore Error: ", err)
	}

	// If there are any local changes push them to the server
	for fileName, localData := range localIndex {
		remoteData, ok := remoteIndex[fileName]
		if !ok {
			// if file not found on the remote server, upload
			client.uploadingFile(localData, blockAddrs)
		} else {
			// Higher version on the local
			if localData.Version > remoteData.Version {
				client.uploadingFile(localData, blockAddrs)
			}
		}
	}

	// If there are any changes on the server, download them and update the db
	for fileName, remoteData := range remoteIndex {
		localData, ok := localIndex[fileName]
		if !ok {
			// if file not present on the local, download
			localIndex[fileName] = &FileMetaData{}
			localData := localIndex[fileName]
			client.downloadingFile(localData, remoteData, blockAddrs)
		} else {
			// Higher version on the remote
			if remoteData.Version >= localData.Version {
				client.downloadingFile(localData, remoteData, blockAddrs)
			}
		}
	}

	err = WriteMetaFile(localIndex, client.BaseDir)
	if err != nil {
		log.Println("Error retrieving data from local db", err)
	}

}

func GetHashList(filePath string, numBlocks int, blockSize int) []string {
	hashes := make([]string, 0)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Error reading file: ", err)
	}

	for i := 0; i < numBlocks; i++ {
		byteSlice := make([]byte, blockSize)
		n, err := file.Read(byteSlice)
		if err != nil {
			log.Println("Unable to read file", err)
		}
		byteSlice = byteSlice[:n]
		hashes = append(hashes, GetBlockHashString(byteSlice))
	}
	return hashes
}

func (client RPCClient) uploadingFile(metaData *FileMetaData, blockStoreAddrs []string) error {
	filePath := ConcatPath(client.BaseDir, metaData.Filename)
	var latestVersion int32
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		err = client.UpdateFile(metaData, &latestVersion)
		if err != nil {
			log.Println("uploading error", err)
		}
		metaData.Version = latestVersion
		return err
	}

	err := client.writeDataOnServer(filePath, blockStoreAddrs, metaData.BlockHashList)
	if err != nil {
		log.Println("error uploading file")
	}

	// If upload failed, update version as -1
	err = client.UpdateFile(metaData, &latestVersion)
	if err != nil {
		log.Println("update failed ", err)
		metaData.Version = -1
	}
	metaData.Version = latestVersion

	return nil
}

func (client RPCClient) writeDataOnServer(filePath string, blockStoreAddrs []string, blockHashes []string) error {
	file, err := os.Open(filePath)
	if err != nil {
		log.Println("file opening error ", err)
		return err
	}

	fileStat, _ := os.Stat(filePath)
	blockStoreMap := make(map[string][]string)
	err = client.GetBlockStoreMap(blockHashes, &blockStoreMap)

	blockStoreReverse := make(map[string]string)
	for s, hl := range blockStoreMap {
		for _, h := range hl {
			blockStoreReverse[h] = s
		}
	}

	// Dividing file into blocks and updating the fileMetaData
	var numBlocks int = int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
	for i := 0; i < numBlocks; i++ {
		blockdata := make([]byte, client.BlockSize)
		n, err := file.Read(blockdata)
		if err != nil && err != io.EOF {
			log.Println("reading file failed", err)
			return err
		}
		blockdata = blockdata[:n]
		blockHash := GetBlockHashString(blockdata)

		block := Block{BlockData: blockdata, BlockSize: int32(n)}
		respServer := blockStoreReverse[blockHash]

		var success bool
		// Putting the block on the server
		err = client.PutBlock(&block, respServer, &success)
		if err != nil {
			log.Println("block update failed", err)
			return err
		}
	}

	return nil
}

func (client RPCClient) downloadingFile(localMetaData *FileMetaData, remoteMetaData *FileMetaData, blockStoreAddrs []string) error {
	*localMetaData = *remoteMetaData

	filePath := ConcatPath(client.BaseDir, remoteMetaData.Filename)
	//File deleted in server
	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == TOMBSTONE_HASHVALUE {
		//  Delete the file from the local
		if err := os.Remove(filePath); err != nil {
			log.Println("error deleting file ", err)
			return err
		}
		return nil
	}
	err := client.writeDataOnLocal(filePath, blockStoreAddrs, remoteMetaData)
	if err != nil {
		log.Println("error downloading file ", err)
		return err
	}

	return nil
}

func (client RPCClient) writeDataOnLocal(filePath string, blockStoreAddrs []string, remoteMetaData *FileMetaData) error {
	file, err := os.Create(filePath)
	if err != nil {
		log.Println("unable to create file: ", err)
	}

	blockStoreMap := make(map[string][]string)
	_ = client.GetBlockStoreMap(remoteMetaData.BlockHashList, &blockStoreMap)

	blockStoreReverse := make(map[string]string)
	for s, hl := range blockStoreMap {
		for _, h := range hl {
			blockStoreReverse[h] = s
		}
	}

	data := ""
	// Downloading all the blocks from the server
	for _, hash := range remoteMetaData.BlockHashList {
		var block Block
		respServer := blockStoreReverse[hash]
		if err := client.GetBlock(hash, respServer, &block); err != nil {
			log.Println("error downloading complete block", err)
		}

		data += string(block.BlockData)
	}
	file.WriteString(data)

	return nil
}
