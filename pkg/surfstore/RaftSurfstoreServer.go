package surfstore

import (
	context "context"
	"math"
	"sync"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader       bool
	isLeaderMutex  *sync.RWMutex
	term           int64
	log            []*UpdateOperation
	id             int64
	peers          []string
	pendingCommits []chan bool
	commitIndex    int64
	lastApplied    int64
	nextIndex      []int64
	matchIndex     []int64
	metaStore      *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	output, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if output.Flag {
		return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
	}
	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	output, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if output.Flag {
		return s.metaStore.GetBlockStoreMap(ctx, hashes)
	}
	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	output, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
	if output.Flag {
		return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	}
	return nil, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	// Append entry to the log
	logEntry := &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	// Commit to own log
	s.log = append(s.log, logEntry)
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, commitChan)

	// Send entry to all the followers in parallel
	go s.sendToAllFollowersInParallel(ctx, logEntry)

	// Keep trying indefinitely (even after responding) Using go Routine for this.... rely on sendHeartbeat Call

	// Commit the entry once majority of followers have it in their logs. This is a blocking operation
	commit := <-commitChan

	// Once committed apply to the state machine
	if commit {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context, logEntry *UpdateOperation) {
	// send entry to all my followers and count the replies
	responses := make(chan bool, len(s.peers)-1)

	// contact all the followers, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollower(ctx, idx, addr, responses, logEntry)
	}

	totalResponses := 1
	totalAppends := 1
	// Wait for responses in loop
	// TODO: Verify Why do we need to wait in loop for all the responses. We only need half right
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	if totalAppends > len(s.peers)/2 {
		// TODO Put correct indices
		s.commitIndex = s.commitIndex + 1
		*&s.pendingCommits[s.commitIndex] <- true
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, serverIdx int, addr string, responses chan bool, logEntry *UpdateOperation) {
	entry := make([]*UpdateOperation, 0)
	entry = append(entry, logEntry)
	prevLogIdx := int(s.nextIndex[serverIdx] - 1)
	var prevLogTerm = 0
	if len(s.log) > 1 {
		prevLogTerm = int(s.log[prevLogIdx].Term)
	}

	entries := AppendEntryInput{
		Term:         s.term,
		PrevLogIndex: int64(prevLogIdx),
		PrevLogTerm:  int64(prevLogTerm),
		Entries:      entry,
		LeaderCommit: s.commitIndex,
	}
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)

	clientOutput, _ := client.AppendEntries(ctx, &entries)
	s.matchIndex[int(clientOutput.ServerId)] = clientOutput.MatchedIndex

	// TODO Check clientOutput
	if clientOutput.Success == true {
		s.nextIndex[clientOutput.ServerId]++
		responses <- true
	}

	if clientOutput.Term > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = clientOutput.Term
	}

	if clientOutput.Success == false {
		responses <- false
	}
	return
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	}
	falseOutput := AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      false,
		MatchedIndex: s.commitIndex,
	}

	// Case 1 Server ID greater than Leader ID
	if s.term > input.Term {
		return &falseOutput, nil
	}

	// Case2 Check previous logIdx and prevTerm matching
	if (input.PrevLogTerm != 0) && int(input.PrevLogIndex) > len(s.log)-1 {
		return &falseOutput, nil
	}

	if (input.PrevLogTerm != 0) && (s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
		return &falseOutput, nil
	}

	// TODO: Recursive Call to delete the previous entries
	currIdx := input.PrevLogIndex
	for _, entry := range input.Entries {
		s.log = append(s.log, entry)
		currIdx++
	}
	for s.commitIndex < input.LeaderCommit {
		entry := s.log[s.commitIndex+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.commitIndex++
	}

	return &AppendEntryOutput{ServerId: s.id,
		Term:         s.term,
		Success:      true,
		MatchedIndex: currIdx,
	}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, nil
	}

	prevLogIdx := int(s.nextIndex[s.id] - 1)
	var prevLogTerm = 0
	if prevLogIdx >= 0 {
		prevLogTerm = int(s.log[prevLogIdx].Term)
	}

	emptyAppendEntriesInput := AppendEntryInput{
		Term:         s.term,
		PrevLogIndex: int64(prevLogIdx),
		PrevLogTerm:  int64(prevLogTerm),
		Entries:      make([]*UpdateOperation, 0),
		LeaderCommit: s.commitIndex,
	}

	// Variables to track maxTerm, received responses, agreed servers
	var maxTerm int64 = s.term
	totalResponses := 1
	totalAgreements := 1

	// contact all the followers, send AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		client := NewRaftSurfstoreClient(conn)

		clientOutput, _ := client.AppendEntries(ctx, &emptyAppendEntriesInput)
		maxTerm = int64(math.Max(float64(clientOutput.Term), float64(maxTerm)))

		totalResponses++
		if clientOutput.Success {
			totalAgreements++
		}
		s.matchIndex[clientOutput.ServerId] = clientOutput.MatchedIndex
		// TODO: Use Match and Next Index to replicate
	}

	if maxTerm > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		defer s.isLeaderMutex.Unlock()
		s.term = maxTerm
	}

	if totalAgreements > len(s.peers)/2 {
		return &Success{Flag: true}, nil
	}
	return &Success{Flag: false}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	for idx, _ := range s.nextIndex {
		s.nextIndex[idx] = int64(s.commitIndex + 1)
		s.matchIndex[idx] = 0
	}

	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
