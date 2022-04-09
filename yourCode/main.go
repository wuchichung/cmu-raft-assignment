package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

const DEBUG bool = false

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {
	log []*raft.LogEntry
	// TODO: Implement this!
	// lock
	lock sync.Mutex
	//
	currentTerm int
	// candidateId that received vote in current in current term
	votedFor int
	// for each server, index of the next log entry to send to that server
	nextIndex map[int]int
	// for each server, index of highest log entry known to be replicated on serve
	matchIndex map[int]int
	// role
	role raft.Role
	// the key-value store
	store map[string]int
	// reset channel
	restChan chan bool
	// commit channel
	commitChan chan bool
	// election timeout
	electionTimeOut int
	// heartbeat interval
	heartBeatInterval int
	// node id
	nodeId int
	//
	numMajority int
	//
	commitIndex int
}

// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.
func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {
	// TODO: Implement this!

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		log:               nil,
		currentTerm:       0,
		votedFor:          -1,
		nextIndex:         make(map[int]int),
		matchIndex:        make(map[int]int),
		role:              raft.Role_Follower,
		store:             make(map[string]int),
		restChan:          make(chan bool),
		commitChan:        make(chan bool),
		heartBeatInterval: heartBeatInterval,
		electionTimeOut:   electionTimeout,
		nodeId:            nodeId,
		numMajority:       int(float32(len(nodeidPortMap)+1)/2 + 0.5),
		commitIndex:       0,
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect nodes
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("Successfully connect all nodes")

	//TODO: kick off leader election here !
	go func() {
		ctx := context.Background()
		for {
			// get node's role
			rn.lock.Lock()
			node_role := rn.role
			rn.lock.Unlock()
			if DEBUG {
				fmt.Printf("%d is %v\n", rn.nodeId, node_role)
			}

			switch node_role {
			case raft.Role_Follower:
				/* apply the log */
				/* start looping */
				select {
				case <-time.After(time.Duration(rn.electionTimeOut) * time.Millisecond):
					if DEBUG {
						fmt.Printf("Trigger %d's Role_Follower toggle channel\n", rn.nodeId)
					}
					// change role from follwer to candidate
					rn.lock.Lock()
					if rn.role == raft.Role_Follower {
						rn.role = raft.Role_Candidate
					}
					rn.lock.Unlock()
				case <-rn.restChan:
					if DEBUG {
						fmt.Printf("Trigger %d's Role_Follower reset channel\n", rn.nodeId)
					}
				}

			case raft.Role_Candidate:

				/* request vote for other nodes */
				rn.lock.Lock()
				// increment current term
				rn.currentTerm++
				// update voted for
				rn.votedFor = rn.nodeId
				// get last log index
				lastLogIndex := len(rn.log)
				// get last log term
				lastLogTerm := 0
				if lastLogIndex > 0 {
					lastLogTerm = int(rn.log[lastLogIndex-1].Term)
				}
				rn.lock.Unlock()
				//
				numVote := 1
				for hostID, client := range hostConnectionMap {
					go func(hid int32, c raft.RaftNodeClient) {
						// involke RequestVote rpc for other nodes
						reply, err := c.RequestVote(ctx, &raft.RequestVoteArgs{
							From:         int32(rn.nodeId),
							To:           hid,
							Term:         int32(rn.currentTerm),
							LastLogIndex: int32(lastLogIndex),
							LastLogTerm:  int32(lastLogTerm),
						})
						// check reply
						if err == nil {
							if reply.VoteGranted {
								// acquire lock
								rn.lock.Lock()
								// count vote
								numVote += 1
								// if the numVote equals num majority
								if numVote == rn.numMajority {
									if rn.role == raft.Role_Candidate {
										// become new leader
										rn.role = raft.Role_Leader
										// reset matchIndex
										for _hid := range rn.matchIndex {
											rn.matchIndex[_hid] = 0
										}
										// reset nextIndex
										for _hid := range rn.nextIndex {
											rn.nextIndex[_hid] = len(rn.log)
										}
										// reset
										rn.restChan <- true
									}
								}
								// release lock
								rn.lock.Unlock()
							}
						} else {
							fmt.Printf("RequestVote error %v\n", err)
						}
					}(hostID, client)
				}

				/* start looping */
				select {
				case <-time.After(time.Duration(rn.electionTimeOut) * time.Millisecond):
					if DEBUG {
						fmt.Printf("Trigger %d's Role_Candidate restart election channel\n", rn.nodeId)
					}
				case <-rn.restChan:
					if DEBUG {
						fmt.Printf("Trigger %d's Role_Candidate reset channel\n", rn.nodeId)
					}
				}

			case raft.Role_Leader:

				/* invoke AppendEntries rpc of the other nodes */
				for hostID, client := range hostConnectionMap {
					go func(hid int32, c raft.RaftNodeClient) {
						rn.lock.Lock()
						// get next index
						nextLogIndex := rn.nextIndex[int(hid)]
						// get prev log index
						prevLogIndex := 0
						if nextLogIndex > 1 {
							prevLogIndex = nextLogIndex - 1
						}
						// get prev log term
						prevLogTerm := 0
						if prevLogIndex > 0 {
							prevLogTerm = int(rn.log[prevLogIndex-1].Term)
						}
						// get entries
						entries := make([]*raft.LogEntry, 0)
						if nextLogIndex > 0 {
							entries = append(entries, rn.log[nextLogIndex-1:]...)
						}
						rn.lock.Unlock()

						// send AppendEntries request
						reply, err := c.AppendEntries(ctx, &raft.AppendEntriesArgs{
							From:         int32(rn.nodeId),
							To:           int32(hid),
							Term:         int32(rn.currentTerm),
							LeaderId:     int32(rn.nodeId),
							PrevLogIndex: int32(prevLogIndex),
							PrevLogTerm:  int32(prevLogTerm),
							Entries:      entries,
							LeaderCommit: int32(rn.commitIndex),
						})

						if err == nil {
							rn.lock.Lock()
							if reply.Success {
								// update next index
								rn.nextIndex[int(hid)] = int(reply.MatchIndex) + 1
								// update match index
								rn.matchIndex[int(hid)] = int(reply.MatchIndex)
								// check whether rn.commitIndex + 1 commit index already get enough commit
								keep_search := true
								for keep_search {
									// propose commit index and verify with the match index
									newCommitIndex := rn.commitIndex + 1
									newCommitIndexCount := 1
									for _, m_index := range rn.matchIndex {
										if m_index >= newCommitIndex {
											newCommitIndexCount++
										}
									}

									if newCommitIndexCount == rn.numMajority {
										// update commit index
										rn.commitIndex = newCommitIndex
										// trigger commit channel
										rn.commitChan <- true
									} else if newCommitIndexCount < rn.numMajority {
										//
										keep_search = false
									}
								}

							} else {
								// decrement nextIndex
								rn.nextIndex[int(hid)]--
							}
							rn.lock.Unlock()
						} else {
							fmt.Printf("AppendEntries error %v\n", err)
						}

					}(hostID, client)
				}

				/* start looping */
				select {
				case <-time.After(time.Duration(rn.heartBeatInterval) * time.Millisecond):
					if DEBUG {
						fmt.Printf("Trigger %d's Role_Leader restart heartbeat timer channel\n", rn.nodeId)
					}
				case <-rn.restChan:
					if DEBUG {
						fmt.Printf("Trigger %d's Role_Leader reset channel\n", rn.nodeId)
					}
				}
			}
		}
	}()

	return &rn, nil
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	/*
		If a Propose is sent to a follower node, Status WrongNode shall be replied to tell the
		requester who is the current leader and the requester shall resend the request to the
		current leader
	*/

	// TODO: Implement this!
	var ret raft.ProposeReply

	if rn.role == raft.Role_Leader {
		rn.lock.Lock()
		// append entry to local log
		rn.log = append(rn.log, &raft.LogEntry{Term: int32(rn.currentTerm), Op: args.Op, Key: args.Key, Value: args.V})
		rn.lock.Unlock()
		// wait for commit
		<-rn.commitChan
		// apply entry
		ret.CurrentLeader = int32(rn.nodeId)
		//
		if args.Op == raft.Operation_Put {
			rn.lock.Lock()
			rn.store[args.Key] = int(args.V)
			rn.lock.Unlock()
			ret.Status = raft.Status_OK
		} else if args.Op == raft.Operation_Delete {
			rn.lock.Lock()
			_, ok := rn.store[args.Key]
			ret.Status = raft.Status_KeyNotFound
			if ok {
				delete(rn.store, args.Key)
				ret.Status = raft.Status_OK
			}
			rn.lock.Unlock()
		}

	} else if rn.role == raft.Role_Follower {
		ret.CurrentLeader = int32(rn.votedFor)
		ret.Status = raft.Status_WrongNode
	}
	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	// TODO: Implement this!

	//In this version of Raft, every follower shall also answer to GetValue from its own
	//key-value store, not redirecting that request to the leader.
	var ret raft.GetValueReply

	if value, ok := rn.store[args.Key]; ok {
		ret.V = int32(value)
		ret.Status = raft.Status_KeyFound
	} else {
		ret.Status = raft.Status_KeyNotFound
	}

	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	// TODO: Implement this!
	// get node's last log index and last log term
	lastLogIndex := len(rn.log)
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = int(rn.log[lastLogIndex-1].Term)
	}

	// decide whether grant this vote
	isVoteGranted := true
	if int(args.Term) < rn.currentTerm {
		// if request's term is smaller than node's, reject
		isVoteGranted = false
	} else if int(args.Term) == rn.currentTerm && rn.votedFor != -1 {
		// if the node already voted in this term, do not vote again
		isVoteGranted = false
	} else if int(args.LastLogIndex) < lastLogIndex {
		// if request's last log index is smaller than node's, reject
		isVoteGranted = false
	} else if int(args.LastLogTerm) < lastLogTerm {
		// if request's last log term is smaller than node's, reject
		isVoteGranted = false
	}

	// update if vote is granted
	if isVoteGranted {
		rn.lock.Lock()
		rn.votedFor = int(args.From)
		rn.currentTerm = int(args.Term)
		rn.role = raft.Role_Follower
		rn.restChan <- true // reset timmer
		rn.lock.Unlock()
	}

	// update reply
	reply := raft.RequestVoteReply{
		From:        int32(rn.nodeId),
		To:          args.From,
		Term:        int32(rn.currentTerm),
		VoteGranted: isVoteGranted,
	}

	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	// TODO: Implement this
	// flag to indicate successful or not
	is_successful := true
	matchIndex := 0

	// deprecated request from prev leader, reject
	if args.Term < int32(rn.currentTerm) {
		is_successful = false
	}

	// if request prevlogindex is valid (>0)
	if args.PrevLogIndex > 0 {
		// if request's prevlogindex is greater than node's log length, reject
		if args.PrevLogIndex > int32(len(rn.log)) {
			is_successful = false
		}

		// find a conflict, delete the existing entry and the ones following it
		if int32(rn.log[args.PrevLogIndex-1].Term) != args.PrevLogTerm {
			is_successful = false
			rn.lock.Lock()
			rn.log = rn.log[:args.PrevLogIndex]
			rn.lock.Unlock()
		}
	}

	// if the appendentry request is successful
	if is_successful {

		rn.lock.Lock()
		rn.currentTerm = int(args.Term)
		rn.votedFor = int(args.From)
		// once we accept a appendentries request, we become a follower
		if rn.role != raft.Role_Follower {
			rn.role = raft.Role_Follower
		}

		// append entry log
		rn.log = append(rn.log, args.Entries...)
		//
		matchIndex = int(args.PrevLogIndex) + len(args.Entries)
		// apply entries in log
		leader_commit := int(args.LeaderCommit)
		if leader_commit > rn.commitIndex {
			// get start index
			s := 0
			if rn.commitIndex > 0 {
				s = rn.commitIndex - 1
			}
			// apply log till leader_commit
			for _, entry := range rn.log[s:leader_commit] {
				if entry.Op == raft.Operation_Put {
					rn.store[entry.Key] = int(entry.Value)
				} else if entry.Op == raft.Operation_Delete {
					_, ok := rn.store[entry.Key]
					if ok {
						delete(rn.store, entry.Key)
					}
				}
			}
			// update node's commitindex
			rn.commitIndex = leader_commit
		}
		// reset timmer
		rn.restChan <- true
		//
		rn.lock.Unlock()
	}

	// init reply
	reply := raft.AppendEntriesReply{
		From:       int32(rn.nodeId),
		To:         args.From,
		Success:    is_successful,
		Term:       int32(rn.currentTerm),
		MatchIndex: int32(matchIndex),
	}

	return &reply, nil
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	// TODO: Implement this!
	var reply raft.SetElectionTimeoutReply
	rn.electionTimeOut = int(args.Timeout)
	rn.restChan <- true
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	// TODO: Implement this!
	var reply raft.SetHeartBeatIntervalReply
	rn.heartBeatInterval = int(args.Interval)
	rn.restChan <- true
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
