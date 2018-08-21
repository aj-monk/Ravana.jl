using RocksDB

const UNINITIALIZED = 0
const FOLLOWER = 1
const CANDIDATE = 2
const LEADER = 3

current_state = UNINITIALIZED

struct RavanaException <: Exception
    msg::AbstractString
end

mutable struct raftInfo
    currentTerm # latest term server has seen
    votedFor    # candidateId that received vote in current term
    commitIndex # index of highest log entry known to be committed
    lastApplied # index of highest log entry applied to state machine
end

struct raftNode
    name           # IP Address of name of node
    port           # Port where Raft protocol server is active
    nodeId         # Node id of the Raft node
    clusterId      # Cluster that the node belongs to
end

log       = nothing # Handle to the persistent log
nodes     = nothing # Array of nodes in cluster
nodeId    = nothing # This node id
clusterId = nothing # Cluster id
ipAddress = nothing # Protocol server IP
ipPort    = nothing # Protocol server port
rInfo     = nothing # Raft node state

leaderAddress = nothing
leaderPort    = nothing

mutable struct followerState
    nextIndex::Int
    matchIndex::Int
end

followers = Dict{UInt128, followerState}()

const CANDIDATE_SLEEP_GRANULARITY = 0.001
const HEARTBEAT_TIMEOUT = 0.01
# Election timeout must be
#   1. > heartbeat timeout
#   2. > worst one way communication time to another node
const ELECTION_TIMEOUT  = 0.1

function raft_uninitialized()
    return
end

function raft_follower()
    println("Moved to FOLLOWER state")
    start_election_timer()
end

function raft_candidate()
    println("Moved to CANDIDATE state")
    votes = 0
    for n in nodes
        r = remote_request_vote(n)
        println("remote_request_vote() returned :", r)
        (remoteTerm, status) = remote_request_vote(n)
        status && (votes += 1)
    end
    while !is_majority(votes) && (raft_get_state() == CANDIDATE)
        sleep(HEARTBEAT_TIMEOUT)
    end
    is_majority(votes) && raft_set_state(LEADER)
end

function raft_leader()
    println("Moved to LEADER state")
    leaderAddress = ipAddress
    leaderPort = ipPort
    return # TODO
    start_heartbeat()
end

const raft_states = Dict(UNINITIALIZED   =>  raft_uninitialized,
                         FOLLOWER        =>  raft_follower,
                         CANDIDATE       =>  raft_candidate,
                         LEADER          =>  raft_leader)

function state_transition()
    f = raft_states[current_state]
    f()
end

raft_get_state() = (return current_state)
function raft_set_state(s)
    cur = raft_get_state()
    cur == s && return
    if (cur == LEADER) && (s != LEADER)
        leaderAddress = nothing
        leaderPort = nothing
    end
    global current_state = s
    state_transition()
end

function init_raft()
    current_state = UNINITIALIZED
    read_state()
    raft_server()
    ravana_server()
    raft_init_protocol()
end

function read_state()
    init_raft_log()
    global nodes = get_cluster_nodes()
    if nodes == nothing
        println("Cluster not initialized.")
        println("\tCall init_cluster() to start a new cluster with this machine as the first node")
        println("\tCall add_node_to_cluster(clusterId) to add this machine to an existing cluster")
        return
    end

    init_raft_info()
    init_cluster_state()
end

function init_raft_log()
    global log = RocksDB.open_db(basedir() * "raft_log", true)
end

function get_cluster_nodes()
    db_get(log, "RavanaCluster")
end

function init_cluster_state()
    (nId, cluId)  = raft_get_node()
    global clusterId = cluId
    for i in nodes
        if i.nodeId == nodeId
            @assert cId == i.clusterId
            global ipAddress = i.name
            global ipPort    = i.port
        end
    end

    raft_set_state(FOLLOWER)
    println("Node is part of cluster: ", hex(clusterId))
end

function init_raft_info()
    global rInfo = raft_get_info()
end

function raft_init_protocol()
    state_machine_watcher()
    # state_watcher()
end

# A task to watch for changes to state of this node and
# trigger actions
const STATE_CHANGE_TIMEOUT = 0.001
function state_watcher()
    @async begin
        while true
            state_transition()
            sleep(STATE_CHANGE_TIMEOUT)
        end
    end
end

const STATE_MACHINE_TIMEOUT = 1
# A task to apply log entries to state machine
function state_machine_watcher()
    @async begin
        while true
            if raft_get_state() == UNINITIALIZED
                sleep(STATE_MACHINE_TIMEOUT)
                continue
            end
            if (rInfo.commitIndex > rInfo.lastApplied)
                execute_entry(rInfo.lastApplied + 1)
                rInfo.lastApplied += 1
            end
            raft_save_info(rInfo)
            sleep(STATE_MACHINE_TIMEOUT)
        end
    end
end

# Applies log entry at index to state machine
function execute_entry(index)
    (term, op, argv) = get_log_entry(index)
    raft_execute(op, op_table(op), argv)
end

raft_increment_term() = (rInfo.currentTerm = rInfo.currentTerm + 1)

global receivedHeartbeat = false
get_heartbeat() = return receivedHeartbeat
set_heartbeat(val) = (receivedHeartbeat = val)

function start_election_timer()
    @async begin
        while raft_get_state() == FOLLOWER
            sleep(ELECTION_TIMEOUT)
            if !get_heartbeat()
                raft_increment_term()
                raft_set_state(CANDIDATE)
            end
            set_heartbeat(false)
        end
    end
end

"""
    start_heartbeat()
Start a heartbeat task for each follower
"""
function start_heartbeat()
    for n in nodes
        heartbeat_task(n)
    end
end

# Exits if not the leader
function heartbeat_task(node)
    @async begin
        while raft_get_state() == LEADER
            # TODO
            sleep(HEARTBEAT_TIMEOUT)
        end
    end
end

"""
    remote_append_entries()
op         : Operation to replicate
argv       : Arguments to OP
prevIndex  : Index in log of entry just before current
prevTerm   : Term of prevIndex
commitIndex: Index of log that is committed on the leader
node       : Raft node to which this is sent

Called by leader to send log entries to followers.
"""
function remote_append_entries(op, argv, prevIndex, prevTerm, commitIndex, node)
    println("append_entries() op=", op, " prevIndex=", prevIndex, " term=", prevTerm, "node=", node)
    payload = (rInfo.currentTerm, rInfo.nodeId, prevIndex, prevTerm, commitIndex, op, argv)
    (term, log_index, status) = raft_client(node.name, node.port, RAFT_APPEND_ENTRIES, payload)
    status && return
    if term > rInfo.currentTerm
        rInfo.currentTerm = term
        raft_set_state(FOLLOWER)
    end
    # TODO if log_index is behind then send previous entries
end

"""
    local_append_entries()
Called on the followers to add log entries from leader to their logs.
"""
function local_append_entries(argv)
    (currentTerm, leaderNodeId, prevIndex, prevTerm, commitIndex, op, args) = argv

    # Add entry without question if this node is leader
    if leaderNodeId == rInfo.nodeId
        add_log_entry(prevIndex + 1, currentTerm, op, args)
        return (rInfo.currentTerm, prevIndex, true)
    end

    entry = get_log_entry(prevIndex)

    # Don't apply entry in the following cases
    # 1. Entry preceding the current one is not written to log
    entry == nothing && return (rInfo.currentTerm, find_last_log_index(prevIndex), false)
    (term, op, args) = entry
    # 2. Preceding entry's term does not match with leader
    prevTerm != term && return (term, prevIndex, false)

    # Success case
    add_log_entry(prevIndex + 1, currentTerm, op, args)
    rInfo.currentTerm = currentTerm
    global received_heartbeat = true
    return (rInfo.currentTerm, prevIndex, true)
end

"""
    remote_request_vote()

Called by candidates in order to request votes to become the leader.
Messages are sent to all nodes in the Raft cluster.
"""
function remote_request_vote(node)
    (term, op, args) = get_log_entry(rInfo.commitIndex)
    payload = (rInfo.currentTerm, nodeId, rInfo.commitIndex, term)
    r = raft_client(node.name, node.port, RAFT_REQUEST_VOTE, payload)
    r
end

votedFor  = nothing
votedTerm = 1
"""
    local_request_vote()

Receives a request_vote request from a candidate and processes it.
"""
function local_request_vote(argv)
    (newTerm, leaderId, lastIndex, lastTerm) = argv

    # Grant vote to self unconditionally
    leaderId == nodeId && return (rInfo.currentTerm, true)

    # Sleep for a random interval to avoid majority vote failure
    sleep_random()

    # Deny vote for these conditions
    # 1. Candidate's term is < this node's term
    newTerm < rInfo.currentTerm && return (rInfo.currentTerm, false)
    # 2. This node voted for a node who's term is >= candidate's term
    votedTerm >= newTerm && return (rInfo.currentTerm, false)
    # 3. This node's log has more entries than candidate's log
    rInfo.commitIndex > lastIndex && (rInfo.currentTerm, false)

    # Success case
    return (rInfo.currentTerem, true)
end

#=
   ## Raft log utils ##
=#
function get_log_entry(index)
    r = db_get(log, index)
    r
end

function add_log_entry(index, term, op, args)
    db_put(log, index, (term, op, args))
end

"""
    find_last_log_index(hint)
Find the index of the last valid entry in the log.
"""
function find_last_log_index(hint)
    @assert(get_log_entry(rInfo.lastApplied) != nothing)
    search_log(rInfo.lastApplied, hint)
end

function search_log(first, last)
    mid = floor(Int, (first + last) / 2)
    if first == mid
        if get_log_entry(last) != nothing
            return last
        else
            return first
        end
    end
    if get_log_entry(mid + 1) != nothing
        search_log(mid, last)
    else
        search_log(first, mid)
    end
end

# Save some persistent variables
function raft_save_info(st::raftInfo)
    db_put(log, "RaftInfo", st)
end

# Get persistent variables
function raft_get_info()
    db_get(log, "RaftInfo")
end
