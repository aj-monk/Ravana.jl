# Core Ops

function raft_execute(op, func, argv)
    return func(argv)
end

function raft_init_node(nodeId, clusterId)
    db_put(log, "NodeId", (nodeId, clusterId))
    nodeId
end

function raft_get_node()
    db_get(log, "NodeId")
end

"""
    raft_init_cluster()
Cluster bootstrap. Insert an entry in log with clusterid, node id, ipadress and
port of this node. When new nodes join this entry will be replicated to other
nodes.
"""
function raft_init_cluster(args)
    clusterId != nothing && throw(RavanaException("This node is part of cluster " *  hex(clusterId)))
    thisNode = nothing
    (arg) = args
    if length(args) == 0
        global nodeId    = rand(UInt128)
        global clusterId = rand(UInt128)
        nodes = raftNode[]
        thisNode = raftNode(ipAddress, ipPort, nodeId, clusterId)
        push!(nodes, thisNode)
        arg = nodes
    end
    raft_init_node(nodeId, clusterId)
    db_put(log, 1, (1, OP_INIT_CLUSTER, arg))
    db_put(log, "RavanaCluster", arg)

    global rInfo = raftInfo(1, nodeId, 1, 1)
    raft_save_info(rInfo)
    raft_set_state(FOLLOWER)

    return arg
end

# Ravana ops
const OP_INIT_CLUSTER            = Int32(1)
const OP_NONE                    = Int32(101)

const OP_UNKNOWN                 = Int32(10000)

# Lookup table for Core Ravana Ops
const op_table = Dict(OP_INIT_CLUSTER      =>  raft_init_cluster)
