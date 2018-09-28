# Client and server for communicating with the RAFT cluster
using Sockets

const RAFT_API_VERSION = 1
const RAFT_PROTO_VERSION = 1
const RAFT_JULIA_CLIENT  = 0x1
const RAFT_PROTO_CLIENT  = 0x2

const COMMS_PORT = 2000

function process_preamble(h::UInt64)
    size    = UInt32(h & 0xffffffff)
    version = UInt16((h >> 32) & 0xffff)
    flags   = UInt16((h >> 48) & 0xffff)
    @debug ("size=$size version=$version flags=$flags", " ", h)
    (size, version, flags)
end

function check_version(version, flags, lookup_table)
    if (flags & RAFT_JULIA_CLIENT) == RAFT_JULIA_CLIENT
        (version != RAFT_API_VERSION) && throw(RavanaException("Unsupported API version $(version)"))
    elseif (flags & RAFT_PROTO_CLIENT) == RAFT_PROTO_CLIENT
        (version != RAFT_PROTO_VERSION) && throw(RavanaException("Unsupported proto version $(version)"))
    else
        throw(RavanaException("Bad protocol header. Unknown flag $(flags)"))
    end
end

# Disassemble protocol header
function get_opt(sock, lookup_table)
    (size::UInt32, version::UInt16, flags::UInt16) = process_preamble(read(sock, UInt64))
    check_version(version, flags, lookup_table)

    (op, argv) = array_to_type(read(sock, size))
    !haskey(lookup_table, op) && throw(RavanaException("Invalid op $(op)"))
    func = lookup_table[op]
    @debug ("op: ", op, "   argv: ", argv)
    return (op, func, argv)
end

"""

"""
function ravana_server(;address=IPv4(0), port=COMMS_PORT)
    @async begin
        server = 0 # Init server socket
        sockErr = true
        @debug ("In ravana_server")
        while (sockErr)
            try
                server = listen(address, port)
                sockErr = false
                @info ("Starting cluster communication server at $(address):$(port)")
            catch e
                @info ("Could not listen on port $(port). Trying $(port + 1)")
                port += 1  # Try next port
            end
        end

        while true
            sock = accept(server)
            if isopen(sock) != true
                throw(RaftException("Error! Socket not open"))
            end
            # Disassemble op and arguments
            @async begin
                try
                    (op, func, argv) = get_opt(sock, op_table)
                    # Execute on cluster
                    ret = raft_cluster_execute(op, func, argv)
                    # Return result to client
                    b = byte_array(ret)
                    write(sock, length(b), b)
                catch e
                    @error ("ravana_server(): Exception! ", e)
                    b = byte_array(e)
                    write(sock, length(b), b)
                end
                close(sock)
            end
        end
    end
end

"""
    ravana_client(address, port, op::Int32, argv...)
Low level function that can be called from a Julia prompt/program.
```jldoxctest
julia> Ravana.ravana_client(IPv4(0), 2000, Ravana.OP_GET_NODE_PARAMS)

```
"""
function ravana_client(address, port, op::Int32, argv...)
    bytes = byte_array((op, argv))
    size = UInt32(length(bytes))
    version = UInt16(RAFT_API_VERSION)
    flags = UInt16(RAFT_JULIA_CLIENT)
    client = connect(address, port)

    ret = write(client, size, version, flags, bytes)
    ret_size = read(client, Int)
    ret = array_to_type(read(client, ret_size))
    close(client)
    ret
end

# If leader execute op, otherwise redirect to leader
function raft_cluster_execute(op, func, argv)
    @debug ("In raft_cluster_execute")
    if op == OP_INIT_CLUSTER
        ret = raft_execute(op, func, argv) # Bootstrap bypasses append entries
    elseif  current_state == LEADER
        @debug("Leader before executing raft_run_command")
        ret = raft_run_command(op, func, argv) # Commits as per RAFT protocol
    else
        ret = ravana_client(leaderAddress, leaderPort, op, argv) # Redirect to leader
    end
    ret
end

function init_cluster(;address=IPv4(0), port=2000)
    ravana_client(address, port, OP_INIT_CLUSTER, nothing)
end

function get_cluster_config(node::raftNode)
    ravana_client(node.name, node.port, OP_GET_CLUSTER_CONF, nothing)
end

get_cluster_config() = get_cluster_config(raftNode(leaderAddress, COMMS_PORT, Int128(0), Int128(0)))
