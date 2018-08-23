# Client and server for communicating with the RAFT cluster
using Sockets

const RAFT_API_VERSION = 1
const RAFT_JULIA_CLIENT  = 1

function process_preamble(h::UInt64)
    size    = UInt32(h & 0xffffffff)
    version = UInt16((h >> 32) & 0xffff)
    flags   = UInt16((h >> 48) & 0xffff)
    println("size=$size version=$version flags=$flags", " ", h)
    (size, version, flags)
end

function get_opt(sock, lookup_table)
    (size::UInt32, version::UInt16, flags::UInt16) = process_preamble(read(sock, UInt64))
    if version != RAFT_API_VERSION
        throw(RavanaException("Unsupported version $(version)"))
    end

    (op, argv) = array_to_type(read(sock, size))
    func = lookup_table[op]
    println("op: ", op, "   argv: ", argv)
    return (op, func, argv)
end

"""

"""
function ravana_server(;address=IPv4(0), port=2000)
    @async begin
        server = 0 # Init server socket
        sockErr = true
        println("In ravana_server")
        while (sockErr)
            try
                server = listen(address, port)
                sockErr = false
                println("Starting cluster communication server at $(address):$(port)")
            catch e
                println("Could not listen on port $(port). Trying $(port + 1)")
                port += 1  # Try next port
            end
        end

        while true
            sock = accept(server)
            if isopen(sock) != true
                throw(RaftException("Error! Socket not open"))
            end
            # Disassemble op and arguments
            try
                (op, func, argv) = get_opt(sock, op_table)
                if op == OP_UNKNOWN continue end
                # Execute on cluster
                ret = raft_cluster_execute(op, func, argv)
                # Return result to client
                b = byte_array(ret)
                write(sock, length(b), b)
            catch e
                b = byte_array(e)
                write(sock, length(b), b)
            end
            close(sock)
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

# If leader execute op, otherise redirect to leader
function raft_cluster_execute(op, func, argv)
    if op == OP_INIT_CLUSTER
        ret = raft_execute(op, func, argv) # Bootstrap bypasses append entries
    elseif  current_state == LEADER
        ret = raft_run_command(op, func, argv) # Commits as per RAFT protocol
    else
        ret = ravana_client(leaderAddress, leaderPort, op, argv) # Redirect to leader
    end
    ret
end

function init_cluster(;address=IPv4(0), port=2000)
    ravana_client(address, port, Ravana.OP_INIT_CLUSTER)
end
