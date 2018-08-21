# Intra cluster communication
using Sockets

const RAFT_PROTO_VERSION = 1

"""
    raft_server()
Takes raft protocol requests from another raft node.
"""
function raft_server(;address=IPv4(0), port=1346)
    @async begin
        server = 0 # Init server socket
        sockErr = true
        while (sockErr)
            try
                server = listen(address, port)
                sockErr = false
                println("Starting Raft protocol server at $(address):$(port)")
            catch e
                port += 1  # Try next port
            end
        end

        global ipAddress = address
        global ipPort   = port

        while true
            sock = accept(server)
            if isopen(sock) != true
                throw(RavanaException("Error! Socket not open"))
            end
            # Disassemble op and arguments
            (op, func, argv) = get_opt(sock, raft_proto_table)
            if op == OP_UNKNOWN continue end
            # Execute RAFT protocol command on this node
            #try
                ret = func(argv)
                b = byte_array(ret)
                write(sock, length(b), b)
            #= catch e
                println("Protocol Server Error: ", e)
                b = byte_array(e)
                write(sock, length(b), b)
            end
            =#
            close(sock)
        end
    end
end

"""
    raft_client()
Called by one raft node to communicate with another raft node.
"""
function raft_client(address, port, proto_op::Int32, argv)
    bytes = byte_array((proto_op, argv))
    size = UInt32(length(bytes))
    version = UInt16(RAFT_PROTO_VERSION)
    flags = UInt16(0)
    client = connect(address, port)

    write(client, size, version, flags, bytes)
    ret_size = read(client, Int)
    ret = array_to_type(read(client, ret_size))
    close(client)
    ret
end



# Raft protocol ops
const RAFT_REQUEST_VOTE          = Int32(1)
const RAFT_APPEND_ENTRIES        = Int32(2)

const OP_UNKNOWN                 = Int32(10000)

# Lookup table for Raft protocol Ops
const raft_proto_table = Dict(RAFT_APPEND_ENTRIES      =>  local_append_entries,
                              RAFT_REQUEST_VOTE        =>  local_request_vote)
