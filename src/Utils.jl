using Serialization

sleep_random() = sleep(random(Int)%13 * CANDIDATE_SLEEP_GRANULARITY)

is_majority(votes) = return (votes < (length(nodes) + 1)/2 ? false : true)

"""
    byte_array(x)

Serialize Julia type *x* such that it can be written to
a db or file
"""
function byte_array(x)
    iob = IOBuffer()
    serialize(iob, x)
    iob.data
end


struct KVSSerializeException <: Exception
    msg::String
end

"""
    array_to_type{T}(arr, ::Type{T})

Deserialize from Array{UInt8} back to Julia type.
"""
function array_to_type(arr, ::Type{T}) where T
    iob = IOBuffer(arr)
    seek(iob, 0)
    t = deserialize(iob)
    if ! isa(t, T)
        throw(KVSSerializeException("Could not deserialize to type"))
    end
    t
end

function array_to_type(arr)
    iob = IOBuffer(arr)
    seek(iob, 0)
    t = deserialize(iob)
    t
end

const DEFAUT_BASE_DIR = "/tmp"
function basedir()
    b = DEFAUT_BASE_DIR
    try
        b =  ENV["RAVANA_HOME"] * "/.ravana/"
    catch e
        b = DEFAUT_BASE_DIR * "/.ravana/"
    end

    try
        stat(b).inode == 0 && Base.mkdir(b)
    catch e
        error("Cannot access $(b)")
    end
    b
end
