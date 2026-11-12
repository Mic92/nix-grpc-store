#pragma once
// A client's request stream. Reading past its end throws ClientGone, so the
// end of a client's stream cannot be taken for the end of some other one,
// such as the connection to the local nix-daemon.

#include <nix/util/error.hh>

namespace nixgrpc {

struct ClientGone : nix::Error
{
    using nix::Error::Error;
};

template<typename Reader>
class ClientStream
{
public:
    explicit ClientStream(Reader & reader)
        : reader_(&reader)
    {
    }

    template<typename Msg>
    auto Read(Msg * msg) -> bool // NOLINT(readability-identifier-naming): gRPC's reader interface
    {
        if (!reader_->Read(msg)) {
            throw ClientGone("the client ended its stream early");
        }
        return true;
    }

private:
    Reader * reader_;
};

} // namespace nixgrpc
