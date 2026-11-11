// nixgrpc::Call against an in-process server.

#include <atomic>
#include <cassert>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/support/status.h>

#include <nix/util/signals.hh>

// Not standalone: needs signals.hh first, so keep it in a block of its own.
#include <nix/util/signals-impl.hh> // IWYU pragma: keep

#include "channel.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"

namespace {

class OkService final : public nix::remote::NixRemote::Service
{
public:
    auto StoreInfo(grpc::ServerContext * /*ctx*/, const nix::remote::StoreInfoRequest * /*request*/,
                   nix::remote::StoreInfoReply * /*reply*/) -> grpc::Status override
    {
        return grpc::Status::OK;
    }
};

struct Fixture
{
    OkService service;
    std::unique_ptr<grpc::Server> server;
    std::unique_ptr<nix::remote::NixRemote::Stub> stub;

    Fixture()
    {
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        server = builder.BuildAndStart();
        assert(server);
        stub = nix::remote::NixRemote::NewStub(
            grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
    }

    auto storeInfo(nixgrpc::Call & call) const -> grpc::Status
    {
        nix::remote::StoreInfoRequest const request;
        nix::remote::StoreInfoReply reply;
        return stub->StoreInfo(&call.ctx(), request, &reply);
    }
};

// A SIGINT after the flag check but before the callback exists must still
// cancel the call. The hook raises it from inside checkInterrupt().
void interruptDuringSetupCancelsTheCall()
{
    Fixture const fix;
    bool fired = false;
    nix::unix::interruptCheck = [&fired] -> bool {
        if (!fired) {
            fired = true;
            nix::unix::triggerInterrupt();
        }
        return false;
    };
    grpc::Status status;
    {
        nixgrpc::Call call;
        status = fix.storeInfo(call);
    }
    nix::unix::interruptCheck = nullptr;
    nix::setInterrupted(false);
    assert(fired);
    assert(status.error_code() == grpc::StatusCode::CANCELLED);
}

void startingAfterInterruptThrows()
{
    nix::setInterrupted(true);
    bool threw = false;
    try {
        nixgrpc::Call const call;
    } catch (nix::Interrupted &) {
        threw = true;
    }
    nix::setInterrupted(false);
    assert(threw);
}

// Nix copies an interrupt callback under its lock and calls it outside, so
// it can run after its Call unregistered it. Only a sanitizer sees the
// difference.
void interruptRacingTheEndOfACallIsHarmless()
{
    std::atomic<bool> stop = false;
    std::thread interrupter([&stop] -> void {
        while (!stop) {
            nix::unix::triggerInterrupt();
        }
    });
    constexpr int calls = 20'000;
    for (int i = 0; i < calls; i++) {
        try {
            nixgrpc::Call const call;
        } catch (nix::Interrupted &) {
            nix::setInterrupted(false);
        }
    }
    stop = true;
    interrupter.join();
    nix::setInterrupted(false);
}

} // namespace

auto main() -> int
try {
    interruptDuringSetupCancelsTheCall();
    startingAfterInterruptThrows();
    interruptRacingTheEndOfACallIsHarmless();
    return 0;
} catch (...) {
    return 1;
}
