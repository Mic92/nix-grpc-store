#include <cassert>
#include <chrono>
#include <thread>

#include "upload-claims.hh"

using nixgrpc::UploadClaims;
using Outcome = UploadClaims::Outcome;

namespace {

void firstEntrantOwnsAndTheRestWait()
{
    UploadClaims claims;
    auto first = claims.enter("p");
    auto second = claims.enter("p");
    assert(first.owner && !first.waiter);
    assert(!second.owner && second.waiter);
    first.owner->done();
    assert(second.waiter->wait() == Outcome::done);
}

void anOwnerThatDiesFailsItsWaiters()
{
    UploadClaims claims;
    auto owner = std::make_optional(claims.enter("p"));
    auto other = claims.enter("p");
    owner.reset();
    assert(other.waiter->wait() == Outcome::failed);
}

void aWaiterThatNeverWaitsDoesNotBlockTheNextUpload()
{
    UploadClaims claims;
    {
        auto owner = claims.enter("p");
        auto other = claims.enter("p");
        owner.owner->done();
    }
    auto again = claims.enter("p");
    assert(again.owner);
    again.owner->done();
}

void waitingBlocksUntilTheOwnerIsDone()
{
    UploadClaims claims;
    auto owner = claims.enter("p");
    auto other = claims.enter("p");
    Outcome seen = Outcome::pending;
    std::thread thread([&] { seen = other.waiter->wait(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(seen == Outcome::pending);
    owner.owner->done();
    thread.join();
    assert(seen == Outcome::done);
}

void differentPathsDoNotInteract()
{
    UploadClaims claims;
    auto a = claims.enter("a");
    auto b = claims.enter("b");
    assert(a.owner && b.owner);
    a.owner->done();
    b.owner->done();
}

} // namespace

auto main() -> int
try {
    firstEntrantOwnsAndTheRestWait();
    anOwnerThatDiesFailsItsWaiters();
    aWaiterThatNeverWaitsDoesNotBlockTheNextUpload();
    waitingBlocksUntilTheOwnerIsDone();
    differentPathsDoNotInteract();
    return 0;
} catch (...) {
    return 1;
}
