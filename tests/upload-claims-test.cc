#include <cassert>
#include <chrono>
#include <future>
#include <thread>

#include "upload-claims.hh"

using nixgrpc::UploadClaims;

namespace {

constexpr std::chrono::milliseconds kSettle{100};

auto isReady(const std::shared_future<void> & result) -> bool
{
    return result.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

void firstEntrantOwnsAndTheRestWait()
{
    UploadClaims claims;
    auto first = claims.enter("path");
    auto second = claims.enter("path");
    assert(first.owner);
    assert(!second.owner);
    assert(!isReady(second.result));
    first.promise.set_value();
    second.result.get();
}

void anOwnerThatGivesUpFailsItsWaiters()
{
    UploadClaims claims;
    auto waiter = [&claims] -> std::shared_future<void> {
        auto owner = claims.enter("path");
        return claims.enter("path").result;
    }();
    bool failed = false;
    try {
        waiter.get();
    } catch (const std::future_error &) {
        failed = true;
    }
    assert(failed);
}

void aFinishedPathCanBeUploadedAgain()
{
    UploadClaims claims;
    claims.enter("path").promise.set_value();
    auto again = claims.enter("path");
    assert(again.owner);
    again.promise.set_value();
}

void waitingBlocksUntilTheOwnerIsDone()
{
    UploadClaims claims;
    auto owner = claims.enter("path");
    auto other = claims.enter("path");
    std::thread thread([&other] -> void { other.result.get(); });
    std::this_thread::sleep_for(kSettle);
    assert(!isReady(other.result));
    owner.promise.set_value();
    thread.join();
}

void differentPathsDoNotInteract()
{
    UploadClaims claims;
    auto first = claims.enter("one");
    auto second = claims.enter("two");
    assert(first.owner);
    assert(second.owner);
    first.promise.set_value();
    second.promise.set_value();
}

} // namespace

auto main() -> int
try {
    firstEntrantOwnsAndTheRestWait();
    anOwnerThatGivesUpFailsItsWaiters();
    aFinishedPathCanBeUploadedAgain();
    waitingBlocksUntilTheOwnerIsDone();
    differentPathsDoNotInteract();
    return 0;
} catch (...) {
    return 1;
}
