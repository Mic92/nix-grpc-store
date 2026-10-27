// relayBuildLog cuts over-long strings instead of failing the build.

#include <cassert>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include <nix/store/worker-protocol.hh>
#include <nix/util/serialise.hh>

#include "build-log.hh"

namespace {

void longLinesAreCut()
{
    std::string const huge(nixgrpc::kMaxLogString + 3, 'x');
    nix::StringSink sink;
    sink << STDERR_NEXT << huge;
    sink << STDERR_START_ACTIVITY << uint64_t{1} << uint64_t{0} << uint64_t{0} << huge << uint64_t{0} << uint64_t{0};
    sink << STDERR_NEXT << "after";
    sink << STDERR_LAST;

    nix::StringSource source(sink.s);
    std::vector<std::string> lines;
    nixgrpc::relayBuildLog(source, [&](nixgrpc::BuildEvent event) -> void { lines.push_back(std::move(event.text)); });
    assert(lines.size() == 2);
    assert(lines.at(0) == huge.substr(0, nixgrpc::kMaxLogString));
    assert(lines.at(1) == "after");
}

} // namespace

auto main() -> int
{
    try {
        longLinesAreCut();
    } catch (std::exception & err) {
        std::cerr << "error: " << err.what() << "\n";
        return 1;
    }
    return 0;
}
