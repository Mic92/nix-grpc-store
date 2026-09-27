#include <cassert>
#include <string>
#include <vector>

#include <nix/util/serialise.hh>
#include <sys/resource.h>

#include "upload-spool.hh"

namespace {

constexpr rlim_t kDescriptorLimit = 32;

void manyNarsNeedOneDescriptor()
{
    rlimit limit{};
    assert(getrlimit(RLIMIT_NOFILE, &limit) == 0);
    limit.rlim_cur = kDescriptorLimit;
    assert(setrlimit(RLIMIT_NOFILE, &limit) == 0);

    constexpr int kNars = 200;
    nixgrpc::NarSpool spool;
    std::vector<nixgrpc::SpoolRange> nars;
    for (int i = 0; i < kNars; i++) {
        std::string const content = "nar-" + std::to_string(i) + std::string(i, 'x');
        nix::StringSource source(content);
        nars.push_back(spool.add(source, content.size()));
    }
    for (int i = kNars - 1; i >= 0; i--) {
        std::string const content = "nar-" + std::to_string(i) + std::string(i, 'x');
        auto reader = spool.reader(nars.at(i));
        assert(reader.drain() == content);
    }
}

void discardedBytesAreNotKept()
{
    nixgrpc::NarSpool spool;
    std::string const skipped(1000, 's');
    nix::StringSource skip(skipped);
    nixgrpc::NarSpool::discard(skip, skipped.size());
    assert(skip.drain().empty());

    std::string const kept = "kept";
    nix::StringSource source(kept);
    auto reader = spool.reader(spool.add(source, kept.size()));
    assert(reader.drain() == kept);
}

} // namespace

auto main() -> int
try {
    manyNarsNeedOneDescriptor();
    discardedBytesAreNotKept();
    return 0;
} catch (...) {
    return 1;
}
