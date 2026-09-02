#pragma once
// Shared helpers for the libFuzzer targets.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <string>
#include <string_view>

#include <nix/store/globals.hh>
#include <nix/util/configuration.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/util/logging.hh>
#include <nix/util/ref.hh>

namespace nixgrpc::fuzz {

// Stands in for a grpc::ServerReader/ClientReader: yields `data` as messages
// of `chunk` bytes so message-boundary handling is fuzzed too.
template<class Msg>
struct ChunkReader
{
    std::string_view data;
    size_t chunk;

    auto Read(Msg * msg) -> bool
    {
        if (data.empty()) {
            return false;
        }
        size_t const n = std::min(chunk, data.size());
        msg->set_data(std::string(data.substr(0, n)));
        data.remove_prefix(n);
        return true;
    }
};

// Collects everything a ZstdWriterSink ships into one byte string.
template<class Msg>
struct CollectWriter
{
    std::string out;

    auto Write(const Msg & msg) -> bool
    {
        out += msg.data();
        return true;
    }
};

// Splits the first byte off as a parameter and returns the rest.
inline auto takeByte(const uint8_t *& raw, size_t & size) -> uint8_t
{
    if (size == 0) {
        return 0;
    }
    uint8_t const value = *raw;
    ++raw;
    --size;
    return value;
}

inline auto view(const uint8_t * raw, size_t size) -> std::string_view
{
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return {reinterpret_cast<const char *>(raw), size};
}

// With NIX_GRPC_FUZZ_STRICT=1 a parse error aborts, so scripts/fuzz.sh can
// verify that generated seeds are actually well-formed.
inline void rejected(const std::exception & err)
{
    static bool const strict = [] -> bool {
        const char * env = std::getenv("NIX_GRPC_FUZZ_STRICT");
        return env != nullptr && *env == '1';
    }();
    if (strict) {
        std::fprintf(stderr, "seed rejected: %s\n", err.what());
        std::abort();
    }
}

// In-memory store for the worker-protocol deserialisers, which need a
// StoreDirConfig to parse store paths.
inline auto store() -> nix::Store &
{
    static nix::ref<nix::Store> const instance = []() -> nix::ref<nix::Store> {
        nix::initLibStore(false);
        nix::verbosity = nix::lvlError;
        nix::experimentalFeatureSettings.set("experimental-features", "ca-derivations dynamic-derivations impure-derivations");
        return nix::openStore("dummy://");
    }();
    return *instance;
}

} // namespace nixgrpc::fuzz
