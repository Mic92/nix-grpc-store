#include "sandbox.hh"

#include <cstdint>
#include <string>
#include <vector>

#include <nix/util/error.hh>

#include "sbpl.hh"

extern "C" {
// Deprecated and missing from the SDK headers, but what Nix's own darwin
// build sandbox uses.
auto sandbox_init_with_parameters(
    const char * profile, uint64_t flags, const char * const parameters[], char ** errorbuf) -> int;
void sandbox_free_error(char * errorbuf);
}

namespace nixgrpc::sandbox {

void apply(const Policy & policy)
{
    auto const text = sbpl::profile(policy);
    auto const params = sbpl::parameters(policy);
    std::vector<const char *> raw;
    raw.reserve(params.size() + 1);
    for (auto const & param : params) {
        raw.push_back(param.c_str());
    }
    raw.push_back(nullptr);
    char * error = nullptr;
    if (sandbox_init_with_parameters(text.c_str(), 0, raw.data(), &error) != 0) {
        std::string const why = error != nullptr ? error : "unknown error";
        sandbox_free_error(error);
        throw nix::Error("sandbox_init: %s", why);
    }
}

} // namespace nixgrpc::sandbox
