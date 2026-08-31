// ACL rule parsing + CN matching, and the logfmt escaper the CN is logged
// through: a CN must never be able to break out of its log line.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>

#include <nix/util/error.hh>

#include "../src/acl.hh"
#include "../src/logfmt.hh"
#include "support.hh"

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t * raw, size_t size) -> int
{
    auto input = nixgrpc::fuzz::view(raw, size);

    // Layout: rule\nrule\n...\0cn
    auto sep = input.find('\0');
    auto rules = input.substr(0, sep);
    std::optional<std::string> commonName;
    if (sep != std::string_view::npos) {
        commonName = std::string(input.substr(sep + 1));
    }

    nixgrpc::Acl acl;
    while (!rules.empty()) {
        auto eol = rules.find('\n');
        try {
            acl.addRule(rules.substr(0, eol));
        } catch (nix::Error &) {
        }
        if (eol == std::string_view::npos) {
            break;
        }
        rules.remove_prefix(eol + 1);
    }
    auto role = acl.roleFor(commonName);
    if (acl.active() && commonName && commonName->contains('\0') && role) {
        std::abort();
    }

    auto escaped = nixgrpc::logfmtValue(commonName.value_or(""));
    if (escaped.contains('\n') || escaped.empty()) {
        std::abort();
    }
    return 0;
}
