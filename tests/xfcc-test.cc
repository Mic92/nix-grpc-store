#include <cassert>
#include <optional>
#include <string>

#include "xfcc.hh"

using nixgrpc::xfcc::subjectCommonName;
using nixgrpc::xfcc::TrustedProxies;

auto main() -> int
try {
    assert(
        subjectCommonName(R"(By=spiffe://lb;Hash=abcd;Subject="CN=ci-1,OU=build,O=numtide";URI=spiffe://x)") == "ci-1");
    assert(subjectCommonName(R"(Hash=ab;Subject="O=nt,CN=dev")") == "dev");
    assert(subjectCommonName("Subject=CN=plain") == "plain");
    assert(subjectCommonName("Subject=/CN=legacy/O=x") == "legacy");
    assert(subjectCommonName(R"(Subject="CN=a\,b,O=x")") == "a,b");
    assert(subjectCommonName(R"(Subject="CN=q\"x")") == "q\"x");

    // only the first element (the hop that saw the client) counts
    assert(subjectCommonName(R"(Hash=1;URI=u,Hash=2;Subject="CN=evil")") == std::nullopt);
    assert(subjectCommonName(R"(Hash=1;Subject="O=only")") == std::nullopt);
    assert(subjectCommonName("") == std::nullopt);
    assert(subjectCommonName("garbage;;==,,") == std::nullopt);
    assert(subjectCommonName(R"(Subject="CN=)") == std::nullopt);
    assert(subjectCommonName(R"(Subject="CN=unterminated)") == std::nullopt);
    assert(subjectCommonName(R"(Subject="CN=x\)") == std::nullopt);
    assert(subjectCommonName(std::string("Subject=\"CN=a\nb\"")) == std::nullopt);
    assert(subjectCommonName(std::string("Subject=CN=a") + '\0' + "b") == std::nullopt);
    assert(subjectCommonName("Subject=CN=" + std::string(300, 'a')) == std::nullopt);
    assert(subjectCommonName(std::string(20000, ';')) == std::nullopt);

    TrustedProxies proxies;
    assert(!proxies.matches(std::string("lb-1")));
    proxies.add("lb-*");
    assert(proxies.matches(std::string("lb-1")));
    assert(!proxies.matches(std::string("ci-1")));
    assert(!proxies.matches(std::nullopt));
    assert(!proxies.matches(std::string("")));
    assert(!proxies.matches(std::string("lb-1") + '\0' + "x"));
    return 0;
} catch (...) {
    return 1;
}
