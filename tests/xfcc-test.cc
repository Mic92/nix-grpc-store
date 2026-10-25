#include <cassert>
#include <optional>
#include <string>
#include <string_view>

#include "peer.hh"
#include "xfcc.hh"

using nixgrpc::Peer;
using nixgrpc::xfcc::elementPeer;
using nixgrpc::xfcc::TrustedProxies;

namespace {

auto commonName(std::string_view value) -> std::optional<std::string>
{
    return elementPeer(value).commonName;
}

auto spiffeId(std::string_view value) -> std::optional<std::string>
{
    return elementPeer(value).spiffeId;
}

} // namespace

auto main() -> int
try {
    assert(commonName(R"(By=spiffe://lb;Hash=abcd;Subject="CN=ci-1,OU=build,O=numtide";URI=spiffe://x)") == "ci-1");
    assert(commonName(R"(Hash=ab;Subject="O=nt,CN=dev")") == "dev");
    assert(commonName("Subject=CN=plain") == "plain");
    assert(commonName("Subject=/CN=legacy/O=x") == "legacy");
    assert(commonName(R"(Subject="CN=a\,b,O=x")") == "a,b");
    assert(commonName(R"(Subject="CN=q\"x")") == "q\"x");
    // Only the first element is the client; later ones are upstream hops
    // an attacker could have prepended behind a non-sanitizing proxy.
    assert(commonName(R"(Hash=1;URI=u,Hash=2;Subject="CN=evil")") == std::nullopt);
    assert(commonName(R"(Hash=1;Subject="O=only")") == std::nullopt);
    assert(commonName("") == std::nullopt);
    assert(commonName("garbage;;==,,") == std::nullopt);
    assert(commonName(R"(Subject="CN=)") == std::nullopt);
    assert(commonName(R"(Subject="CN=unterminated)") == std::nullopt);
    assert(commonName(R"(Subject="CN=x\)") == std::nullopt);
    assert(commonName("Subject=\"CN=a\nb\"") == std::nullopt);
    assert(commonName(std::string("Subject=CN=a") + '\0' + "b") == std::nullopt);
    assert(commonName("Subject=CN=" + std::string(300, 'a')) == std::nullopt);
    assert(commonName(std::string(20000, ';')) == std::nullopt);
    assert(commonName("Subject=CN=a;Subject=CN=b") == std::nullopt);

    assert(spiffeId(R"(By=spiffe://lb/x;Hash=ab;Subject="CN=ci-1";URI=spiffe://nt.example/ci/1)") == "spiffe://nt.example/ci/1");
    // A SPIRE SVID: no CN, the certificate still counts as present.
    {
        auto const peer = elementPeer(R"(Hash=ab;Subject="O=SPIRE,C=US";URI=spiffe://nt.example/ci)");
        assert(peer.commonName == "" && peer.spiffeId == "spiffe://nt.example/ci");
    }
    assert(spiffeId("Hash=ab;URI=spiffe://nt.example/ci") == "spiffe://nt.example/ci");
    // By is the proxy's URI, not the client's.
    assert(spiffeId("By=spiffe://nt.example/lb;Subject=CN=ci-1") == std::nullopt);
    assert(spiffeId("Subject=CN=ci-1;URI=spiffe://a.example/x;URI=spiffe://b.example/y") == std::nullopt);
    assert(spiffeId("Subject=CN=ci-1;URI=https://nt.example/ci") == std::nullopt);
    assert(spiffeId("Subject=CN=ci-1;URI=spiffe://nt.example/c*") == std::nullopt);
    assert(spiffeId("Subject=CN=ci-1;URI=spiffe://NT.example/ci") == std::nullopt);
    assert(spiffeId("Subject=CN=ci-1;URI=spiffe:///ci") == std::nullopt);
    assert(spiffeId("Hash=1;Subject=CN=ci-1,URI=spiffe://nt.example/hop") == std::nullopt);
    assert(!elementPeer("Hash=ab;URI=https://nt.example/ci").present());

    TrustedProxies proxies;
    assert(!proxies.matches(Peer{.commonName = "lb-1"}));
    proxies.add("lb-*");
    assert(proxies.matches(Peer{.commonName = "lb-1"}));
    assert(!proxies.matches(Peer{.commonName = "ci-1"}));
    assert(!proxies.matches(Peer{}));
    assert(!proxies.matches(Peer{.commonName = ""}));
    assert(!proxies.matches(Peer{.commonName = std::string("lb-1") + '\0' + "x"}));
    assert(!proxies.matches(Peer{.commonName = "", .spiffeId = "spiffe://nt.example/lb/1"}));
    proxies.add("spiffe://nt.example/lb/*");
    assert(proxies.matches(Peer{.commonName = "", .spiffeId = "spiffe://nt.example/lb/1"}));
    assert(!proxies.matches(Peer{.commonName = "spiffe://nt.example/lb/1"}));
    return 0;
} catch (...) {
    return 1;
}
