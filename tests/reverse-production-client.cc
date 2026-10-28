// Keep client and daemon internals in separate translation units: both use
// distinct GrpcStream and NarSpool types in the same namespace.
#include "../src/client/store.hh"

#include <cassert>
#include <atomic>
#include <iostream>
#include <nix/store/derivations.hh>

extern "C" void nix_plugin_entry();
extern std::atomic<unsigned> reverseProbeNotFound;
extern std::atomic<unsigned> reverseClosureUploaded;

void clientDrivenReverseBuild(const std::string & farmUri, const std::string & sourceUri,
                               const std::string & drvPath)
{
    auto source = nix::openStore(sourceUri);
    // This standalone fixture links the real GrpcStore sources into the test
    // executable, rather than loading the normal plugin via Nix configuration.
    // Register that implementation explicitly for openStore(grpc://...).
    nix_plugin_entry();
    auto grpcStore = nix::openStore(farmUri);
    auto const path = source->parseStorePath(drvPath);
    // Supply the isolated evaluation store explicitly; the ordinary
    // buildDerivation entry point looks for the drv in the host /nix/store.
    auto * client = dynamic_cast<nix::GrpcStore *>(grpcStore.get_ptr().get());
    assert(client);
    auto requests = std::vector<nix::DerivedPath>{nix::DerivedPath::parse(*source, source->printStorePath(path) + "^out")};
    {
        // Without the opt-in the client refuses to send inputs to a reverse
        // builder; the build fails instead of retrying.
        auto const optIn = farmUri.find("&reverse-uploads=true");
        assert(optIn != std::string::npos);
        auto closedUri = farmUri;
        closedUri.erase(optIn, std::string_view("&reverse-uploads=true").size());
        auto closedStore = nix::openStore(closedUri);
        auto * closed = dynamic_cast<nix::GrpcStore *>(closedStore.get_ptr().get());
        assert(closed);
        auto refused = closed->dispatchBuild(requests, nix::bmNormal, source.get_ptr());
        assert(refused.size() == 1);
        auto const & failed = static_cast<nix::BuildResult &>(refused.front());
        auto const message = nixcompat::buildFailureMsg(failed);
        if (message) std::cerr << "fail-closed build: " << *message << "\n";
        assert(!nixcompat::succeeded(failed) && message && message->find("reverse-uploads") != std::string::npos);
        assert(reverseProbeNotFound == 1 && reverseClosureUploaded == 0);
        reverseProbeNotFound = 0;
    }
    auto results = client->dispatchBuild(requests, nix::bmNormal, source.get_ptr());
    assert(results.size() == 1);
    auto & result = static_cast<nix::BuildResult &>(results.front());
    if (auto failure = nixcompat::buildFailureMsg(result)) std::cerr << "client-driven build: " << *failure << "\n";
    assert(nixcompat::succeeded(result));
    assert(reverseProbeNotFound == 1 && reverseClosureUploaded == 1);
}

void clientDrivenConcurrentReverseBuild(const std::string & farmUri, const std::string & sourceUri,
                                        const std::string & first, const std::string & second)
{
    auto source = nix::openStore(sourceUri);
    auto grpcStore = nix::openStore(farmUri);
    auto * client = dynamic_cast<nix::GrpcStore *>(grpcStore.get_ptr().get());
    assert(client);
    std::vector<nix::DerivedPath> requests;
    for (auto const & drv : {first, second})
        requests.push_back(nix::DerivedPath::parse(*source, drv + "^out"));
    auto results = client->dispatchBuild(requests, nix::bmNormal, source.get_ptr());
    assert(results.size() == 2);
    assert(reverseProbeNotFound == 3 && reverseClosureUploaded == 3);
    for (auto const & result : results) {
        auto const & built = static_cast<const nix::BuildResult &>(result);
        if (auto failure = nixcompat::buildFailureMsg(built)) std::cerr << "concurrent build: " << *failure << "\n";
        assert(nixcompat::succeeded(built));
    }
}

void clientDrivenRecoveredReverseBuild(const std::string & farmUri, const std::string & sourceUri,
                                       const std::string & drvPath)
{
    auto source = nix::openStore(sourceUri);
    auto grpcStore = nix::openStore(farmUri);
    auto * client = dynamic_cast<nix::GrpcStore *>(grpcStore.get_ptr().get());
    assert(client);
    auto requests = std::vector<nix::DerivedPath>{nix::DerivedPath::parse(*source, drvPath + "^out")};
    auto results = client->dispatchBuild(requests, nix::bmNormal, source.get_ptr());
    assert(results.size() == 1);
    auto const & result = static_cast<const nix::BuildResult &>(results.front());
    if (auto failure = nixcompat::buildFailureMsg(result)) std::cerr << "recovered build: " << *failure << "\n";
    assert(nixcompat::succeeded(result));
}
