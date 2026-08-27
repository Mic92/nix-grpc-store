// Nix store plugin that tunnels the worker protocol over a gRPC bidirectional
// stream. The gRPC layer is a dumb byte pipe; all store operations are handled
// by the existing RemoteStore implementation. The `nix copy` hot path
// (queryValidPaths, queryPathInfo, addMultipleToStore, narFromPath) uses
// dedicated RPCs when the server supports them.
//
// URI: grpc://host:port
// Params: insecure, ca-cert, client-cert, client-key
// Unset TLS params fall back to standard locations, see defaultCaCert() and
// defaultClientCred().

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/status.h>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <nix/store/globals.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-reference.hh>
#include <nix/util/configuration.hh>
#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>
#include <nix/util/file-system.hh>
#include <nix/util/logging.hh>
#include <nix/util/ref.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/serialise.hh>
#include <nix/util/types.hh>
#include <nix/util/util.hh>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/types.h>
#include <openssl/x509.h>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <variant>
#include <vector>

#include <grpcpp/grpcpp.h>

#include <nix/store/path-info.hh>
#include <nix/store/remote-store-connection.hh>
#include <nix/store/remote-store.hh>
#include <nix/store/store-registration.hh>
#include <nix/store/worker-protocol.hh>
// Generic definitions for the std::vector serialiser wrappers, which
// libnixstore does not instantiate explicitly.
#include <nix/store/worker-protocol-impl.hh> // IWYU pragma: keep
#include <nix/util/callback.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/url.hh>
#include <utility>

#include "nix-compat.hh"
#include "nar-fetcher.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"

using GrpcStream = grpc::ClientReaderWriter<nix::remote::Chunk, nix::remote::Chunk>;

namespace {

// First readable candidate, or empty.
auto firstReadable(const std::vector<std::string> &candidates) -> std::string {
  for (const auto &path : candidates) {
    // NOLINTNEXTLINE(misc-include-cleaner): R_OK comes from <unistd.h>
    if (::access(path.c_str(), R_OK) == 0) {
      return path;
    }
  }
  return "";
}

// The server rejects an expired client cert during the TLS handshake, which
// gRPC reports only as "Socket closed", so check up front.
auto readValidCert(const std::string &path) -> std::string {
  auto pem = nix::readFile(path);
  std::unique_ptr<BIO, decltype(&BIO_free)> const bio{
      BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())), BIO_free};
  std::unique_ptr<X509, decltype(&X509_free)> const cert{
      PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr), X509_free};
  if (!cert) {
    throw nix::Error("client certificate '%s' is not a valid PEM certificate", path);
  }
  if (X509_cmp_current_time(X509_get0_notAfter(cert.get())) < 0) {
    throw nix::Error("client certificate '%s' has expired", path);
  }
  if (X509_cmp_current_time(X509_get0_notBefore(cert.get())) > 0) {
    throw nix::Error("client certificate '%s' is not yet valid", path);
  }
  return pem;
}

// Same lookup order as Nix's ssl-cert-file setting.
auto defaultCaCert() -> std::string {
  std::vector<std::string> candidates;
  for (const auto *env : {"NIX_SSL_CERT_FILE", "SSL_CERT_FILE"}) {
    if (auto value = nix::getEnv(env)) {
      candidates.push_back(*value);
    }
  }
  candidates.emplace_back("/etc/ssl/certs/ca-certificates.crt");
  candidates.emplace_back("/nix/var/nix/profiles/default/etc/ssl/certs/ca-bundle.crt");
  return firstReadable(candidates);
}

// Env override, then per-user XDG data dir, then system-wide location.
auto defaultClientCred(const char *envVar, const std::string &fileName) -> std::string {
  std::vector<std::string> candidates;
  if (auto value = nix::getEnv(envVar)) {
    candidates.push_back(*value);
  }
  if (auto dataHome = nix::getEnv("XDG_DATA_HOME")) {
    candidates.push_back(*dataHome + "/nix-grpc-store/" + fileName);
  } else if (auto home = nix::getEnv("HOME")) {
    candidates.push_back(*home + "/.local/share/nix-grpc-store/" + fileName);
  }
  candidates.push_back("/run/nix-grpc-store/" + fileName);
  candidates.push_back("/var/lib/nix-grpc-store/" + fileName);
  return firstReadable(candidates);
}

} // namespace

namespace nix {

// Follows Nix's own StoreConfig pattern (e.g. DummyStoreConfig).
// NOLINTNEXTLINE(misc-multiple-inheritance,misc-use-internal-linkage)
struct GrpcStoreConfig : std::enable_shared_from_this<GrpcStoreConfig>, virtual RemoteStoreConfig
{
private:
    friend struct GrpcStore;

    ParsedURL::Authority authority;

    Setting<bool> insecure{this, false, "insecure", "Use plaintext instead of TLS. Only for local testing."};

    Setting<std::string> caCert{
        this,
        "",
        "ca-cert",
        "Path to a PEM file with the CA certificate used to verify the server. "
        "Defaults to `$NIX_SSL_CERT_FILE`, `$SSL_CERT_FILE` or the system CA bundle."};

    Setting<std::string> clientCert{
        this,
        "",
        "client-cert",
        "Path to a PEM client certificate chain to present for mTLS. Defaults to "
        "`$NIX_GRPC_CLIENT_CERT`, then `client.crt` in `$XDG_DATA_HOME/nix-grpc-store`, "
        "`/run/nix-grpc-store` or `/var/lib/nix-grpc-store`."};

    Setting<std::string> clientKey{
        this,
        "",
        "client-key",
        "Path to the PEM private key for `client-cert`. Defaults to "
        "`$NIX_GRPC_CLIENT_KEY`, then `client.key` next to the default `client-cert`."};

    Setting<unsigned> narConnections{
        this,
        4,
        "nar-connections",
        "Number of parallel TCP connections used for NAR downloads."};

public:
    GrpcStoreConfig(const Params & params)
        : StoreConfig(NIX_COMPAT_STORE_CONFIG_ARGS(params))
        , RemoteStoreConfig(NIX_COMPAT_STORE_CONFIG_ARGS(params))
    {
    }

#if !NIX_COMPAT_AT_LEAST(2, 34)
    // Nix < 2.34 constructs store configs from the raw scheme and authority
    // strings instead of a pre-parsed ParsedURL::Authority.
    GrpcStoreConfig(std::string_view /*scheme*/, std::string_view authority, const Params &params)
        : StoreConfig(params),
          RemoteStoreConfig(params),
          authority(ParsedURL::Authority::parse(authority)) {}
#endif

    GrpcStoreConfig(ParsedURL::Authority authority, const Params &params)
        : StoreConfig(NIX_COMPAT_STORE_CONFIG_ARGS(params)),
          RemoteStoreConfig(NIX_COMPAT_STORE_CONFIG_ARGS(params)),
          authority(std::move(authority)) {}

    static auto name() -> std::string { return "gRPC Store"; }

    static auto uriSchemes() -> StringSet { return {"grpc"}; }

    static auto doc() -> std::string {
      return "Connects to a `nix-grpc-daemon` and tunnels the Nix worker "
             "protocol over a gRPC bidirectional stream.";
    }

    auto getReference() const -> StoreReference override {
      return {
          .variant =
              StoreReference::Specified{
                  .scheme = *uriSchemes().begin(),
                  .authority = authority.to_string(),
              },
          .params = getQueryParams(),
      };
    }

    auto openStore() const -> ref<Store> override;
};

// NOLINTNEXTLINE(misc-multiple-inheritance): inherited from Nix's store hierarchy
struct GrpcStore : virtual RemoteStore
{
    using Config = GrpcStoreConfig;

private:
    ref<const Config> config;

    std::shared_ptr<grpc::ChannelCredentials> creds;
    bool haveClientCert = false;

    /* One channel is shared by all connections in the pool; gRPC multiplexes
       streams over it internally. The stub keeps the channel alive. */
    std::unique_ptr<remote::NixRemote::Stub> stub;

    nixgrpc::NarFetcher narFetcher;

    auto makeChannel(bool ownConnection) -> std::shared_ptr<grpc::Channel>
    {
      grpc::ChannelArguments args;
      // The worker protocol streams NARs; do not cap message size.
      args.SetMaxReceiveMessageSize(-1);
      args.SetMaxSendMessageSize(-1);
      if (ownConnection) {
        // Private subchannel pool = own TCP connection.
        args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
      }
      auto channel = grpc::CreateCustomChannel(config->authority.to_string(), creds, args);
      // Start TCP+TLS setup now instead of stalling the first RPC.
      channel->GetState(true);
      return channel;
    }

public:
    GrpcStore(const ref<const Config> &config)
        : Store{*config}, RemoteStore{*config}, config{config},
          narFetcher{
              [this] -> std::shared_ptr<grpc::Channel> { return makeChannel(true); },
              config->authority.to_string(), config->narConnections} {
      if (config->insecure) {
        creds = grpc::InsecureChannelCredentials();
      } else {
        grpc::SslCredentialsOptions ssl;
        auto caCert = config->caCert.get().empty() ? defaultCaCert() : config->caCert.get();
        if (!caCert.empty()) {
          ssl.pem_root_certs = readFile(caCert);
        }
        auto clientCert = config->clientCert.get();
        auto clientKey = config->clientKey.get();
        if (clientCert.empty() != clientKey.empty()) {
          throw Error("gRPC store '%s': client-cert and client-key must be set together",
                      config->authority.to_string());
        }
        if (clientCert.empty()) {
          clientCert = defaultClientCred("NIX_GRPC_CLIENT_CERT", "client.crt");
          clientKey = defaultClientCred("NIX_GRPC_CLIENT_KEY", "client.key");
        }
        if (!clientCert.empty() && !clientKey.empty()) {
          ssl.pem_cert_chain = readValidCert(clientCert);
          ssl.pem_private_key = readFile(clientKey);
          haveClientCert = true;
        }
        creds = grpc::SslCredentials(ssl);
      }

      stub = remote::NixRemote::NewStub(makeChannel(false));
    }

    GrpcStore(const GrpcStore &) = delete;
    GrpcStore(GrpcStore &&) = delete;
    auto operator=(const GrpcStore &) -> GrpcStore & = delete;
    auto operator=(GrpcStore &&) -> GrpcStore & = delete;

    auto getBuildLogExact(const StorePath & /*path*/)
        -> std::optional<std::string> override {
      unsupported("getBuildLogExact");
    }

private:
    auto statusError(const grpc::Status & status, const char * opName) const -> Error
    {
      std::string hint;
      auto code = status.error_code();
      // UNAVAILABLE: a TLS-level rejection shows up only as "Socket closed".
      if (!config->insecure &&
          (code == grpc::StatusCode::UNAUTHENTICATED || code == grpc::StatusCode::UNAVAILABLE)) {
        hint = haveClientCert
                   ? "\nhint: the server may have rejected the client certificate "
                     "(untrusted CA or revoked)."
                   : "\nhint: no client certificate was presented. If the server requires "
                     "mTLS, set the 'client-cert'/'client-key' store parameters or install "
                     "client.crt/client.key in $XDG_DATA_HOME/nix-grpc-store or "
                     "/var/lib/nix-grpc-store.";
      }
      // NOLINTNEXTLINE(modernize-return-braced-init-list): Error ctor is explicit
      return Error("gRPC %s on '%s' failed: %s%s", opName, config->authority.to_string(),
                   status.error_message(), hint);
    }

    void checkStatus(const grpc::Status & status, const char * opName) const
    {
      if (!status.ok()) {
        throw statusError(status, opName);
      }
    }

public:
    auto queryValidPaths(const StorePathSet &paths,
                         SubstituteFlag maybeSubstitute)
        -> StorePathSet override {
      grpc::ClientContext ctx;
      remote::QueryValidPathsRequest request;
      request.set_substitute(maybeSubstitute == Substitute);
      for (const auto &path : paths) {
        request.add_paths(std::string(path.to_string()));
      }
      remote::QueryValidPathsReply reply;
      checkStatus(stub->QueryValidPaths(&ctx, request, &reply),
                  "QueryValidPaths");
      StorePathSet res;
      for (const auto &path : reply.paths()) {
        res.insert(StorePath(path));
      }
      return res;
    }

    // RemoteStore would tunnel this. One RPC, with a fallback to the
    // client-side walk for servers without QueryMissing.
    auto queryMissing(const std::vector<DerivedPath> & targets)
        -> MissingPaths override {
      grpc::ClientContext ctx;
      remote::QueryMissingRequest request;
      for (const auto & target : targets) {
        request.add_targets(target.to_string(*this));
      }
      remote::QueryMissingReply reply;
      auto status = stub->QueryMissing(&ctx, request, &reply);
      if (status.error_code() == grpc::StatusCode::UNIMPLEMENTED) {
        // Deliberate: RemoteStore::queryMissing would tunnel, the generic
        // Store walk works with native path queries.
        // NOLINTNEXTLINE(bugprone-parent-virtual-call)
        return Store::queryMissing(targets);
      }
      checkStatus(status, "QueryMissing");
      MissingPaths res;
      for (const auto & path : reply.will_build()) {
        res.willBuild.insert(StorePath(path));
      }
      for (const auto & path : reply.will_substitute()) {
        res.willSubstitute.insert(StorePath(path));
      }
      for (const auto & path : reply.unknown()) {
        res.unknown.insert(StorePath(path));
      }
      res.downloadSize = reply.download_size();
      res.narSize = reply.nar_size();
      return res;
    }

    // Same: keep read-only clients off the tunnel.
    auto isValidPathUncached(const StorePath & path) -> bool override {
      return queryValidPaths({path}, NoSubstitute).contains(path);
    }

    // The build hook probes reachability at store open. One StoreInfo RPC
    // answers it and caches the trust flag, no tunnel needed.
    void connect() override { isTrustedClient(); }

    auto isTrustedClient() -> std::optional<TrustedFlag> override {
      std::call_once(trustedOnce, [&]() -> void {
        grpc::ClientContext ctx;
        remote::StoreInfoRequest const request;
        remote::StoreInfoReply reply;
        checkStatus(stub->StoreInfo(&ctx, request, &reply), "StoreInfo");
        if (reply.has_trusted()) {
          trusted = reply.trusted() ? Trusted : NotTrusted;
        }
      });
      return trusted;
    }

    // nix copy "builds" opaque paths; answering already-valid ones here
    // avoids the tunnel, which read-only clients may not open.
    auto alreadyValidResults(const std::vector<DerivedPath> & reqs)
        -> std::optional<std::vector<KeyedBuildResult>> {
      StorePathSet paths;
      for (const auto & req : reqs) {
        const auto * opaque = std::get_if<DerivedPath::Opaque>(&req.raw());
        if (opaque == nullptr) {
          return std::nullopt;
        }
        paths.insert(opaque->path);
      }
      if (queryValidPaths(paths, NoSubstitute).size() != paths.size()) {
        return std::nullopt;
      }
      std::vector<KeyedBuildResult> results;
      results.reserve(reqs.size());
      for (const auto & req : reqs) {
        KeyedBuildResult res{{}, req};
        nixcompat::setAlreadyValid(res);
        results.push_back(std::move(res));
      }
      return results;
    }

#if NIX_COMPAT_HAS_BUILDER
    auto getBuilder(std::shared_ptr<Store> evalStore) -> ref<Builder> override {
      class GrpcBuilder : public Builder {
        GrpcStore * store;
        std::shared_ptr<Store> evalStore;
        ref<Builder> inner;

      public:
        GrpcBuilder(GrpcStore * store, std::shared_ptr<Store> evalStore,
                    ref<Builder> inner)
            : store(store), evalStore(std::move(evalStore)),
              inner(std::move(inner)) {}
        void buildPaths(const std::vector<DerivedPath> & reqs,
                        BuildMode buildMode) override {
          if (buildMode == bmNormal && store->alreadyValidResults(reqs)) {
            return;
          }
          store->copyDrvsFromEvalStore(reqs, evalStore);
          if (auto results = store->buildPathsWithResultsNative(reqs, buildMode)) {
            store->throwOnFailedBuilds(*results);
            return;
          }
          inner->buildPaths(reqs, buildMode);
        }
        auto buildPathsWithResults(const std::vector<DerivedPath> & reqs,
                                   BuildMode buildMode)
            -> std::vector<KeyedBuildResult> override {
          if (buildMode == bmNormal) {
            if (auto results = store->alreadyValidResults(reqs)) {
              return std::move(*results);
            }
          }
          store->copyDrvsFromEvalStore(reqs, evalStore);
          if (auto results = store->buildPathsWithResultsNative(reqs, buildMode)) {
            return std::move(*results);
          }
          return inner->buildPathsWithResults(reqs, buildMode);
        }
        auto buildDerivation(const StorePath & drvPath,
                             const BasicDerivation & drv, BuildMode buildMode)
            -> BuildResult override {
          return store->buildDerivationNative(drvPath, drv, buildMode);
        }
        void ensurePath(const StorePath & path) override {
          inner->ensurePath(path);
        }
        void repairPath(const StorePath & path) override {
          inner->repairPath(path);
        }
      };
      return make_ref<GrpcBuilder>(this, evalStore,
                                   RemoteStore::getBuilder(evalStore));
    }
#else
    void buildPaths(const std::vector<DerivedPath> & reqs, BuildMode buildMode,
                    std::shared_ptr<Store> evalStore) override {
      if (buildMode == bmNormal && alreadyValidResults(reqs)) {
        return;
      }
      copyDrvsFromEvalStore(reqs, evalStore);
      if (auto results = buildPathsWithResultsNative(reqs, buildMode)) {
        throwOnFailedBuilds(*results);
        return;
      }
      RemoteStore::buildPaths(reqs, buildMode, std::move(evalStore));
    }
    auto buildPathsWithResults(const std::vector<DerivedPath> & reqs,
                               BuildMode buildMode,
                               std::shared_ptr<Store> evalStore)
        -> std::vector<KeyedBuildResult> override {
      if (buildMode == bmNormal) {
        if (auto results = alreadyValidResults(reqs)) {
          return std::move(*results);
        }
      }
      copyDrvsFromEvalStore(reqs, evalStore);
      if (auto results = buildPathsWithResultsNative(reqs, buildMode)) {
        return std::move(*results);
      }
      return RemoteStore::buildPathsWithResults(reqs, buildMode,
                                                std::move(evalStore));
    }
    auto buildDerivation(const StorePath & drvPath, const BasicDerivation & drv,
                         BuildMode buildMode) -> BuildResult override {
      return buildDerivationNative(drvPath, drv, buildMode);
    }
#endif

    // One RPC per build: log lines stream during the build, the result
    // arrives with the output path infos so no follow-up queries are needed.
    auto buildDerivationNative(const StorePath & drvPath,
                               const BasicDerivation & drv,
                               BuildMode buildMode) -> BuildResult {
      remote::BuildDerivationRequest request;
      request.set_drv_path(std::string(drvPath.to_string()));
      request.set_build_mode(static_cast<uint32_t>(buildMode));
      request.set_protocol(nixcompat::kBuildProtocolWire);
      {
        StringSink sink;
        nixcompat::writeDrv(sink, *this, drv);
        *request.mutable_drv() = std::move(sink.s);
      }

      grpc::ClientContext ctx;
      auto reader = stub->BuildDerivation(&ctx, request);

      std::optional<BuildResult> res;
      PathInfoMap infos;
      remote::BuildDerivationChunk msg;
      while (reader->Read(&msg)) {
        if (msg.has_log_line()) {
          printError(msg.log_line());
        } else if (msg.has_done()) {
          StringSource source(msg.done().result());
          res = WorkerProto::Serialise<BuildResult>::read(
              *this, WorkerProto::ReadConn{.from = source,
                                           .version = nixcompat::buildProtocolVersion()});
          for (const auto & entry : msg.done().outputs()) {
            infos.insert(parseInfoEntry(entry));
          }
        }
      }
      checkStatus(reader->Finish(), "BuildDerivation");
      if (!res) {
        throw Error("gRPC BuildDerivation stream ended without a result");
      }
      if (!infos.empty()) {
        std::scoped_lock const lock(prefetchMutex);
        prefetchedInfos.merge(infos);
      }
      return std::move(*res);
    }

    // Builds run server-side under the proxy user, so the write role
    // suffices where the raw worker-protocol tunnel would not. Returns
    // nullopt for servers without the RPC.
    [[nodiscard]] auto buildPathsWithResultsNative(
        const std::vector<DerivedPath> & reqs, BuildMode buildMode)
        -> std::optional<std::vector<KeyedBuildResult>> {
      remote::BuildPathsRequest request;
      for (const auto & req : reqs) {
        request.add_targets(req.to_string(*this));
      }
      request.set_build_mode(static_cast<uint32_t>(buildMode));
      request.set_protocol(nixcompat::kBuildProtocolWire);

      grpc::ClientContext ctx;
      auto reader = stub->BuildPaths(&ctx, request);

      std::optional<std::vector<KeyedBuildResult>> results;
      PathInfoMap infos;
      remote::BuildPathsChunk msg;
      while (reader->Read(&msg)) {
        if (msg.has_log_line()) {
          printError(msg.log_line());
        } else if (msg.has_done()) {
          StringSource source(msg.done().results());
          results = WorkerProto::Serialise<std::vector<KeyedBuildResult>>::read(
              *this, WorkerProto::ReadConn{.from = source,
                                           .version = nixcompat::buildProtocolVersion()});
          for (const auto & entry : msg.done().outputs()) {
            infos.insert(parseInfoEntry(entry));
          }
        }
      }
      auto status = reader->Finish();
      if (status.error_code() == grpc::StatusCode::UNIMPLEMENTED) {
        return std::nullopt;
      }
      checkStatus(status, "BuildPaths");
      if (!results) {
        throw Error("gRPC BuildPaths stream ended without a result");
      }
      if (!infos.empty()) {
        std::scoped_lock const lock(prefetchMutex);
        prefetchedInfos.merge(infos);
      }
      return results;
    }

    // The server cannot reach the client's eval store, so the .drvs are
    // imported first (content-addressed, passes signature checks).
    void copyDrvsFromEvalStore(const std::vector<DerivedPath> & paths,
                               const std::shared_ptr<Store> & evalStore) {
      if (evalStore && evalStore.get() != this) {
        StorePathSet drvPaths;
        for (const auto & req : paths) {
          if (const auto * built = std::get_if<DerivedPath::Built>(&req.raw())) {
            drvPaths.insert(built->drvPath->getBaseStorePath());
          }
        }
        if (!drvPaths.empty()) {
          copyClosure(*evalStore, *this, drvPaths);
        }
      }
    }

    void throwOnFailedBuilds(std::vector<KeyedBuildResult> & results) {
      for (auto & res : results) {
        if (auto errorMsg = nixcompat::buildFailureMsg(res)) {
          throw Error("build of '%s' failed: %s", res.path.to_string(*this),
                      *errorMsg);
        }
      }
    }

private:
    using PathInfoMap = std::map<StorePath, std::shared_ptr<const ValidPathInfo>>;

    auto parseInfoEntry(const remote::PathInfo & entry)
        -> std::pair<StorePath, std::shared_ptr<const ValidPathInfo>> {
      StorePath const path(entry.path());
      StringSource source(entry.info());
      auto info = WorkerProto::Serialise<UnkeyedValidPathInfo>::read(
          *this, WorkerProto::ReadConn{.from = source,
                                       .version = nixcompat::infoProtocolVersion()});
      return {path, std::make_shared<ValidPathInfo>(StorePath(path),
                                                    std::move(info))};
    }

    std::once_flag trustedOnce;
    std::optional<TrustedFlag> trusted;

    /* Path infos fetched in bulk by topoSortPaths(), consumed by
       queryPathInfoUncached() so `nix copy` needs one QueryPathInfos RPC
       instead of one round trip per path. */
    std::mutex prefetchMutex;
    PathInfoMap prefetchedInfos;

    auto queryPathInfosNative(const StorePathSet &paths) -> PathInfoMap {
      grpc::ClientContext ctx;
      remote::QueryPathInfosRequest request;
      for (const auto &path : paths) {
        request.add_paths(std::string(path.to_string()));
      }
      remote::QueryPathInfosReply reply;
      checkStatus(stub->QueryPathInfos(&ctx, request, &reply),
                  "QueryPathInfos");

      PathInfoMap res;
      for (const auto &entry : reply.infos()) {
        res.insert(parseInfoEntry(entry));
      }
      return res;
    }

public:
// topoSortPaths() only became virtual in Nix 2.35; without the hook, `nix
// copy` falls back to one QueryPathInfos RPC per path.
#if NIX_COMPAT_AT_LEAST(2, 35)
    auto topoSortPaths(const StorePathSet &paths) -> StorePaths override {
      // copyPaths() hands the source store the full set of paths to copy
      // here, right before querying their info one by one; prefetch them in
      // a single RPC.
      std::map<StorePath, uint64_t> sizes;
      if (!paths.empty()) {
        auto infos = queryPathInfosNative(paths);
        for (const auto & [infoPath, info] : infos) {
          sizes.emplace(infoPath, info->narSize);
        }
        std::scoped_lock const lock(prefetchMutex);
        prefetchedInfos.merge(infos);
      }
      // copyPaths() consumes the sources in reverse topological order.
      // Record it so narFromPath() can pipeline its NAR requests.
      auto sorted = Store::topoSortPaths(paths);
      narFetcher.recordOrder({sorted.rbegin(), sorted.rend()}, std::move(sizes));
      return sorted;
    }
#endif

private:
    /* Batches concurrent queryPathInfoUncached() calls into one
       QueryPathInfos RPC. Closure computation awaits many paths at once.
       Queries piling up during an in-flight RPC form the next batch, so a
       BFS level costs one round trip instead of one per path. */
    using InfoCallback = Callback<std::shared_ptr<const ValidPathInfo>>;
    std::mutex infoBatchMutex;
    std::condition_variable infoBatchWakeup;
    std::vector<std::pair<StorePath, InfoCallback>> infoBatch;
    std::thread infoBatchWorker;
    bool infoBatchStop = false;

    void runInfoBatches()
    {
        while (true) {
            std::vector<std::pair<StorePath, InfoCallback>> batch;
            {
                std::unique_lock lock(infoBatchMutex);
                infoBatchWakeup.wait(lock, [&] -> bool { return infoBatchStop || !infoBatch.empty(); });
                if (infoBatchStop) {
                    return;
                }
                batch.swap(infoBatch);
            }
            try {
                StorePathSet paths;
                for (const auto & [path, callback] : batch) {
                    paths.insert(path);
                }
                auto infos = queryPathInfosNative(paths);
                for (auto & [path, callback] : batch) {
                    auto found = infos.find(path);
                    callback(found == infos.end() ? nullptr : found->second);
                }
            } catch (...) {
                for (auto & [path, callback] : batch) {
                    callback.rethrow();
                }
            }
        }
    }

public:
    void queryPathInfoUncached(const StorePath & path, InfoCallback callback) noexcept override
    {
        try {
            {
              std::scoped_lock const lock(prefetchMutex);
              if (auto found = prefetchedInfos.find(path);
                  found != prefetchedInfos.end()) {
                auto info = std::move(found->second);
                prefetchedInfos.erase(found);
                callback(std::move(info));
                return;
              }
            }

            {
              std::scoped_lock const lock(infoBatchMutex);
              infoBatch.emplace_back(path, std::move(callback));
              if (!infoBatchWorker.joinable()) {
                infoBatchWorker = std::thread([this] -> void { runInfoBatches(); });
              }
            }
            infoBatchWakeup.notify_one();
        } catch (...) {
            callback.rethrow();
        }
    }

private:
    struct Connection : RemoteStore::Connection
    {
    private:
        friend struct GrpcStore;

        // RemoteStore::Connection speaks through FdSink/FdSource, so bridge the
        // gRPC stream to a pair of pipes with pump threads. This keeps the
        // blocking, ordered semantics the worker protocol relies on without
        // reimplementing Source/Sink on top of gRPC.
        grpc::ClientContext ctx;
        std::unique_ptr<GrpcStream> stream;

        Pipe toRemote;   // plugin writes → reader thread sends over gRPC
        Pipe fromRemote; // writer thread receives from gRPC → plugin reads

        std::thread reader;
        std::thread writer;
        bool finished = false;

    public:
        Connection() = default;
        Connection(const Connection &) = delete;
        Connection(Connection &&) = delete;
        auto operator=(const Connection &) -> Connection & = delete;
        auto operator=(Connection &&) -> Connection & = delete;

        void closeWrite() override
        {
            // Closing the write side of the pipe makes the reader thread hit
            // EOF, which then calls WritesDone() on the stream.
            toRemote.writeSide.close();
        }

        ~Connection() override {
          // Unblock the reader pump (poll() on toRemote.readSide sees EOF)
          // and the writer pump (stream->Read() returns false), then join.
          // The pipe ends drained by the pump threads are owned by those
          // threads; touching them here would race with their own close().
          toRemote.writeSide.close();
          ctx.TryCancel();
          if (reader.joinable()) {
            reader.join();
          }
          if (writer.joinable()) {
            writer.join();
          }
          if (stream && !finished) {
            (void)stream->Finish();
          }
        }
    };

public:
    // NOLINTNEXTLINE(misc-override-with-different-visibility): Store and RemoteStore already disagree
    void narFromPath(const StorePath & path, Sink & sink) override
    {
      narFetcher.fetchInto(path, sink);
    }

    ~GrpcStore() override {
      {
        std::scoped_lock const lock(infoBatchMutex);
        infoBatchStop = true;
      }
      infoBatchWakeup.notify_one();
      if (infoBatchWorker.joinable()) {
        infoBatchWorker.join();
      }
    }

    void addMultipleToStore(
        PathsSource && pathsToCopy_, Activity & act, RepairFlag repair, CheckSigsFlag checkSigs) override
    {
        auto pathsToCopy = std::move(pathsToCopy_);
        uint64_t bytesExpected = 0;
        for (auto &[pathInfo, pathSource] : pathsToCopy) {
          bytesExpected += pathInfo.narSize;
        }
        act.setExpected(actCopyPath, bytesExpected);

        grpc::ClientContext ctx;
        remote::AddMultipleReply reply;
        auto writer = stub->AddMultipleToStore(&ctx, &reply);

        try {
            // Flags travel on the first message; everything after is one zstd
            // stream in worker-protocol AddMultipleToStore framing.
            remote::AddMultipleChunk flags;
            flags.set_repair(repair == Repair);
            flags.set_check_sigs(checkSigs == CheckSigs);
            if (!writer->Write(flags)) {
              throw Error("gRPC stream closed by peer");
            }

            nixgrpc::ZstdWriterSink<grpc::ClientWriter<remote::AddMultipleChunk>, remote::AddMultipleChunk> sink(
                *writer);
            size_t const nrTotal = pathsToCopy.size();
            sink << nrTotal;
            // Reverse, so we can release memory at the original start.
            std::ranges::reverse(pathsToCopy);
            while (!pathsToCopy.empty()) {
                act.progress(
                    nrTotal - pathsToCopy.size(), nrTotal, static_cast<size_t>(1), static_cast<size_t>(0));
                auto & [pathInfo, pathSource] = pathsToCopy.back();
                WorkerProto::Serialise<ValidPathInfo>::write(
                    *this, WorkerProto::WriteConn{.to = sink, .version = nixcompat::infoProtocolVersion()}, pathInfo);
                pathSource->drainInto(sink);
                pathsToCopy.pop_back();
            }
            sink.finish();
            writer->WritesDone();
        } catch (...) {
            // The server-side status usually explains a broken stream better
            // than the local write failure. Finish() half-closes, so the
            // server stops waiting for the remaining paths.
            checkStatus(writer->Finish(), "AddMultipleToStore");
            throw;
        }

        checkStatus(writer->Finish(), "AddMultipleToStore");
    }

    // NOLINTNEXTLINE(misc-override-with-different-visibility): see narFromPath
    void setOptions(RemoteStore::Connection & /*conn*/) override {
      // As with SSHStore, do not forward local settings automatically.
    }

    // NOLINTNEXTLINE(misc-override-with-different-visibility): see narFromPath
    auto openConnection() -> ref<RemoteStore::Connection> override;
};

auto GrpcStore::openConnection() -> ref<RemoteStore::Connection> {
  auto conn = make_ref<Connection>();

  conn->stream = stub->Connect(&conn->ctx);
  if (!conn->stream) {
    throw Error("failed to open gRPC stream to '%s'",
                config->authority.to_string());
  }

  conn->toRemote.create();
  conn->fromRemote.create();
  nixgrpc::growPipe(conn->toRemote);
  nixgrpc::growPipe(conn->fromRemote);

  conn->reader = std::thread([connPtr = &*conn] -> void {
    try {
      nixgrpc::pumpFdToStream(connPtr->toRemote.readSide.get(), *connPtr->stream);
    } catch (...) {
      ignoreExceptionInDestructor();
    }
    connPtr->stream->WritesDone();
    // If the stream broke, RemoteStore may be blocked in FdSink writing to
    // a full pipe with no drainer. Closing the read side turns that into
    // EPIPE so the error surfaces instead of hanging.
    connPtr->toRemote.readSide.close();
  });

  conn->writer = std::thread([this, connPtr = &*conn] -> void {
    try {
      nixgrpc::pumpStreamToFd(*connPtr->stream, connPtr->fromRemote.writeSide.get());
    } catch (...) {
      ignoreExceptionInDestructor();
    }
    // RemoteStore only sees EOF on the pipe, so surface the real status here.
    auto status = connPtr->stream->Finish();
    connPtr->finished = true;
    if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED) {
      logError(statusError(status, "Connect").info());
    }
    // Propagate EOF / error to the worker-protocol reader.
    connPtr->fromRemote.writeSide.close();
  });

  conn->to = FdSink(conn->toRemote.writeSide.get());
  conn->from = FdSource(conn->fromRemote.readSide.get());
  return conn;
}

auto GrpcStoreConfig::openStore() const -> ref<Store> {
  return make_ref<GrpcStore>(ref{shared_from_this()});
}

} // namespace nix

namespace {

// "2.31.5" / "2.35pre20260619_f8bb823a" -> "2.31" / "2.35"
auto majorMinor(std::string_view version) -> std::string_view {
  auto firstDot = version.find('.');
  auto end = version.find_first_not_of("0123456789", firstDot + 1);
  return version.substr(0, end);
}

} // namespace

// Called by Nix right after dlopen(). Registering here instead of in a static
// initializer lets a version mismatch degrade to a warning.
extern "C" void nix_plugin_entry() {
  auto running = majorMinor(nix::nixVersion);
  auto builtAgainst = majorMinor(NIX_GRPC_BUILT_AGAINST_NIX);
  if (running != builtAgainst) {
    nix::warn(
        "nix-grpc-store plugin was built against Nix %s but is being loaded by Nix %s. "
        "grpc:// stores are unavailable",
        std::string(builtAgainst), std::string(running));
    return;
  }
  static const nix::RegisterStoreImplementation<nix::GrpcStoreConfig> regGrpcStore;
}
