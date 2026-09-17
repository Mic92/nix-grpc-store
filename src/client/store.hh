#pragma once
// The grpc:// Store: class layout. Definitions live in store.cc (setup,
// errors, tunnel), build.cc (BuildDerivation, farm fan-out) and transfer.cc
// (path info, NAR upload).

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <grpc/event_engine/event_engine.h>
#include <grpc/grpc.h>
#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/security/auth_context.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/config.h>
#include <grpcpp/support/string_ref.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/interceptor.h>
#include <grpcpp/support/status.h>
#include <atomic>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cassert>
#include <deque>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <nix/store/globals.hh>
#include <nix/store/derived-path.hh>
#include <nix/store/path.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-open.hh>
#include <nix/store/store-reference.hh>
#include <nix/util/configuration.hh>
#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>
#include <nix/util/file-system.hh>
#include <nix/util/fmt.hh>
#include <nix/util/logging.hh>
#include <nix/util/ref.hh>
#include <nix/util/repair-flag.hh>
#include <nix/util/serialise.hh>
#include <nix/util/strings.hh>
#include <nix/util/types.hh>
#include <nix/util/signals.hh>
#include <nix/util/util.hh>
#include <openssl/bio.h>
#include <openssl/crypto.h>
#include <openssl/pem.h>
#include <openssl/types.h>
#include <openssl/x509.h>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <stop_token>
#include <thread>
#include <unistd.h>
#include <variant>
#include <vector>

#include <grpcpp/grpcpp.h>

#include <nix/store/path-info.hh>
#include <nix/store/build-result.hh>
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

#include "channel.hh"
#include "nix-compat.hh"
#include "nar-fetcher.hh"
#include "path-info-wire.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"

using GrpcStream = grpc::ClientReaderWriter<nix::remote::Chunk, nix::remote::Chunk>;

namespace nix {

// Follows Nix's own StoreConfig pattern (e.g. DummyStoreConfig).
// NOLINTNEXTLINE(misc-multiple-inheritance,misc-use-internal-linkage)
struct GrpcStoreConfig : std::enable_shared_from_this<GrpcStoreConfig>, virtual RemoteStoreConfig
{
private:
    friend struct GrpcStore;

    ParsedURL::Authority authority;

    Setting<bool> insecure{this, false, "insecure", "Use plaintext instead of TLS. Only for local testing."};

    static constexpr unsigned defaultConnectTimeout = 30;
    Setting<unsigned int> connectTimeout{this, defaultConnectTimeout, "connect-timeout",
        "Seconds the first call waits for the server to become reachable before failing."};

    Setting<std::string> routeSystem{this, "", "system",
        "Send `x-nix-system: VALUE` on every call, not only on builds. Use one "
        "`nix.buildMachines` entry per system behind a balancer so input uploads "
        "and substitution land on a worker of that system."};

    Setting<bool> debug{this, nix::getEnv("NIX_GRPC_DEBUG").value_or("") == "1", "debug",
        "Log TLS setup and enable gRPC handshake tracing on stderr. Also `NIX_GRPC_DEBUG=1`."};

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

    static constexpr unsigned defaultMaxBuilds = 64;

    Setting<unsigned> maxBuilds{
        this,
        defaultMaxBuilds,
        "max-builds",
        "Number of concurrent BuildDerivation calls when talking to a build farm."};

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

    auto sslOptions() -> grpc::SslCredentialsOptions;

    auto makeChannel(bool ownConnection) -> std::shared_ptr<grpc::Channel>
;

public:
    explicit GrpcStore(const ref<const Config> & config);

    GrpcStore(const GrpcStore &) = delete;
    GrpcStore(GrpcStore &&) = delete;
    auto operator=(const GrpcStore &) -> GrpcStore & = delete;
    auto operator=(GrpcStore &&) -> GrpcStore & = delete;

    auto getBuildLogExact(const StorePath & /*path*/)
        -> std::optional<std::string> override {
      unsupported("getBuildLogExact");
    }

private:
    // gRPC folds every connect failure into UNAVAILABLE. Tell TCP from TLS apart by the message.
    // gRPC folds TCP and TLS failures into UNAVAILABLE, only the text differs.
    static auto transportError(std::string_view msg) -> bool;

    auto connectHint(const std::string & msg) const -> std::string
;

    auto statusError(const grpc::Status & status, const char * opName) const -> Error
;

    void checkStatus(const grpc::Status & status, const char * opName) const
    {
      if (!status.ok()) {
        throw statusError(status, opName);
      }
    }

    using Metadata = std::vector<std::pair<std::string, std::string>>;

    auto queryValidPathsRouted(const StorePathSet & paths, SubstituteFlag maybeSubstitute,
                               const Metadata & headers) -> StorePathSet;

public:
    auto queryValidPaths(const StorePathSet & paths, SubstituteFlag maybeSubstitute)
        -> StorePathSet override {
      return queryValidPathsRouted(paths, maybeSubstitute, {});
    }

    // RemoteStore would tunnel this. One RPC, with a fallback to the
    // client-side walk for servers without QueryMissing.
    auto queryMissing(const std::vector<DerivedPath> & targets)
        -> MissingPaths override;

    // Same: keep read-only clients off the tunnel.
    // RemoteStore would tunnel this even with an eval store at hand.
    auto queryPartialDerivationOutputMap(const StorePath & path, Store * evalStore)
        -> std::map<std::string, std::optional<StorePath>> override {
      if (evalStore != nullptr && evalStore != this) {
        return evalStore->queryStaticPartialDerivationOutputMap(path);
      }
      return RemoteStore::queryPartialDerivationOutputMap(path, nullptr);
    }

    auto isValidPathUncached(const StorePath & path) -> bool override {
      return queryValidPaths({path}, NoSubstitute).contains(path);
    }

    // The build hook probes reachability at store open. One StoreInfo RPC
    // answers it and caches the trust flag, no tunnel needed. TCP-level
    // failures are retried for connect-timeout so a balancer or worker
    // restart does not fail the build; TLS rejections fail at once.
    void connect() override { isTrustedClient(); }

    // A worker or the balancer restarting under us. TLS/auth failures are
    // also UNAVAILABLE but retrying those only delays the error.
    static auto goneAway(const grpc::Status & status) -> bool;

    // connect-timeout until the service first answers, restartGrace after: a
    // worker restart behind the balancer is expected, being offline is not.
    static constexpr std::chrono::seconds restartGrace{120};
    template<typename F>
    void retrying(const char * what, const F & attempt) {
      auto giveUp = std::chrono::steady_clock::now()
                    + (everConnected ? restartGrace : std::chrono::seconds(config->connectTimeout.get()));
      auto pause = std::chrono::milliseconds(500); // NOLINT(*-magic-numbers)
      for (;;) {
        auto status = attempt();
        if (status.ok()) {
          everConnected = true;
        }
        if (!goneAway(status) || std::chrono::steady_clock::now() + pause > giveUp) {
          checkStatus(status, what);
          return;
        }
        checkInterrupt();
        printError("%s: %s, retrying", config->authority.to_string(), firstLine(status.error_message()));
        std::this_thread::sleep_for(pause);
        pause = std::min(pause * 2, std::chrono::milliseconds(4000)); // NOLINT(*-magic-numbers)
      }
    }

    auto isTrustedClient() -> std::optional<TrustedFlag> override;

    // nix copy "builds" opaque paths; answering already-valid ones here
    // avoids the tunnel, which read-only clients may not open.
    auto alreadyValidResults(const std::vector<DerivedPath> & reqs)
        -> std::optional<std::vector<KeyedBuildResult>>;

    // Farm fan-out, else already-valid short cut, else native BuildPaths.
    // nullopt: server predates the RPC, use the worker-protocol tunnel.
    auto dispatchBuild(const std::vector<DerivedPath> & reqs, BuildMode buildMode,
                       const std::shared_ptr<Store> & evalStore)
        -> std::optional<std::vector<KeyedBuildResult>>;

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
          if (auto results = store->dispatchBuild(reqs, buildMode, evalStore)) {
            store->throwOnFailedBuilds(*results);
            return;
          }
          inner->buildPaths(reqs, buildMode);
        }
        auto buildPathsWithResults(const std::vector<DerivedPath> & reqs,
                                   BuildMode buildMode)
            -> std::vector<KeyedBuildResult> override {
          if (auto results = store->dispatchBuild(reqs, buildMode, evalStore)) {
            return std::move(*results);
          }
          return inner->buildPathsWithResults(reqs, buildMode);
        }
        auto buildDerivation(const StorePath & drvPath,
                             const BasicDerivation & drv, BuildMode buildMode)
            -> BuildResult override {
          return store->buildDerivationNative(drvPath, drv, buildMode, evalStore.get());
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
      if (auto results = dispatchBuild(reqs, buildMode, evalStore)) {
        throwOnFailedBuilds(*results);
        return;
      }
      RemoteStore::buildPaths(reqs, buildMode, std::move(evalStore));
    }
    auto buildPathsWithResults(const std::vector<DerivedPath> & reqs,
                               BuildMode buildMode,
                               std::shared_ptr<Store> evalStore)
        -> std::vector<KeyedBuildResult> override {
      if (auto results = dispatchBuild(reqs, buildMode, evalStore)) {
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

    static constexpr unsigned unavailableRetries = 5;

    static auto firstLine(const std::string & msg) -> std::string {
      return msg.substr(0, msg.find('\n'));
    }

    // An actBuild activity opened on the first streamed log line or phase.
    class BuildLogActivity {
      std::string text;
      std::vector<Logger::Field> fields;
      std::optional<Activity> act;

    public:
      BuildLogActivity(std::string text, std::vector<Logger::Field> fields)
          : text(std::move(text)), fields(std::move(fields)) {}

      template <typename Chunk> auto relay(const Chunk & msg) -> bool {
        if (!msg.has_log_line() && !msg.has_phase()) {
          return false;
        }
        if (!act) {
          act.emplace(*logger, lvlInfo, actBuild, text, fields);
        }
        if (msg.has_log_line()) {
          act->result(resBuildLogLine, msg.log_line());
        } else {
          act->result(resSetPhase, msg.phase());
        }
        return true;
      }
    };

    // What the load balancer routes on.
    auto routingFor(const StorePath & drvPath, const BasicDerivation & drv) -> Metadata;

    static void addHeaders(grpc::ClientContext & ctx, const Metadata & headers) {
      for (const auto & [key, value] : headers) {
        ctx.AddMetadata(key, value);
      }
    }

    // Same headers on every RPC so the balancer keeps us on that worker.
    void uploadDrvClosure(Store & evalStore, const StorePath & drvPath, const Metadata & headers);

    // Log lines stream, the result carries the output path infos.
    auto tryBuildDerivation(const remote::BuildDerivationRequest & request, const Metadata & headers,
                            std::optional<BuildResult> & res) -> grpc::Status;

    // NOT_FOUND: the worker lacks the drv closure, upload it and ask again.
    auto buildDerivationNative(const StorePath & drvPath,
                               const BasicDerivation & drv,
                               BuildMode buildMode,
                               Store * evalStore = nullptr) -> BuildResult;

    // A farm refuses BuildPaths. An empty request is the cheapest probe.
    auto isFarm() -> bool;

    struct FarmJob {
      StorePath drvPath;
      BasicDerivation drv;
      std::vector<FarmJob *> dependants;
      size_t waiting = 0;
      std::optional<StorePath> failedInput;
      std::optional<BuildResult> result;
    };

    struct FarmRun {
      std::mutex mutex;
      std::condition_variable cv;
      std::deque<FarmJob *> ready;
      size_t remaining;

      auto next() -> FarmJob * {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&]() -> bool { return !ready.empty() || remaining == 0; });
        if (ready.empty()) {
          return nullptr;
        }
        auto * job = ready.front();
        ready.pop_front();
        return job;
      }

      void finish(FarmJob & job, BuildResult res) {
        std::scoped_lock const lock(mutex);
        bool const succeeded = nixcompat::succeeded(res);
        job.result = std::move(res);
        assert(remaining > 0);
        remaining--;
        for (auto * dep : job.dependants) {
          if (!succeeded) {
            dep->failedInput = job.drvPath;
          }
          assert(dep->waiting > 0);
          if (--dep->waiting == 0) {
            ready.push_back(dep);
          }
        }
        cv.notify_all();
      }
    };

    // Runs on a fan-out thread, nothing may escape.
    auto runFarmJob(FarmJob & job, BuildMode buildMode, Store & evalStore) -> BuildResult;

    // The build hook (untrusted role) and plain `--store grpc://` pass no
    // eval store but usually have the derivations in the local store.
    auto localEvalStore(const std::vector<DerivedPath> & reqs) -> std::shared_ptr<Store>;

    // The farm builds one derivation per RPC, so walk the DAG here and send
    // every ready derivation at once. Cached ones come back AlreadyValid.
    auto farmBuildPaths(const std::vector<DerivedPath> & reqs, BuildMode buildMode,
                        Store * evalStore) -> std::vector<KeyedBuildResult>;

    auto basicForFarm(Store & evalStore, const StorePath & drvPath, const Derivation & full)
        -> BasicDerivation;

    void loadFarmJobs(const std::vector<DerivedPath> & reqs, Store & evalStore,
                      std::map<StorePath, FarmJob> & jobs);

    // Builds run server-side under the proxy user, so the write role
    // suffices where the raw worker-protocol tunnel would not. Returns
    // nullopt for servers without the RPC.
    [[nodiscard]] auto buildPathsWithResultsNative(
        const std::vector<DerivedPath> & reqs, BuildMode buildMode)
        -> std::optional<std::vector<KeyedBuildResult>>;

    // The server cannot reach the client's eval store, so the .drvs are
    // imported first (content-addressed, passes signature checks).
    void importDrvsFromEvalStore(const std::vector<DerivedPath> & paths,
                               const std::shared_ptr<Store> & evalStore);

    void throwOnFailedBuilds(std::vector<KeyedBuildResult> & results);

private:
    using PathInfoMap = std::map<StorePath, std::shared_ptr<const ValidPathInfo>>;

    std::atomic<bool> everConnected = false;
    std::once_flag trustedOnce;
    std::optional<TrustedFlag> trusted;
    std::once_flag farmOnce;
    bool farm = false;

    /* Path infos fetched in bulk by topoSortPaths(), consumed by
       queryPathInfoUncached() so `nix copy` needs one QueryPathInfos RPC
       instead of one round trip per path. */
    std::mutex prefetchMutex;
    PathInfoMap prefetchedInfos;

    auto queryPathInfosNative(const StorePathSet &paths) -> PathInfoMap;

public:
// topoSortPaths() only became virtual in Nix 2.35; without the hook, `nix
// copy` falls back to one QueryPathInfos RPC per path.
#if NIX_COMPAT_AT_LEAST(2, 35)
    auto topoSortPaths(const StorePathSet &paths) -> StorePaths override;
#endif

private:
    /* Batches concurrent queryPathInfoUncached() calls into one
       QueryPathInfos RPC. Closure computation awaits many paths at once.
       Queries piling up during an in-flight RPC form the next batch, so a
       BFS level costs one round trip instead of one per path. */
    using InfoCallback = Callback<std::shared_ptr<const ValidPathInfo>>;
    std::mutex infoBatchMutex;
    std::condition_variable_any infoBatchWakeup;
    std::vector<std::pair<StorePath, InfoCallback>> infoBatch;
    std::jthread infoBatchWorker;

    void runInfoBatches(const std::stop_token & stop)
;

public:
    void queryPathInfoUncached(const StorePath & path, InfoCallback callback) noexcept override
;

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

        std::jthread reader;
        std::jthread writer;
        std::atomic<bool> finished = false;

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
          reader = {};
          writer = {};
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

    // The worker uses members declared after it, so stop it before they go.
    ~GrpcStore() override
    {
        infoBatchWorker = {};
    }

    void addMultipleToStore(
        PathsSource && pathsToCopy, Activity & act, RepairFlag repair, CheckSigsFlag checkSigs) override
    {
        addMultipleToStoreRouted(std::move(pathsToCopy), act, repair, checkSigs, {});
    }

private:
    // Caller sources are single-use and may sit on a pooled daemon connection,
    // so read each to the end exactly once, teeing to a temp file for retries.
    struct ReplayableNar
    {
        std::unique_ptr<Source> upstream;
        AutoCloseFD fd;
        bool complete = false;

        explicit ReplayableNar(std::unique_ptr<Source> src)
            : upstream(std::move(src))
        {
            auto [tmpFd, tmpPath] = createTempFile("nix-grpc-upload");
            ::unlink(tmpPath.c_str());
            fd = std::move(tmpFd);
        }

        void drainInto(Sink & sink)
        {
            if (!complete) {
                std::exception_ptr sinkError;
                LambdaSink tee([&](std::string_view data) -> void {
                    writeFull(fd.get(), data);
                    if (sinkError) {
                      return;
                    }
                    try {
                      sink(data);
                    } catch (...) {
                      sinkError = std::current_exception();
                    }
                });
                upstream->drainInto(tee);
                complete = true;
                upstream.reset();
                if (sinkError) {
                  std::rethrow_exception(sinkError);
                }
                return;
            }
            if (::lseek(fd.get(), 0, SEEK_SET) != 0) {
              throw SysError("rewinding upload spool");
            }
            FdSource replay(fd.get());
            replay.drainInto(sink);
        }
    };

    void addMultipleToStoreRouted(
        PathsSource pathsToCopy, Activity & act, RepairFlag repair, CheckSigsFlag checkSigs, const Metadata & headers)
;

    auto addMultipleToStoreOnce(
        std::vector<std::pair<ValidPathInfo, ReplayableNar>> & paths, Activity & act, RepairFlag repair,
        CheckSigsFlag checkSigs, const Metadata & headers) -> grpc::Status
;

public:
    // NOLINTNEXTLINE(misc-override-with-different-visibility): see narFromPath
    void setOptions(RemoteStore::Connection & /*conn*/) override {
      // As with SSHStore, do not forward local settings automatically.
    }

    // NOLINTNEXTLINE(misc-override-with-different-visibility): see narFromPath
    auto openConnection() -> ref<RemoteStore::Connection> override;
};

} // namespace nix
