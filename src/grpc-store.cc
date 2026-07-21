// Nix store plugin that tunnels the worker protocol over a gRPC bidirectional
// stream. The gRPC layer is a dumb byte pipe; all store operations are handled
// by the existing RemoteStore implementation. The `nix copy` hot path
// (queryValidPaths, queryPathInfo, addMultipleToStore, narFromPath) uses
// dedicated RPCs when the server supports them.
//
// URI: grpc://host:port
// Params: insecure, ca-cert, client-cert, client-key

#include <map>
#include <mutex>
#include <thread>

#include <grpcpp/grpcpp.h>

#include <nix/store/path-info.hh>
#include <nix/store/remote-store.hh>
#include <nix/store/remote-store-connection.hh>
#include <nix/store/store-registration.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/archive.hh>
#include <nix/util/callback.hh>
#include <nix/util/file-descriptor.hh>
#include <nix/util/url.hh>

#include "pump.hh"

using GrpcStream = grpc::ClientReaderWriter<nix::remote::Chunk, nix::remote::Chunk>;

namespace nix {

struct GrpcStoreConfig : std::enable_shared_from_this<GrpcStoreConfig>, virtual RemoteStoreConfig
{
    GrpcStoreConfig(const Params & params)
        : StoreConfig(params, FilePathType::Unix)
        , RemoteStoreConfig(params, FilePathType::Unix)
    {
    }

    GrpcStoreConfig(const ParsedURL::Authority & authority, const Params & params)
        : StoreConfig(params, FilePathType::Unix)
        , RemoteStoreConfig(params, FilePathType::Unix)
        , authority(authority)
    {
    }

    ParsedURL::Authority authority;

    Setting<bool> insecure{
        this,
        false,
        "insecure",
        "Use plaintext instead of TLS. Only for local testing."};

    Setting<std::string> caCert{
        this,
        "",
        "ca-cert",
        "Path to a PEM file with the CA certificate used to verify the server."};

    Setting<std::string> clientCert{
        this,
        "",
        "client-cert",
        "Path to a PEM client certificate chain to present for mTLS."};

    Setting<std::string> clientKey{
        this,
        "",
        "client-key",
        "Path to the PEM private key for `client-cert`."};

    static const std::string name() { return "gRPC Store"; }

    static StringSet uriSchemes() { return {"grpc"}; }

    static std::string doc()
    {
        return "Connects to a `nix-grpc-daemon` and tunnels the Nix worker "
               "protocol over a gRPC bidirectional stream.";
    }

    StoreReference getReference() const override
    {
        return {
            .variant = StoreReference::Specified{
                .scheme = *uriSchemes().begin(),
                .authority = authority.to_string(),
            },
            .params = getQueryParams(),
        };
    }

    ref<Store> openStore() const override;
};

struct GrpcStore : virtual RemoteStore
{
    using Config = GrpcStoreConfig;

    ref<const Config> config;

    /* One channel is shared by all connections in the pool; gRPC multiplexes
       streams over it internally. The stub keeps the channel alive. */
    std::unique_ptr<remote::NixRemote::Stub> stub;

    GrpcStore(ref<const Config> config)
        : Store{*config}
        , RemoteStore{*config}
        , config{config}
    {
        std::shared_ptr<grpc::ChannelCredentials> creds;
        if (config->insecure) {
            creds = grpc::InsecureChannelCredentials();
        } else {
            grpc::SslCredentialsOptions ssl;
            if (!config->caCert.get().empty())
                ssl.pem_root_certs = readFile(config->caCert.get());
            if (!config->clientCert.get().empty()) {
                ssl.pem_cert_chain = readFile(config->clientCert.get());
                ssl.pem_private_key = readFile(config->clientKey.get());
            }
            creds = grpc::SslCredentials(ssl);
        }

        grpc::ChannelArguments args;
        // The worker protocol streams NARs; do not cap message size.
        args.SetMaxReceiveMessageSize(-1);
        args.SetMaxSendMessageSize(-1);

        stub = remote::NixRemote::NewStub(
            grpc::CreateCustomChannel(config->authority.to_string(), creds, args));
    }

    std::optional<std::string> getBuildLogExact(const StorePath &) override
    {
        unsupported("getBuildLogExact");
    }

    /* ValidPathInfo serialisation used by the bulk RPCs, matching the worker
       protocol's AddMultipleToStore framing. */
    static constexpr WorkerProto::Version::Number infoVersion{.major = 1, .minor = 16};

    /* Whether the server implements the native (non-tunnelled) RPCs. Probed
       once with an empty QueryValidPaths so bulk operations can decide
       between the native path and the tunnel before consuming any data. */
    std::optional<bool> nativeOps;
    std::mutex nativeOpsMutex;

    static void checkStatus(const grpc::Status & status, const char * op)
    {
        if (!status.ok())
            throw Error("gRPC %s failed: %s", op, status.error_message());
    }

    bool hasNativeOps()
    {
        std::lock_guard<std::mutex> lock(nativeOpsMutex);
        if (!nativeOps) {
            grpc::ClientContext ctx;
            remote::QueryValidPathsRequest request;
            remote::QueryValidPathsReply reply;
            auto status = stub->QueryValidPaths(&ctx, request, &reply);
            nativeOps = status.ok();
            if (!status.ok())
                debug("gRPC store '%s' lacks native ops, using tunnel: %s",
                    config->authority.to_string(), status.error_message());
        }
        return *nativeOps;
    }

    StorePathSet queryValidPaths(const StorePathSet & paths, SubstituteFlag maybeSubstitute) override
    {
        if (!hasNativeOps())
            return RemoteStore::queryValidPaths(paths, maybeSubstitute);

        grpc::ClientContext ctx;
        remote::QueryValidPathsRequest request;
        request.set_substitute(maybeSubstitute == Substitute);
        for (auto & p : paths)
            request.add_paths(std::string(p.to_string()));
        remote::QueryValidPathsReply reply;
        checkStatus(stub->QueryValidPaths(&ctx, request, &reply), "QueryValidPaths");
        StorePathSet res;
        for (auto & p : reply.paths())
            res.insert(StorePath(p));
        return res;
    }

    using PathInfoMap = std::map<StorePath, std::shared_ptr<const ValidPathInfo>>;

    /* Path infos fetched in bulk by topoSortPaths(), consumed by
       queryPathInfoUncached() so `nix copy` needs one QueryPathInfos RPC
       instead of one round trip per path. */
    std::mutex prefetchMutex;
    PathInfoMap prefetchedInfos;

    PathInfoMap queryPathInfosNative(const StorePathSet & paths)
    {
        grpc::ClientContext ctx;
        remote::QueryPathInfosRequest request;
        for (auto & p : paths)
            request.add_paths(std::string(p.to_string()));
        remote::QueryPathInfosReply reply;
        checkStatus(stub->QueryPathInfos(&ctx, request, &reply), "QueryPathInfos");

        PathInfoMap res;
        for (auto & pi : reply.infos()) {
            StorePath path(pi.path());
            StringSource source(pi.info());
            auto info = WorkerProto::Serialise<UnkeyedValidPathInfo>::read(
                *this, WorkerProto::ReadConn{.from = source, .version = {.number = infoVersion}});
            res.insert_or_assign(path, std::make_shared<ValidPathInfo>(StorePath(path), std::move(info)));
        }
        return res;
    }

    StorePaths topoSortPaths(const StorePathSet & paths) override
    {
        // copyPaths() hands the source store the full set of paths to copy
        // here, right before querying their info one by one; prefetch them in
        // a single RPC.
        if (hasNativeOps() && !paths.empty()) {
            auto infos = queryPathInfosNative(paths);
            std::lock_guard<std::mutex> lock(prefetchMutex);
            prefetchedInfos.merge(infos);
        }
        return Store::topoSortPaths(paths);
    }

    void queryPathInfoUncached(
        const StorePath & path, Callback<std::shared_ptr<const ValidPathInfo>> callback) noexcept override
    {
        try {
            if (!hasNativeOps())
                return RemoteStore::queryPathInfoUncached(path, std::move(callback));

            {
                std::lock_guard<std::mutex> lock(prefetchMutex);
                if (auto it = prefetchedInfos.find(path); it != prefetchedInfos.end()) {
                    auto info = std::move(it->second);
                    prefetchedInfos.erase(it);
                    return callback(std::move(info));
                }
            }

            auto infos = queryPathInfosNative({path});
            auto it = infos.find(path);
            callback(it == infos.end() ? nullptr : std::move(it->second));
        } catch (...) {
            callback.rethrow();
        }
    }

    /* One long-lived NarsFromPaths stream shared by all narFromPath() calls:
       the server compresses all NARs into a single zstd stream, so many small
       paths share the compression window and there is no per-path RPC setup.
       Serialised by narMutex; NARs are self-delimiting (copyNAR), so no
       length framing is needed. */
    using NarStream = grpc::ClientReaderWriter<remote::NarRequest, remote::NarChunk>;

    struct NarSession
    {
        grpc::ClientContext ctx;
        std::unique_ptr<NarStream> stream;
        std::optional<nixgrpc::ZstdReaderSource<NarStream, remote::NarChunk>> source;
    };

    std::mutex narMutex;
    std::unique_ptr<NarSession> narSession;

    void narFromPath(const StorePath & path, Sink & sink) override
    {
        if (!hasNativeOps())
            return RemoteStore::narFromPath(path, sink);

        std::lock_guard<std::mutex> lock(narMutex);

        if (!narSession) {
            auto session = std::make_unique<NarSession>();
            session->stream = stub->NarsFromPaths(&session->ctx);
            if (!session->stream)
                throw Error("failed to open gRPC NarsFromPaths stream to '%s'", config->authority.to_string());
            session->source.emplace(*session->stream, std::string{});
            narSession = std::move(session);
        }

        try {
            remote::NarRequest request;
            request.set_path(std::string(path.to_string()));
            if (!narSession->stream->Write(request))
                throw Error("gRPC stream closed by peer");
            copyNAR(*narSession->source, sink);
        } catch (...) {
            // The session is unusable after an error (the stream position is
            // unknown); surface the server-side status if there is one.
            auto session = std::move(narSession);
            session->ctx.TryCancel();
            auto status = session->stream->Finish();
            if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED)
                throw Error("gRPC NarsFromPaths failed: %s", status.error_message());
            throw;
        }
    }

    ~GrpcStore()
    {
        // Let the server end its NarsFromPaths handler cleanly.
        if (narSession) {
            narSession->stream->WritesDone();
            remote::NarChunk chunk;
            while (narSession->stream->Read(&chunk))
                ;
            (void) narSession->stream->Finish();
        }
    }

    void addMultipleToStore(
        PathsSource && pathsToCopy, Activity & act, RepairFlag repair, CheckSigsFlag checkSigs) override
    {
        if (!hasNativeOps())
            return RemoteStore::addMultipleToStore(std::move(pathsToCopy), act, repair, checkSigs);

        uint64_t bytesExpected = 0;
        for (auto & [pathInfo, _] : pathsToCopy)
            bytesExpected += pathInfo.narSize;
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
            if (!writer->Write(flags))
                throw Error("gRPC stream closed by peer");

            nixgrpc::ZstdWriterSink<grpc::ClientWriter<remote::AddMultipleChunk>, remote::AddMultipleChunk> sink(
                *writer);
            size_t nrTotal = pathsToCopy.size();
            sink << nrTotal;
            // Reverse, so we can release memory at the original start.
            std::reverse(pathsToCopy.begin(), pathsToCopy.end());
            while (!pathsToCopy.empty()) {
                act.progress(nrTotal - pathsToCopy.size(), nrTotal, size_t(1), size_t(0));
                auto & [pathInfo, pathSource] = pathsToCopy.back();
                WorkerProto::Serialise<ValidPathInfo>::write(
                    *this, WorkerProto::WriteConn{.to = sink, .version = {.number = infoVersion}}, pathInfo);
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

    void setOptions(RemoteStore::Connection &) override
    {
        // As with SSHStore, do not forward local settings automatically.
    }

    struct Connection : RemoteStore::Connection
    {
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

        void closeWrite() override
        {
            // Closing the write side of the pipe makes the reader thread hit
            // EOF, which then calls WritesDone() on the stream.
            toRemote.writeSide.close();
        }

        ~Connection()
        {
            // Unblock the reader pump (poll() on toRemote.readSide sees EOF)
            // and the writer pump (stream->Read() returns false), then join.
            // The pipe ends drained by the pump threads are owned by those
            // threads; touching them here would race with their own close().
            toRemote.writeSide.close();
            ctx.TryCancel();
            if (reader.joinable()) reader.join();
            if (writer.joinable()) writer.join();
            if (stream)
                (void) stream->Finish();
        }
    };

    ref<RemoteStore::Connection> openConnection() override;
};

ref<RemoteStore::Connection> GrpcStore::openConnection()
{
    auto conn = make_ref<Connection>();

    conn->stream = stub->Connect(&conn->ctx);
    if (!conn->stream)
        throw Error("failed to open gRPC stream to '%s'", config->authority.to_string());

    conn->toRemote.create();
    conn->fromRemote.create();
    nixgrpc::growPipe(conn->toRemote);
    nixgrpc::growPipe(conn->fromRemote);

    conn->reader = std::thread([c = &*conn] {
        try {
            nixgrpc::pumpFdToStream(c->toRemote.readSide.get(), *c->stream);
        } catch (...) {
            ignoreExceptionInDestructor();
        }
        c->stream->WritesDone();
        // If the stream broke, RemoteStore may be blocked in FdSink writing to
        // a full pipe with no drainer. Closing the read side turns that into
        // EPIPE so the error surfaces instead of hanging.
        c->toRemote.readSide.close();
    });

    conn->writer = std::thread([c = &*conn] {
        try {
            nixgrpc::pumpStreamToFd(*c->stream, c->fromRemote.writeSide.get());
        } catch (...) {
            ignoreExceptionInDestructor();
        }
        // Propagate EOF / error to the worker-protocol reader.
        c->fromRemote.writeSide.close();
    });

    conn->to = FdSink(conn->toRemote.writeSide.get());
    conn->from = FdSource(conn->fromRemote.readSide.get());
    return conn;
}

ref<Store> GrpcStoreConfig::openStore() const
{
    return make_ref<GrpcStore>(ref{shared_from_this()});
}

static RegisterStoreImplementation<GrpcStoreConfig> regGrpcStore;

} // namespace nix
