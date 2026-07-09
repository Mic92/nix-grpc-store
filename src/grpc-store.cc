// Nix store plugin that tunnels the worker protocol over a gRPC bidirectional
// stream. The gRPC layer is a dumb byte pipe; all store operations are handled
// by the existing RemoteStore implementation.
//
// URI: grpc://host:port
// Params: insecure, ca-cert, client-cert, client-key

#include <thread>

#include <grpcpp/grpcpp.h>

#include <nix/store/remote-store.hh>
#include <nix/store/remote-store-connection.hh>
#include <nix/store/store-registration.hh>
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
