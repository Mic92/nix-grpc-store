// gRPC server that proxies tunnelled Nix worker-protocol connections straight
// to a nix-daemon unix socket. The real nix-daemon handles trust, forking and
// interrupt-on-hangup; this process only moves bytes (and optionally zstd).

#include <csignal>
#include <sys/socket.h>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <nix/util/file-descriptor.hh>
#include <nix/util/file-system.hh>
#include <nix/util/logging.hh>
#include <nix/util/unix-domain-socket.hh>
#include <nix/util/util.hh>

#include "pump.hh"

using namespace nix;
using GrpcStream = grpc::ServerReaderWriter<remote::Chunk, remote::Chunk>;

class NixRemoteService final : public remote::NixRemote::Service
{
    std::string socketPath;

public:
    explicit NixRemoteService(std::string socketPath)
        : socketPath(std::move(socketPath))
    {
    }

    grpc::Status Connect(grpc::ServerContext *, GrpcStream * stream) override
    {
        AutoCloseFD sock;
        try {
            sock = nix::connect(std::filesystem::path{socketPath});
        } catch (Error & e) {
            return {grpc::StatusCode::UNAVAILABLE, e.what()};
        }

        std::thread recvT([&] {
            try {
                nixgrpc::pumpStreamToFd(*stream, sock.get());
            } catch (...) {
                ignoreExceptionInDestructor();
            }
            ::shutdown(sock.get(), SHUT_WR);
        });

        try {
            nixgrpc::pumpFdToStream(sock.get(), *stream);
        } catch (...) {
            ignoreExceptionInDestructor();
        }

        recvT.join();
        return grpc::Status::OK;
    }
};

int main(int argc, char ** argv)
try {
    // Pump threads write to a socket whose peer may already be gone; we want
    // EPIPE, not process death.
    std::signal(SIGPIPE, SIG_IGN);

    std::string listen = "0.0.0.0:50051";
    std::string socketPath = "/nix/var/nix/daemon-socket/socket";
    std::string tlsCert, tlsKey, clientCA;

    for (int i = 1; i < argc; ++i) {
        std::string_view a = argv[i];
        auto next = [&]() -> std::string {
            if (++i >= argc)
                throw Error("flag '%s' requires an argument", a);
            return argv[i];
        };
        if (a == "--listen")
            listen = next();
        else if (a == "--proxy-socket")
            socketPath = next();
        else if (a == "--tls-cert")
            tlsCert = next();
        else if (a == "--tls-key")
            tlsKey = next();
        else if (a == "--client-ca")
            clientCA = next();
        else
            throw Error("unknown flag '%s'", a);
    }

    NixRemoteService service(socketPath);

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);

    if (!tlsCert.empty()) {
        // With --client-ca the server requires and verifies a client
        // certificate (mTLS); without it, plain server-auth TLS.
        grpc::SslServerCredentialsOptions ssl(
            clientCA.empty() ? GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE
                             : GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
        ssl.pem_key_cert_pairs.push_back({readFile(tlsKey), readFile(tlsCert)});
        if (!clientCA.empty())
            ssl.pem_root_certs = readFile(clientCA);
        builder.AddListeningPort(listen, grpc::SslServerCredentials(ssl));
    } else {
        if (!clientCA.empty())
            throw Error("--client-ca requires --tls-cert/--tls-key");
        builder.AddListeningPort(listen, grpc::InsecureServerCredentials());
    }

    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    if (!server)
        throw Error("failed to start gRPC server on '%s'", listen);

    printInfo("nix-grpc-daemon listening on %s → %s", listen, socketPath);
    server->Wait();
    return 0;
} catch (Error & e) {
    printError("nix-grpc-daemon: %s", e.what());
    return 1;
}
