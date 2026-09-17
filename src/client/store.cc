// grpc:// Store: channel setup, error mapping, trust probe, worker-protocol tunnel, registration.

#include "store.hh"

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

#include <grpc/impl/channel_arg_names.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <nix/store/derivations.hh>
#include <nix/store/globals.hh>
#include <nix/store/path-info.hh>
#include <nix/store/remote-store.hh>
#include <nix/store/store-api.hh>
#include <nix/store/store-registration.hh>
#include <nix/store/worker-protocol.hh>
#include <nix/util/error.hh>
#include <nix/util/file-system.hh>
#include <nix/util/logging.hh>
#include <nix/util/ref.hh>
#include <nix/util/serialise.hh>
#include <nix/util/util.hh>

#include "channel.hh"
#include "nix_remote.grpc.pb.h"
#include "nix_remote.pb.h"
#include "pump.hh"

namespace nix {

auto GrpcStore::sslOptions() -> grpc::SslCredentialsOptions {
  grpc::SslCredentialsOptions ssl;
  auto caCert = config->caCert.get().empty() ? nixgrpc::defaultCaCert() : config->caCert.get();
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
    clientCert = nixgrpc::defaultClientCred("NIX_GRPC_CLIENT_CERT", "client.crt");
    clientKey = nixgrpc::defaultClientCred("NIX_GRPC_CLIENT_KEY", "client.key");
  }
  if (!clientCert.empty() && !clientKey.empty()) {
    ssl.pem_cert_chain = nixgrpc::readValidCert(clientCert);
    ssl.pem_private_key = readFile(clientKey);
    haveClientCert = true;
  }
  return ssl;
}

auto GrpcStore::makeChannel(bool ownConnection) -> std::shared_ptr<grpc::Channel>
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

GrpcStore::GrpcStore(const ref<const Config> &config)
    : Store{*config}, RemoteStore{*config}, config{config},
      narFetcher{
          [this] -> std::shared_ptr<grpc::Channel> { return makeChannel(true); },
          config->authority.to_string(), config->narConnections} {
  if (config->insecure) {
    creds = grpc::InsecureChannelCredentials();
  } else {
    creds = grpc::SslCredentials(sslOptions());
  }

  stub = remote::NixRemote::NewStub(makeChannel(false));
}

auto GrpcStore::statusError(const grpc::Status & status, const char * opName) const -> Error
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

auto GrpcStore::isTrustedClient() -> std::optional<TrustedFlag> {
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

  conn->reader = std::jthread([connPtr = &*conn] -> void {
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

  conn->writer = std::jthread([this, connPtr = &*conn] -> void {
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
  nixgrpc::retainGrpcRuntime();
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
