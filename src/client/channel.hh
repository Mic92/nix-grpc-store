#pragma once
// Client channel setup: credential file lookup, per-call bearer token, fixed
// routing headers and gRPC runtime lifetime.

#include <exception>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/security/auth_context.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/config.h>
#include <grpcpp/support/interceptor.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/string_ref.h>

#include <nix/util/file-system.hh>
#include <nix/util/util.hh>

namespace nixgrpc {

// The server rejects an expired client cert during the TLS handshake, which
// gRPC reports only as "Socket closed", so check up front.
auto readValidCert(const std::string & path) -> std::string;
// Must run before the first channel: gRPC reads these once at init.
void enableGrpcTracing();
// Same lookup order as Nix's ssl-cert-file setting.
auto defaultCaCert() -> std::string;
// Env override, then per-user XDG data dir, then system-wide location.
auto defaultClientCred(const char * envVar, const std::string & fileName) -> std::string;
// gRPC frees TLS endpoints on EventEngine threads that outlive
// grpc_shutdown_blocking(), racing OpenSSL's atexit cleanup. Owning the
// default EventEngine lets us wait for those threads (bounded, nix may exit
// with a store still referenced). Not at plugin load: nix-daemon's forked
// workers must not run this.
void retainGrpcRuntime();

// Stamps fixed headers (x-nix-system from `system=`) on every RPC so a
// balancer routes store queries and uploads like the builds that follow.
class StaticHeaders final : public grpc::experimental::Interceptor {
public:
  using Headers = std::vector<std::pair<std::string, std::string>>;
  explicit StaticHeaders(std::shared_ptr<const Headers> headers) : headers(std::move(headers)) {}
  void Intercept(grpc::experimental::InterceptorBatchMethods * methods) override {
    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
      auto * metadata = methods->GetSendInitialMetadata();
      for (const auto & [key, value] : *headers) {
        if (!metadata->contains(key)) {
          metadata->emplace(key, value);
        }
      }
    }
    methods->Proceed();
  }

  class Factory final : public grpc::experimental::ClientInterceptorFactoryInterface {
  public:
    explicit Factory(Headers hdrs) : headers(std::make_shared<const Headers>(std::move(hdrs))) {}
    auto CreateClientInterceptor(grpc::experimental::ClientRpcInfo * /*info*/)
        -> grpc::experimental::Interceptor * override {
      return new StaticHeaders(headers); // NOLINT(cppcoreguidelines-owning-memory): gRPC deletes it
    }
  private:
    std::shared_ptr<const Headers> headers;
  };

private:
  std::shared_ptr<const Headers> headers;
};

// Reads the token per call so k8s projected volumes and CI refreshers work.
class TokenFileCredentials final : public grpc::MetadataCredentialsPlugin {
  std::string path;

public:
  explicit TokenFileCredentials(std::string path) : path(std::move(path)) {}

  [[nodiscard]] auto IsBlocking() const -> bool override { return true; }
  auto DebugString() -> std::string override { return "TokenFile(" + path + ")"; }

  auto GetMetadata(grpc::string_ref /*serviceUrl*/, grpc::string_ref /*methodName*/,
                   const grpc::AuthContext & /*channelAuthContext*/,
                   std::multimap<grpc::string, grpc::string> * metadata) -> grpc::Status override {
    try {
      auto const token = nix::chomp(nix::readFile(path));
      if (token.empty() || token.find_first_of("\r\n") != std::string::npos) {
        return {grpc::StatusCode::UNAUTHENTICATED, "token file '" + path + "' is empty or multi-line"};
      }
      metadata->emplace("authorization", "Bearer " + token);
      return grpc::Status::OK;
    } catch (std::exception & err) {
      return {grpc::StatusCode::UNAUTHENTICATED, std::string("reading token file: ") + err.what()};
    }
  }
};

} // namespace nixgrpc
