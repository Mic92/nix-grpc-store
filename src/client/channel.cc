// Client credential lookup and gRPC runtime lifetime.

#include "channel.hh"

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include <grpc/event_engine/event_engine.h>
#include <grpc/grpc.h>

#include <openssl/bio.h>
#include <openssl/crypto.h>
#include <openssl/pem.h>
#include <openssl/types.h>
#include <openssl/x509.h>

#include <nix/util/environment-variables.hh>
#include <nix/util/error.hh>
#include <nix/util/file-system.hh>

namespace nixgrpc {

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
} // namespace

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

void retainGrpcRuntime() {
  struct GrpcRuntime {
    std::shared_ptr<grpc_event_engine::experimental::EventEngine> engine =
        grpc_event_engine::experimental::CreateEventEngine();
    GrpcRuntime() {
      OPENSSL_init_crypto(0, nullptr);
      grpc_init();
      grpc_event_engine::experimental::SetDefaultEventEngine(engine);
    }
    ~GrpcRuntime() {
      grpc_shutdown_blocking();
      grpc_event_engine::experimental::SetDefaultEventEngine(nullptr);
      constexpr std::chrono::seconds patience{2};
      constexpr std::chrono::milliseconds pollEvery{5};
      auto deadline = std::chrono::steady_clock::now() + patience;
      while (engine.use_count() > 1 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(pollEvery);
      }
    }
    GrpcRuntime(const GrpcRuntime &) = delete;
    GrpcRuntime(GrpcRuntime &&) = delete;
    auto operator=(const GrpcRuntime &) -> GrpcRuntime & = delete;
    auto operator=(GrpcRuntime &&) -> GrpcRuntime & = delete;
  };
  static const GrpcRuntime grpcRuntime;
}

} // namespace nixgrpc
