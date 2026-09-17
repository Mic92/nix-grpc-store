// Precompiled: the heavy third-party headers every daemon TU pulls in.
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <nlohmann/json.hpp>
#include <curl/curl.h>
#include <nix/store/store-api.hh>
#include <nix/store/worker-protocol-connection.hh>
#include <nix/util/error.hh>
#include <nix/util/serialise.hh>
#include "nix_remote.grpc.pb.h"
