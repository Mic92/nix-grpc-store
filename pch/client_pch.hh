// Precompiled: the heavy third-party headers every client TU pulls in.
#include <grpcpp/grpcpp.h>
#include <nix/store/remote-store.hh>
#include <nix/store/store-api.hh>
#include <nix/util/error.hh>
#include <nix/util/logging.hh>
#include <nix/util/serialise.hh>
#include "nix_remote.grpc.pb.h"
