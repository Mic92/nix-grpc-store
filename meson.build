project('nix-grpc-store', 'cpp',
  version : '0.1.0',
  default_options : ['cpp_std=c++23'],
)

# --- dependencies -----------------------------------------------------------

grpc_dep     = dependency('grpc++')
protobuf_dep = dependency('protobuf')
zstd_dep     = dependency('libzstd')
threads_dep  = dependency('threads')

nix_store_dep = dependency('nix-store')
nix_util_dep  = dependency('nix-util')

# --- protobuf / gRPC codegen -----------------------------------------------

protoc          = find_program('protoc')
grpc_cpp_plugin = find_program('grpc_cpp_plugin')

proto_dir = meson.current_source_dir() / 'proto'

gen = custom_target('nix_remote_proto',
  output : [
    'nix_remote.pb.cc',        'nix_remote.pb.h',
    'nix_remote.grpc.pb.cc',   'nix_remote.grpc.pb.h',
  ],
  command : [
    protoc,
    '--proto_path=' + proto_dir,
    '--cpp_out=@OUTDIR@',
    '--grpc_out=@OUTDIR@',
    '--plugin=protoc-gen-grpc=' + grpc_cpp_plugin.full_path(),
    # protoc needs the file name to be a suffix of a --proto_path entry, so we
    # cannot use @INPUT@ (which meson turns into a build-dir-relative path).
    'nix_remote.proto',
  ],
  depend_files : 'proto/nix_remote.proto',
)

gen_inc = include_directories('.')

pump_deps = [grpc_dep, protobuf_dep, zstd_dep, threads_dep, nix_util_dep]

# --- client plugin ----------------------------------------------------------

# Built as a shared module so it can be loaded via nix.conf `plugin-files`.
shared_module('nix-grpc-store',
  sources : ['src/grpc-store.cc', gen],
  include_directories : gen_inc,
  dependencies : pump_deps + [nix_store_dep],
  name_prefix : '',
  install : true,
  install_dir : get_option('libdir') / 'nix' / 'plugins',
)

# --- server -----------------------------------------------------------------

executable('nix-grpc-daemon',
  sources : ['src/nix-grpc-daemon.cc', gen],
  include_directories : gen_inc,
  # nix-store: native QueryValidPaths / AddMultipleToStore handlers.
  dependencies : pump_deps + [nix_store_dep],
  install : true,
)
