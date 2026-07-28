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

# Version macros for src/nix-compat.hh; see that header for the differences
# between supported Nix versions.
nix_ver = nix_store_dep.version().split('.')
nix_compat_args = [
  '-DNIX_COMPAT_VERSION_MAJOR=' + nix_ver[0],
  # Pre-release versions look like "2.35pre20260619_f8bb823a".
  '-DNIX_COMPAT_VERSION_MINOR=' + nix_ver[1].split('pre')[0],
]

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
  cpp_args : nix_compat_args,
  dependencies : pump_deps + [nix_store_dep],
  name_prefix : '',
  install : true,
  install_dir : get_option('libdir') / 'nix' / 'plugins',
)

# --- server -----------------------------------------------------------------

executable('nix-grpc-daemon',
  sources : ['src/nix-grpc-daemon.cc', gen],
  include_directories : gen_inc,
  cpp_args : nix_compat_args,
  # nix-store: native QueryValidPaths / AddMultipleToStore handlers.
  dependencies : pump_deps + [nix_store_dep],
  install : true,
)
