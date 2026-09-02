project('nix-grpc-store', 'cpp',
  version : files('.version'),
  default_options : ['cpp_std=c++23'],
)

# --- dependencies -----------------------------------------------------------

grpc_dep     = dependency('grpc++')
protobuf_dep = dependency('protobuf')
zstd_dep     = dependency('libzstd')
threads_dep  = dependency('threads')
openssl_dep  = dependency('libcrypto')

prometheus_dep = [dependency('prometheus-cpp-core'), dependency('prometheus-cpp-pull')]

nix_store_dep = dependency('nix-store')
nix_util_dep  = dependency('nix-util')

# Pre-release versions look like "2.35pre20260619_f8bb823a".
nix_ver = nix_store_dep.version().split('.')
nix_ver_minor = nix_ver[1].split('pre')[0]
# Same rule Nix uses for its SONAME, so the loader can check whether the
# host already has exactly these libraries mapped.
nix_soversion = nix_store_dep.version().split('+')[0].split('pre')[0]

# Version macros for src/nix-compat.hh.
nix_compat_args = [
  '-DNIX_COMPAT_VERSION_MAJOR=' + nix_ver[0],
  '-DNIX_COMPAT_VERSION_MINOR=' + nix_ver_minor,
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

common_deps = [grpc_dep, protobuf_dep, zstd_dep, threads_dep]

# --- client plugin ----------------------------------------------------------

# Compile against the Nix headers only. The symbols resolve at dlopen() time
# from the host `nix` process, avoiding a second copy of the Nix libraries.
nix_header_deps = [
  nix_store_dep.partial_dependency(compile_args : true, includes : true),
  nix_util_dep.partial_dependency(compile_args : true, includes : true),
]
if host_machine.system() == 'darwin'
  plugin_link_args = ['-Wl,-undefined,dynamic_lookup']
  module_suffix = 'dylib'
else
  plugin_link_args = ['-Wl,--unresolved-symbols=ignore-all']
  module_suffix = 'so'
endif

shared_module('nix-grpc-store',
  sources : ['src/grpc-store.cc', gen],
  include_directories : gen_inc,
  # nix_plugin_entry() skips registering the store when the running Nix
  # version differs from the one the plugin was built against.
  cpp_args : nix_compat_args + ['-DNIX_GRPC_BUILT_AGAINST_NIX="' + nix_store_dep.version() + '"'],
  dependencies : common_deps + nix_header_deps + [openssl_dep],
  link_args : plugin_link_args,
  name_prefix : '',
  install : true,
  install_dir : get_option('libdir') / 'nix' / 'nix-grpc-store-versions' / nix_soversion,
)

# Loaded via nix.conf `plugin-files`. It dlopen()s the build matching the
# running Nix from the versions directory above, so merged installs of
# several builds support all of their Nix versions.

shared_module('nix-grpc-store-loader',
  sources : ['src/plugin-loader.cc'],
  cpp_args : ['-DNIX_GRPC_MODULE_SUFFIX="' + module_suffix + '"'],
  dependencies : dependency('dl'),
  link_args : plugin_link_args,
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
  dependencies : common_deps + prometheus_dep + [nix_util_dep, nix_store_dep],
  install : true,
)

# --- fuzzers ----------------------------------------------------------------

# Built only with -Dfuzzers=true (requires clang for libFuzzer).
# Run them with scripts/fuzz.sh.
if get_option('fuzzers')
  foreach name : ['zstd-reader', 'tunnel-pump', 'import-paths', 'build-log', 'nar-frames', 'wire', 'acl']
    executable('fuzz-' + name,
      sources : ['fuzz' / name + '.cc', 'fuzz/support.cc', gen],
      include_directories : gen_inc,
      cpp_args : nix_compat_args + ['-fsanitize=fuzzer'],
      link_args : ['-fsanitize=fuzzer'],
      dependencies : common_deps + [nix_util_dep, nix_store_dep],
    )
  endforeach
  executable('fuzz-gen-seeds',
    sources : ['fuzz/gen-seeds.cc', gen],
    include_directories : gen_inc,
    cpp_args : nix_compat_args,
    dependencies : common_deps + [nix_util_dep, nix_store_dep],
  )
endif
