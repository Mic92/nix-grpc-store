{{/* Envoy bootstrap, mirrors nixos/lb.nix. checks.helm diffs the two. */}}

{{/* Filesystem SDS so certs renewed in place by cert-manager are reloaded. */}}
{{- define "farm.envoy.sds" -}}
{name: {{ . }}, sds_config: {resource_api_version: V3, path_config_source: {path: /etc/envoy/sds-{{ . }}.yaml, watched_directory: {path: /etc/envoy/tls/{{ . }}}}}}
{{- end }}

{{- define "farm.envoy.sdsFile" -}}
resources:
  - "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret
    name: {{ . }}
    {{- if has . (list "lb" "public") }}
    tls_certificate:
      certificate_chain: {filename: /etc/envoy/tls/{{ . }}/tls.crt}
      private_key: {filename: /etc/envoy/tls/{{ . }}/tls.key}
    {{- else }}
    validation_context:
      trusted_ca: {filename: /etc/envoy/tls/ca/ca.crt}
    {{- end }}
{{- end }}

{{- define "farm.envoy.clusterCommon" -}}
{{- $root := .root }}
name: {{ .name | quote }}
{{- if .edsSystem }}
type: EDS
{{- else }}
type: STRICT_DNS
dns_lookup_family: ALL
{{- end }}
connect_timeout: 5s
# Streams last hours; the default 1024 would 503 the next one.
circuit_breakers:
  thresholds: [{max_connections: 1000000, max_pending_requests: 1000000, max_requests: 1000000}]
typed_extension_protocol_options:
  envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
    "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
    explicit_http_config:
      http2_protocol_options:
        max_concurrent_streams: {{ $root.Values.lb.maxStreams }}
        connection_keepalive: {interval: 30s, timeout: 10s}
health_checks:
  - timeout: 2s
    interval: {{ $root.Values.lb.healthCheckInterval }}
    no_traffic_interval: {{ $root.Values.lb.healthCheckInterval }}
    # A standby that just took over should get traffic within a second.
    unhealthy_interval: 1s
    unhealthy_edge_interval: 1s
    unhealthy_threshold: 2
    healthy_threshold: 1
    grpc_health_check: {{ if .healthService }}{service_name: {{ .healthService }}}{{ else }}{}{{ end }}
{{- if .healthService }}
common_lb_config:
  healthy_panic_threshold: {value: 0}
{{- else if .edsSystem }}
# With envoy's default 1 s window, list changes reach the workers late. In
# tests, builders added around an EDS stream reopen then got no requests at
# all.
common_lb_config:
  update_merge_window: 0s
{{- end }}
{{- if .edsSystem }}
# A builder stops getting requests as soon as the scheduler drops it.
ignore_health_on_host_removal: true
eds_cluster_config:
  service_name: {{ .edsSystem | quote }}
  eds_config:
    resource_api_version: V3
    api_config_source:
      api_type: GRPC
      transport_api_version: V3
      grpc_services: [{envoy_grpc: {cluster_name: sched}}]
{{- else }}
load_assignment:
  cluster_name: {{ .name | quote }}
  endpoints:
    - lb_endpoints:
        {{- range $svc := splitList "," .service }}
        - endpoint:
            address:
              socket_address:
                address: {{ printf "%s.%s.svc" $svc $root.Release.Namespace }}
                port_value: 50051
        {{- end }}
{{- end }}
{{- if (include "farm.tlsSecret" (list $root "worker")) }}
transport_socket:
  name: envoy.transport_sockets.tls
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
    {{- with $root.Values.tls.worker.serverName }}
    sni: {{ . }}
    {{- end }}
    common_tls_context:
      alpn_protocols: [h2]
      {{- if (include "farm.tlsSecret" (list $root "lb")) }}
      tls_certificate_sds_secret_configs: [{{ include "farm.envoy.sds" "lb" }}]
      {{- end }}
      {{- if (include "farm.tlsSecret" (list $root "clientCA")) }}
      {{- with $root.Values.tls.worker.serverName }}
      combined_validation_context:
        default_validation_context:
          match_typed_subject_alt_names: [{san_type: DNS, matcher: {exact: {{ . }}}}]
        validation_context_sds_secret_config: {{ include "farm.envoy.sds" "ca" }}
      {{- else }}
      validation_context_sds_secret_config: {{ include "farm.envoy.sds" "ca" }}
      {{- end }}
      {{- end }}
{{- end }}
{{- end }}

{{/* BuildDerivation goes to the worker the scheduler named in x-nix-worker. */}}
{{- define "farm.envoy.workerCluster" -}}
{{ include "farm.envoy.clusterCommon" . }}
load_balancing_policy:
  policies:
    - typed_extension_config:
        name: envoy.load_balancing_policies.override_host
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.load_balancing_policies.override_host.v3.OverrideHost
          override_host_sources: [{header: x-nix-worker}]
          fallback_policy:
            policies:
              - typed_extension_config:
                  name: envoy.load_balancing_policies.least_request
                  typed_config:
                    "@type": type.googleapis.com/envoy.extensions.load_balancing_policies.least_request.v3.LeastRequest
{{- end }}

{{/* Builders that connect to the balancer (--tunnel). hostId picks the tunnel
     for a request. The builder's name was checked against its certificate
     when it opened the tunnel, so here the chain is enough. */}}
{{- define "farm.envoy.tunnelCluster" -}}
name: {{ .name }}
connect_timeout: 5s
lb_policy: CLUSTER_PROVIDED
cluster_type:
  name: envoy.clusters.reverse_connection
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.clusters.reverse_connection.v3.ReverseConnectionClusterConfig
    cleanup_interval: 60s
    host_id_format: {{ .hostId | quote }}
# Streams last hours; the default 1024 would 503 the next one. The default
# for retries lets three requests at a time wait for a tunnel.
circuit_breakers:
  thresholds: [{max_connections: 1000000, max_pending_requests: 1000000, max_requests: 1000000, max_retries: 1000000}]
typed_extension_protocol_options:
  envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
    "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
    explicit_http_config:
      http2_protocol_options:
        max_concurrent_streams: {{ .root.Values.lb.maxStreams }}
        connection_keepalive: {interval: 30s, timeout: 10s}
transport_socket:
  name: envoy.transport_sockets.tls
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
    common_tls_context:
      alpn_protocols: [h2]
      tls_certificate_sds_secret_configs: [{{ include "farm.envoy.sds" "lb" }}]
      validation_context_sds_secret_config: {{ include "farm.envoy.sds" "ca" }}
{{- end }}

{{/* A builder joins the set its ALPN name asks for, under its certificate's
     name and worker name. */}}
{{- define "farm.envoy.tunnelChain" -}}
filter_chain_match: {application_protocols: [{{ .protocol }}]}
transport_socket:
  name: envoy.transport_sockets.tls
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
    require_client_certificate: true
    common_tls_context:
      tls_certificate_sds_secret_configs: [{{ include "farm.envoy.sds" "lb" }}]
      validation_context_sds_secret_config: {{ include "farm.envoy.sds" "ca" }}
filters:
  - name: envoy.filters.network.reverse_tunnel
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.network.reverse_tunnel.v3.ReverseTunnel
      validation: {{ toJson .validation }}
{{- end }}

{{- define "farm.envoy.tunnelListener" -}}
name: tunnels
address:
  socket_address: {address: "::", port_value: 9443, ipv4_compat: true}
listener_filters:
  - name: envoy.filters.listener.tls_inspector
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.listener.tls_inspector.v3.TlsInspector
filter_chains:
  {{- /* The common name of the builder's certificate if auth.workerCNs lists
       it. Envoy gives the subject as text, common name first. It refuses a
       field that comes out empty only if it is the whole field, which the
       tenant is; a builder sends its name there. */}}
  {{- $names := list }}
  {{- range .Values.auth.workerCNs }}
  {{- $names = append $names (. | replace "." "\\." | replace "*" "[^,+/\\\\]*") }}
  {{- end }}
  {{- $name := printf "%%CEL(re.extract(connection.subject_peer_certificate, r'^CN=(%s)(,|$)', r'\\1'))%%" (join "|" $names) }}
  {{- $node := print $name "/%REQ(x-nix-instance)%" }}
  - {{- include "farm.envoy.tunnelChain" (dict "protocol" "nix-tunnel" "validation" (dict "node_id_format" $node "tenant_id_format" $name)) | nindent 4 }}
  - {{- include "farm.envoy.tunnelChain" (dict "protocol" "nix-tunnel-any" "validation" (dict "node_id_format" (print "any/" $node) "cluster_id_format" "any" "tenant_id_format" $name)) | nindent 4 }}
{{- end }}

{{/* A tunnelled builder is known by a name, a dialled one by IP:port. */}}
{{- define "farm.envoy.tunnelRoute" -}}
match:
  prefix: /
  grpc: {}
  headers:
    - {name: x-nix-worker, present_match: true}
    - name: x-nix-worker
      string_match: {safe_regex: {regex: "^.+:[0-9]+$"}}
      invert_match: true
route:
  cluster: tunnelled
  timeout: 0s
  # A balancer that just started lacks its builders' tunnels for some
  # seconds. Nothing was sent to a builder when the connection fails.
  retry_policy:
    retry_on: connect-failure
    num_retries: 10
    retry_back_off: {base_interval: 0.5s, max_interval: 2s}
{{- end }}

{{- define "farm.envoy.route" -}}
match:
  prefix: {{ .prefix }}
  grpc: {}
  {{- if and .system (not .isDefault) }}
  headers:
    - name: x-nix-system
      string_match: {exact: {{ .system }}}
  {{- end }}
route:
  cluster: {{ .cluster | quote }}
  timeout: 0s # builds run for hours
  # An evicted pod stays in DNS until it is gone and in the healthy set for
  # two probe intervals. Nothing reached it, so try the next host instead.
  retry_policy:
    retry_on: connect-failure,refused-stream
    num_retries: 2
    retry_host_predicate:
      - name: envoy.retry_host_predicates.previous_hosts
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.retry.host.previous_hosts.v3.PreviousHostsPredicate
    host_selection_retry_max_attempts: 3
{{- end }}

{{- define "farm.envoy.listener" -}}
name: farm
address:
  socket_address: {address: "::", port_value: 50051, ipv4_compat: true}
{{- if .Values.lb.accessLog }}
access_log:
  - name: envoy.access_loggers.stderr
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StderrAccessLog
      log_format:
        text_format_source:
          inline_string: "conn peer=%DOWNSTREAM_REMOTE_ADDRESS% tls=%DOWNSTREAM_TLS_VERSION% subject=\"%DOWNSTREAM_PEER_SUBJECT%\" sni=%REQUESTED_SERVER_NAME% flags=%RESPONSE_FLAGS% tls_fail=\"%DOWNSTREAM_TRANSPORT_FAILURE_REASON%\" rx=%BYTES_RECEIVED% tx=%BYTES_SENT% ms=%DURATION%\n"
{{- end }}
{{- $root := . }}
{{- $public := include "farm.tlsSecret" (list . "public") }}
{{- /* Clients get the public certificate if there is one, else the farm-CA
     one. The nodes always get tls.lb, picked by the in-cluster name they dial. */}}
{{- $chains := list (dict "cert" ($public | empty | ternary "lb" "public") "names" list) }}
{{- if $public }}
{{- $name := include "farm.fullname" . }}
{{- $chains = prepend $chains (dict "cert" "lb" "names" (list $name (printf "%s.%s" $name .Release.Namespace) (printf "%s.%s.svc" $name .Release.Namespace))) }}
listener_filters:
  - name: envoy.filters.listener.tls_inspector
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.listener.tls_inspector.v3.TlsInspector
{{- end }}
filter_chains:
{{- range $chain := $chains }}
  -
    {{- with $chain.names }}
    filter_chain_match: {server_names: {{ toJson . }}}
    {{- end }}
    filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: farm
          codec_type: HTTP2
          stream_idle_timeout: 0s
          http2_protocol_options:
            max_concurrent_streams: {{ $root.Values.lb.maxStreams }}
            connection_keepalive: {interval: 10s, timeout: 5s}
          forward_client_cert_details: SANITIZE_SET
          # The URI too: a node matches spiffe:// rules on it.
          set_current_client_cert_details: {subject: true, uri: true}
          route_config:
            virtual_hosts:
              - name: farm
                domains: ["*"]
                routes: {{ toJson $root.routes }}
          http_filters:
            - name: envoy.filters.http.router
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
    {{- if or $public (include "farm.tlsSecret" (list $root "lb")) }}
    transport_socket:
      name: envoy.transport_sockets.tls
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
        {{- if (include "farm.tlsSecret" (list $root "clientCA")) }}
        require_client_certificate: false # tokens keep working
        {{- end }}
        common_tls_context:
          alpn_protocols: [h2]
          tls_certificate_sds_secret_configs: [{{ include "farm.envoy.sds" $chain.cert }}]
          {{- if (include "farm.tlsSecret" (list $root "clientCA")) }}
          validation_context_sds_secret_config: {{ include "farm.envoy.sds" "ca" }}
          {{- end }}
    {{- end }}
{{- end }}
{{- end }}

{{/* The admin API can stop envoy, so it stays on loopback. Probes and
     scrapes reach just these two paths through a listener of their own. */}}
{{- define "farm.envoy.statsListener" -}}
name: stats
address:
  socket_address: {address: "::", port_value: 9901, ipv4_compat: true}
filter_chains:
  - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: stats
          route_config:
            virtual_hosts:
              - name: stats
                domains: ["*"]
                routes:
                  - {match: {path: /ready}, route: {cluster: admin}}
                  - {match: {path: /stats/prometheus}, route: {cluster: admin}}
          http_filters:
            - name: envoy.filters.http.router
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
{{- end }}

{{- define "farm.envoy.adminCluster" -}}
name: admin
type: STATIC
load_assignment:
  cluster_name: admin
  endpoints:
    - lb_endpoints:
        - endpoint: {address: {socket_address: {address: 127.0.0.1, port_value: 9902}}}
{{- end }}

{{- define "farm.envoy.config" -}}
{{- $root := . }}
{{- $defaultGroup := include "farm.defaultGroup" . }}
{{- $groups := keys .Values.workers | sortAlpha }}
{{- /* default group last so its catch-all route does not shadow the others */}}
{{- $clusters := list }}
{{- /* Envoy reads the builder list from the scheduler over its own connection. A route for it here would let any client read the list. */}}
{{- $routes := list (dict "match" (dict "prefix" "/envoy.service.endpoint.v3.EndpointDiscoveryService/" "grpc" (dict)) "direct_response" (dict "status" 404)) (include "farm.envoy.route" (dict "prefix" "/nix.remote.Scheduler/" "cluster" "sched" "system" "" "isDefault" true) | fromYaml) }}
{{- $tunnels := .Values.lb.tunnel.enabled }}
{{- if $tunnels }}
{{- if not (and (include "farm.tlsSecret" (list . "lb")) (include "farm.tlsSecret" (list . "clientCA"))) }}{{ fail "lb.tunnel needs tls.lb and tls.clientCA: the balancer knows a builder by its certificate" }}{{ end }}
{{- $routes = append $routes (include "farm.envoy.tunnelRoute" . | fromYaml) }}
{{- end }}
{{- range $g := concat (without $groups $defaultGroup) (list $defaultGroup) }}
{{- $w := mustMergeOverwrite (deepCopy $root.Values.workerDefaults) ((index $root.Values.workers $g) | default dict) }}
{{- $svc := include "farm.workerService" (dict "root" $root "group" $g) }}
{{- $isDefault := eq $g $defaultGroup }}
{{- if not $w.system }}{{ fail (printf "workers.%s.system is required, the scheduler names the endpoints of a group by system" $g) }}{{ end }}
{{- $clusters = append $clusters (include "farm.envoy.workerCluster" (dict "root" $root "name" $g "edsSystem" $w.system) | fromYaml) }}
{{- $routes = append $routes (include "farm.envoy.route" (dict "prefix" "/" "cluster" $g "system" $w.system "isDefault" $isDefault) | fromYaml) }}
{{- end }}
{{- $clusters = append $clusters (include "farm.envoy.clusterCommon" (dict "root" $root "name" "sched" "service" (include "farm.schedulerNames" $root) "healthService" "nix.scheduler") | fromYaml) }}
{{- $listener := include "farm.envoy.listener" (dict "Values" .Values "Chart" .Chart "Release" .Release "routes" $routes) | fromYaml }}
{{- $listeners := list $listener (include "farm.envoy.statsListener" . | fromYaml) }}
{{- $config := dict
  "node" (dict "id" "nix-grpc-farm-lb" "cluster" "nix-grpc-farm")
  "admin" (dict "address" (dict "socket_address" (dict "address" "127.0.0.1" "port_value" 9902)))
}}
{{- if $tunnels }}
{{- $listeners = append $listeners (include "farm.envoy.tunnelListener" . | fromYaml) }}
{{- $clusters = concat $clusters (list
  (include "farm.envoy.tunnelCluster" (dict "root" $root "name" "tunnelled" "hostId" "%REQ(x-nix-worker)%") | fromYaml)
  (include "farm.envoy.tunnelCluster" (dict "root" $root "name" "tunnelled-any" "hostId" "any") | fromYaml)) }}
{{- $_ := set $config "bootstrap_extensions" (list (dict
  "name" "envoy.bootstrap.reverse_tunnel.upstream_socket_interface"
  "typed_config" (dict
    "@type" "type.googleapis.com/envoy.extensions.bootstrap.reverse_tunnel.upstream_socket_interface.v3.UpstreamReverseConnectionSocketInterface"
    "stat_prefix" "upstream_reverse_connection"))) }}
{{- end }}
{{- $_ := set $config "static_resources" (dict "listeners" $listeners "clusters" (append $clusters (include "farm.envoy.adminCluster" . | fromYaml))) }}
{{- toJson $config }}
{{- end }}
