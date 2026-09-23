{{/* Envoy bootstrap, mirrors nixos/lb.nix. checks.helm diffs the two. */}}

{{/* Filesystem SDS so certs renewed in place by cert-manager are reloaded. */}}
{{- define "farm.envoy.sds" -}}
{name: {{ . }}, sds_config: {resource_api_version: V3, path_config_source: {path: /etc/envoy/sds-{{ . }}.yaml, watched_directory: {path: /etc/envoy/tls/{{ . }}}}}}
{{- end }}

{{- define "farm.envoy.sdsFile" -}}
resources:
  - "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret
    name: {{ . }}
    {{- if eq . "lb" }}
    tls_certificate:
      certificate_chain: {filename: /etc/envoy/tls/lb/tls.crt}
      private_key: {filename: /etc/envoy/tls/lb/tls.key}
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
      validation_context_sds_secret_config: {{ include "farm.envoy.sds" "ca" }}
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
filter_chains:
  - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: farm
          codec_type: HTTP2
          stream_idle_timeout: 0s
          http2_protocol_options:
            max_concurrent_streams: {{ .Values.lb.maxStreams }}
            connection_keepalive: {interval: 10s, timeout: 5s}
          forward_client_cert_details: SANITIZE_SET
          set_current_client_cert_details: {subject: true}
          route_config:
            virtual_hosts:
              - name: farm
                domains: ["*"]
                routes: {{ toJson .routes }}
          http_filters:
            - name: envoy.filters.http.router
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
    {{- if (include "farm.tlsSecret" (list . "lb")) }}
    transport_socket:
      name: envoy.transport_sockets.tls
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
        {{- if (include "farm.tlsSecret" (list . "clientCA")) }}
        require_client_certificate: false # tokens keep working
        {{- end }}
        common_tls_context:
          alpn_protocols: [h2]
          tls_certificate_sds_secret_configs: [{{ include "farm.envoy.sds" "lb" }}]
          {{- if (include "farm.tlsSecret" (list . "clientCA")) }}
          validation_context_sds_secret_config: {{ include "farm.envoy.sds" "ca" }}
          {{- end }}
    {{- end }}
{{- end }}

{{- define "farm.envoy.config" -}}
{{- $root := . }}
{{- $defaultGroup := include "farm.defaultGroup" . }}
{{- $groups := keys .Values.workers | sortAlpha }}
{{- /* default group last so its catch-all route does not shadow the others */}}
{{- $clusters := list }}
{{- /* Envoy reads the builder list from the scheduler over its own connection. A route for it here would let any client read the list. */}}
{{- $routes := list (dict "match" (dict "prefix" "/envoy.service.endpoint.v3.EndpointDiscoveryService/" "grpc" (dict)) "direct_response" (dict "status" 404)) (include "farm.envoy.route" (dict "prefix" "/nix.remote.Scheduler/" "cluster" "sched" "system" "" "isDefault" true) | fromYaml) }}
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
{{- toJson (dict
  "node" (dict "id" "nix-grpc-farm-lb" "cluster" "nix-grpc-farm")
  "admin" (dict "address" (dict "socket_address" (dict "address" "::" "port_value" 9901 "ipv4_compat" true)))
  "static_resources" (dict "listeners" (list $listener) "clusters" $clusters)
) }}
{{- end }}
