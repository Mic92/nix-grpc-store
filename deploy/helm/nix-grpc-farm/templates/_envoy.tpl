{{/* Envoy bootstrap, mirrors nixos/lb.nix. checks.helm diffs the two. */}}

{{- define "farm.envoy.clusterCommon" -}}
{{- $root := .root }}
name: {{ .name | quote }}
type: STRICT_DNS
connect_timeout: 5s
dns_lookup_family: ALL
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
    unhealthy_threshold: 2
    healthy_threshold: 1
    grpc_health_check: {{ if .healthService }}{service_name: {{ .healthService }}}{{ else }}{}{{ end }}
{{- if .healthService }}
common_lb_config:
  healthy_panic_threshold: {value: 0}
{{- end }}
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
{{- if $root.Values.tls.worker.existingSecret }}
transport_socket:
  name: envoy.transport_sockets.tls
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
    {{- with $root.Values.tls.worker.serverName }}
    sni: {{ . }}
    {{- end }}
    common_tls_context:
      alpn_protocols: [h2]
      {{- if $root.Values.tls.lb.existingSecret }}
      tls_certificates:
        - certificate_chain: {filename: /etc/envoy/tls/lb/tls.crt}
          private_key: {filename: /etc/envoy/tls/lb/tls.key}
      {{- end }}
      {{- if $root.Values.tls.clientCA.existingSecret }}
      validation_context:
        trusted_ca: {filename: /etc/envoy/tls/ca/ca.crt}
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
    {{- if .Values.tls.lb.existingSecret }}
    transport_socket:
      name: envoy.transport_sockets.tls
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
        {{- if .Values.tls.clientCA.existingSecret }}
        require_client_certificate: false # tokens keep working
        {{- end }}
        common_tls_context:
          alpn_protocols: [h2]
          tls_certificates:
            - certificate_chain: {filename: /etc/envoy/tls/lb/tls.crt}
              private_key: {filename: /etc/envoy/tls/lb/tls.key}
          {{- if .Values.tls.clientCA.existingSecret }}
          validation_context:
            trusted_ca: {filename: /etc/envoy/tls/ca/ca.crt}
          {{- end }}
    {{- end }}
{{- end }}

{{- define "farm.envoy.config" -}}
{{- $root := . }}
{{- $defaultGroup := include "farm.defaultGroup" . }}
{{- $groups := keys .Values.workers | sortAlpha }}
{{- /* default group last so its catch-all route does not shadow the others */}}
{{- $clusters := list }}
{{- $routes := list (include "farm.envoy.route" (dict "prefix" "/nix.remote.Scheduler/" "cluster" "sched" "system" "" "isDefault" true) | fromYaml) }}
{{- range $g := concat (without $groups $defaultGroup) (list $defaultGroup) }}
{{- $w := mustMergeOverwrite (deepCopy $root.Values.workerDefaults) ((index $root.Values.workers $g) | default dict) }}
{{- $svc := include "farm.workerService" (dict "root" $root "group" $g) }}
{{- $isDefault := eq $g $defaultGroup }}
{{- if and (not $isDefault) (not $w.system) }}{{ fail (printf "workers.%s.system is required for non-default groups" $g) }}{{ end }}
{{- $clusters = append $clusters (include "farm.envoy.workerCluster" (dict "root" $root "name" $g "service" $svc "healthService" "") | fromYaml) }}
{{- $routes = append $routes (include "farm.envoy.route" (dict "prefix" "/" "cluster" $g "system" $w.system "isDefault" $isDefault) | fromYaml) }}
{{- end }}
{{- $clusters = append $clusters (include "farm.envoy.clusterCommon" (dict "root" $root "name" "sched" "service" (include "farm.schedulerNames" $root) "healthService" "nix.scheduler") | fromYaml) }}
{{- $listener := include "farm.envoy.listener" (dict "Values" .Values "routes" $routes) | fromYaml }}
{{- toJson (dict
  "admin" (dict "address" (dict "socket_address" (dict "address" "::" "port_value" 9901 "ipv4_compat" true)))
  "static_resources" (dict "listeners" (list $listener) "clusters" $clusters)
) }}
{{- end }}
