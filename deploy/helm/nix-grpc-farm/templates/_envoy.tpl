{{/* Envoy bootstrap, mirrors nixos/lb.nix. checks.helm diffs the two. */}}

{{- define "farm.envoy.cluster" -}}
{{- $root := .root }}
name: {{ .name | quote }}
type: STRICT_DNS
connect_timeout: 5s
lb_policy: MAGLEV
dns_lookup_family: ALL
common_lb_config:
  consistent_hashing_lb_config:
    hash_balance_factor: 125
typed_extension_protocol_options:
  envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
    "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
    explicit_http_config:
      http2_protocol_options:
        max_concurrent_streams: {{ $root.Values.lb.maxStreams }}
        connection_keepalive: {interval: 30s, timeout: 10s}
outlier_detection:
  consecutive_5xx: 3
  base_ejection_time: 30s
  max_ejection_percent: 50
health_checks:
  - timeout: 2s
    interval: {{ $root.Values.lb.healthCheckInterval }}
    no_traffic_interval: {{ $root.Values.lb.healthCheckInterval }}
    unhealthy_threshold: 2
    healthy_threshold: 1
    grpc_health_check: {}
load_assignment:
  cluster_name: {{ .name | quote }}
  endpoints:
    - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: {{ printf "%s.%s.svc" .service $root.Release.Namespace }}
                port_value: 50051
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

{{- define "farm.envoy.route" -}}
match:
  prefix: /
  grpc: {}
  {{- if or .feature (not .isDefault) }}
  headers:
    {{- with .feature }}
    - name: x-nix-features
      string_match:
        safe_regex:
          regex: {{ printf "(.*,)?%s(,.*)?" (regexQuoteMeta .) | quote }}
    {{- end }}
    {{- if not .isDefault }}
    - name: x-nix-system
      string_match: {exact: {{ .system }}}
    {{- end }}
  {{- end }}
route:
  cluster: {{ .cluster | quote }}
  timeout: 0s # builds run for hours
  hash_policy:
    - header: {header_name: x-nix-drv}
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
{{- $featureRoutes := list }}
{{- $routes := list }}
{{- range $g := concat (without $groups $defaultGroup) (list $defaultGroup) }}
{{- $w := mustMergeOverwrite (deepCopy $root.Values.workerDefaults) ((index $root.Values.workers $g) | default dict) }}
{{- $svc := include "farm.workerService" (dict "root" $root "group" $g) }}
{{- $isDefault := eq $g $defaultGroup }}
{{- if and (not $isDefault) (not $w.system) }}{{ fail (printf "workers.%s.system is required for non-default groups" $g) }}{{ end }}
{{- $clusters = append $clusters (include "farm.envoy.cluster" (dict "root" $root "name" $g "service" $svc) | fromYaml) }}
{{- range $f := $w.features }}
{{- $cname := printf "%s/%s" $g $f }}
{{- $clusters = append $clusters (include "farm.envoy.cluster" (dict "root" $root "name" $cname "service" $svc) | fromYaml) }}
{{- $featureRoutes = append $featureRoutes (include "farm.envoy.route" (dict "cluster" $cname "feature" $f "system" $w.system "isDefault" $isDefault) | fromYaml) }}
{{- end }}
{{- $routes = append $routes (include "farm.envoy.route" (dict "cluster" $g "feature" "" "system" $w.system "isDefault" $isDefault) | fromYaml) }}
{{- end }}
{{- $listener := include "farm.envoy.listener" (dict "Values" .Values "routes" (concat $featureRoutes $routes)) | fromYaml }}
{{- toJson (dict
  "admin" (dict "address" (dict "socket_address" (dict "address" "::" "port_value" 9901 "ipv4_compat" true)))
  "static_resources" (dict "listeners" (list $listener) "clusters" $clusters)
) }}
{{- end }}
