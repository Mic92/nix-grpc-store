{{- define "farm.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "farm.fullname" -}}
{{- if contains .Chart.Name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "farm.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "farm.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "farm.selectorLabels" -}}
app.kubernetes.io/name: {{ include "farm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* call with (dict "root" $ "group" "<name>") */}}
{{- define "farm.workerSelectorLabels" -}}
{{ include "farm.selectorLabels" .root }}
app.kubernetes.io/component: worker
nix-grpc-farm/group: {{ .group }}
{{- end }}

{{- define "farm.lbSelectorLabels" -}}
{{ include "farm.selectorLabels" . }}
app.kubernetes.io/component: lb
{{- end }}

{{- define "farm.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "farm.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "farm.image" -}}
{{ .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end }}

{{- define "farm.lbImage" -}}
{{ .Values.lbImage.repository }}:{{ default .Chart.AppVersion .Values.lbImage.tag }}
{{- end }}

{{- define "farm.workerService" -}}
{{ include "farm.fullname" .root }}-worker-{{ .group }}
{{- end }}

{{- define "farm.schedulerService" -}}
{{ include "farm.fullname" . }}-scheduler
{{- end }}

{{/* scheduler-<i> Service names, best first. One entry when replicas=1. */}}
{{- define "farm.schedulerNames" -}}
{{- $root := . }}
{{- $n := int (.Values.scheduler.replicas | default 1) }}
{{- $names := list }}
{{- range $i := until $n }}
{{- $names = append $names (ternary (include "farm.schedulerService" $root) (printf "%s-%d" (include "farm.schedulerService" $root) $i) (eq $n 1)) }}
{{- end }}
{{- join "," $names }}
{{- end }}

{{/* --scheduler for builders: direct, or via the balancer with replicas > 1. */}}
{{- define "farm.schedulerFor" -}}
{{- if .Values.scheduler.external -}}
{{ .Values.scheduler.external }}
{{- else if gt (int (.Values.scheduler.replicas | default 1)) 1 -}}
{{ include "farm.fullname" . }}.{{ .Release.Namespace }}.svc:{{ .Values.lb.service.port }}
{{- else -}}
{{ include "farm.schedulerService" . }}.{{ .Release.Namespace }}.svc:50051
{{- end }}
{{- end }}

{{- define "farm.schedulerSelectorLabels" -}}
{{ include "farm.selectorLabels" . }}
app.kubernetes.io/component: scheduler
{{- end }}

{{/* Validate cross-field constraints once; included from the worker template. */}}
{{- define "farm.validate" -}}
{{- $_ := required "niks3.serverURL is required" .Values.niks3.serverURL }}
{{- $_ := required "niks3.cacheURL is required" .Values.niks3.cacheURL }}
{{- if empty .Values.niks3.publicKeys }}{{ fail "niks3.publicKeys must list the cache's signing keys" }}{{ end }}
{{- $tok := .Values.niks3.auth }}
{{- $tokens := list $tok.existingSecret $tok.serviceAccountToken.enabled .Values.tls.worker.spire.niks3Token | compact }}
{{- if gt (len $tokens) 1 }}{{ fail "set only one of niks3.auth.existingSecret, niks3.auth.serviceAccountToken.enabled and tls.worker.spire.niks3Token" }}{{ end }}
{{- if not $tokens }}{{ fail "set niks3.auth.existingSecret, niks3.auth.serviceAccountToken.enabled or tls.worker.spire.niks3Token" }}{{ end }}
{{- if empty .Values.workers }}{{ fail "workers is empty, add e.g. workers.x86-64: {system: x86_64-linux, replicas: 2}" }}{{ end }}
{{- if and (or .Values.auth.accessRules .Values.auth.anonymousRole) (not (or (include "farm.tlsSecret" (list . "clientCA")) (include "farm.oidcProviders" .) .Values.tls.worker.spire.agentSocket)) }}{{ fail "auth.accessRules/anonymousRole need tls.clientCA, tls.worker.spire or an OIDC provider" }}{{ end }}
{{- $spire := .Values.tls.worker.spire }}
{{- if and (include "farm.tlsSecret" (list . "clientCA")) (not (or (include "farm.tlsSecret" (list . "worker")) $spire.agentSocket)) }}{{ fail "tls.clientCA requires tls.worker (workers verify the balancer over TLS)" }}{{ end }}
{{- if $spire.agentSocket }}
{{- if or .Values.tls.worker.existingSecret .Values.tls.certManager.enabled }}{{ fail "tls.worker.spire.agentSocket replaces the worker certificate: drop tls.worker.existingSecret and tls.certManager" }}{{ end }}
{{- if .Values.tls.clientCA.existingSecret }}{{ fail "tls.worker.spire.agentSocket makes the SPIRE bundle the client CA: drop tls.clientCA" }}{{ end }}
{{- if not .Values.scheduler.external }}{{ fail "tls.worker.spire.agentSocket needs scheduler.external: this release's scheduler and balancer would not trust the SVIDs" }}{{ end }}
{{- else if $spire.niks3Token }}{{ fail "tls.worker.spire.niks3Token needs tls.worker.spire.agentSocket" }}
{{- end }}
{{- if and (include "farm.lbEnabled" .) (include "farm.tlsSecret" (list . "clientCA")) (not (include "farm.tlsSecret" (list . "lb"))) }}{{ fail "tls.clientCA requires tls.lb (the balancer presents it to clients and workers)" }}{{ end }}
{{- with .Values.defaultGroup }}{{ if not (hasKey $.Values.workers .) }}{{ fail (printf "defaultGroup %q is not a key of workers" .) }}{{ end }}{{ end }}
{{- end }}

{{/* spiffe-helper config for the worker pods; "daemon" false fetches once (the init container). */}}
{{- define "farm.spiffeHelperConf" -}}
agent_address = "/run/spire/agent/{{ base .root.Values.tls.worker.spire.agentSocket }}"
cert_dir = "/svid"
svid_file_name = "svid.pem"
svid_key_file_name = "svid.key"
svid_bundle_file_name = "bundle.pem"
key_file_mode = 0640
daemon_mode = {{ .daemon }}
{{- if .root.Values.tls.worker.spire.niks3Token }}
jwt_svids = [{jwt_audience = {{ .root.Values.niks3.serverURL | quote }}, jwt_svid_file_name = "niks3-token"}]
{{- end }}
{{- end }}

{{/* The helper writes as the daemon's user, so the daemon reads the key. */}}
{{- define "farm.spiffeHelperSecurityContext" -}}
runAsUser: 990
runAsGroup: 990
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities: {drop: [ALL]}
{{- end }}

{{- define "farm.spiffeHelperMounts" -}}
- {name: svid, mountPath: /svid}
- {name: spire-agent-socket, mountPath: /run/spire/agent, readOnly: true}
- {name: spiffe-helper, mountPath: /etc/spiffe-helper, readOnly: true}
{{- end }}

{{/* Non-empty when this release runs the balancer: an external scheduler brings its own. */}}
{{- define "farm.lbEnabled" -}}
{{- if and .Values.lb.enabled (not .Values.scheduler.external) }}true{{ end }}
{{- end }}

{{- define "farm.defaultGroup" -}}
{{- default (keys .Values.workers | sortAlpha | first) .Values.defaultGroup }}
{{- end }}

{{/* OIDC providers map incl. the synthesized Kubernetes one; empty when unused.
     Same shape as niks3's chart so both are configured alike. */}}
{{- define "farm.oidcProviders" -}}
{{- $providers := deepCopy (.Values.auth.oidcProviders | default dict) }}
{{- with .Values.auth.workloadIdentity }}
{{- if .enabled }}
{{- $subjects := list }}
{{- range (required "auth.workloadIdentity.allowedServiceAccounts must not be empty" .allowedServiceAccounts) }}
{{- $subjects = append $subjects (printf "system:serviceaccount:%s" .) }}
{{- end }}
{{- $_ := set $providers "kubernetes" (dict
  "issuer" .issuer
  "jwks_url" "https://kubernetes.default.svc/openid/v1/jwks"
  "audience" .audience
  "bound_subject" $subjects
  "scopes" .scopes
  "ca_file" "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
  "bearer_token_file" "/var/run/secrets/kubernetes.io/serviceaccount/token") }}
{{- end }}
{{- end }}
{{- if $providers }}{{ toJson (dict "providers" $providers) }}{{- end }}
{{- end }}

{{/* Secret name for tls.<part> (lb | worker | clientCA): the cert-manager
     managed one when tls.certManager.enabled, else existingSecret. For public,
     its existingSecret, else the managed one if tls.certManager.public has
     an issuerRef, else nothing. */}}
{{- define "farm.tlsSecret" -}}
{{- $root := index . 0 }}{{ $part := index . 1 }}
{{- if eq $part "public" -}}
{{- if $root.Values.tls.public.existingSecret -}}
{{ $root.Values.tls.public.existingSecret }}
{{- else if and $root.Values.tls.certManager.enabled $root.Values.tls.certManager.public.issuerRef.name -}}
{{ include "farm.fullname" $root }}-public-tls
{{- end }}
{{- else if $root.Values.tls.certManager.enabled -}}
{{ include "farm.fullname" $root }}-{{ get (dict "lb" "lb-tls" "worker" "worker-tls" "clientCA" "ca") $part }}
{{- else -}}
{{ (get $root.Values.tls $part).existingSecret }}
{{- end }}
{{- end }}

{{/* One pod per node. spread: true prefers it, "required" insists on it.
     Tainted nodes the pods can't run on don't count as empty ones.
     Only pods of the same rollout count, so old pods that are still
     draining don't affect where the new ones go.
     Call with (dict "spread" <value> "selector" <matchLabels YAML>). */}}
{{- define "farm.spread" -}}
{{- if .spread }}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: {{ eq (toString .spread) "required" | ternary "DoNotSchedule" "ScheduleAnyway" }}
    nodeTaintsPolicy: Honor
    matchLabelKeys: [pod-template-hash]
    labelSelector:
      matchLabels: {{- .selector | nindent 8 }}
{{- end }}
{{- end }}
