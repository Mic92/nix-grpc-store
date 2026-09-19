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
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
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
{{- if gt (int (.Values.scheduler.replicas | default 1)) 1 -}}
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
{{- if and $tok.existingSecret $tok.serviceAccountToken.enabled }}{{ fail "set only one of niks3.auth.existingSecret and niks3.auth.serviceAccountToken.enabled" }}{{ end }}
{{- if not (or $tok.existingSecret $tok.serviceAccountToken.enabled) }}{{ fail "set niks3.auth.existingSecret or niks3.auth.serviceAccountToken.enabled" }}{{ end }}
{{- if empty .Values.workers }}{{ fail "workers is empty, add e.g. workers.x86-64: {system: x86_64-linux, replicas: 2}" }}{{ end }}
{{- if and (or .Values.auth.accessRules .Values.auth.anonymousRole) (not (or .Values.tls.clientCA.existingSecret (include "farm.oidcProviders" .))) }}{{ fail "auth.accessRules/anonymousRole need tls.clientCA or an OIDC provider" }}{{ end }}
{{- if and .Values.tls.clientCA.existingSecret (not .Values.tls.worker.existingSecret) }}{{ fail "tls.clientCA requires tls.worker (workers verify the balancer over TLS)" }}{{ end }}
{{- if and .Values.lb.enabled .Values.tls.clientCA.existingSecret (not .Values.tls.lb.existingSecret) }}{{ fail "tls.clientCA requires tls.lb (the balancer presents it to clients and workers)" }}{{ end }}
{{- with .Values.defaultGroup }}{{ if not (hasKey $.Values.workers .) }}{{ fail (printf "defaultGroup %q is not a key of workers" .) }}{{ end }}{{ end }}
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
