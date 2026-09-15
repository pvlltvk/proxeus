{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "chart.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "chart.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "chart.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "chart.labels" -}}
helm.sh/chart: {{ include "chart.chart" . }}
{{ include "chart.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "chart.selectorLabels" -}}
app.kubernetes.io/name: {{ include "chart.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "chart.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "chart.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Defins the name of configuratiob map
*/}}
{{- define "chart.configname" -}}
{{- if .Values.configMap -}}
{{- .Values.configMap -}}
{{- else -}}
{{- include "chart.fullname" . -}}-config
{{- end -}}
{{- end -}}

{{- define "split-host-port" -}}
{{- $hp := split ":" . -}}
{{- printf "%s" $hp._1 -}}
{{- end -}}

{{/*
Image reference, digest taking precedence over tag
*/}}
{{- define "chart.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{/*
Value of a flag the chart renders itself: the extraArgs entry when the user set
one, else the chart's own value. Keeps an override from being passed twice.
Call as: include "chart.flag" (list . "http.shutdown-delay" .Values.shutdownDelay)
*/}}
{{- define "chart.flag" -}}
{{- $ctx := index . 0 -}}
{{- $name := index . 1 -}}
{{- $fallback := index . 2 -}}
{{- $override := index (default dict $ctx.Values.extraArgs) $name -}}
{{- if $override -}}
{{- $override -}}
{{- else -}}
{{- $fallback -}}
{{- end -}}
{{- end -}}

{{/*
Seconds in a go-duration string, for the arithmetic behind
terminationGracePeriodSeconds. Only the integer+unit forms the shutdown flags
are given in practice parse; anything else is a hard error rather than a
silently wrong grace period.
*/}}
{{- define "chart.durationSeconds" -}}
{{- $d := toString . -}}
{{- $n := regexFind "^[0-9]+" $d -}}
{{- $unit := trimPrefix $n $d -}}
{{- $units := dict "s" 1 "m" 60 "h" 3600 -}}
{{- if or (not $n) (not (hasKey $units $unit)) -}}
{{- fail (printf "cannot parse duration %q: expected an integer followed by s, m or h" $d) -}}
{{- end -}}
{{- mul (atoi $n) (get $units $unit) -}}
{{- end -}}

{{/*
Grace period long enough for proxeus' own shutdown: it keeps serving for
shutdown-delay so load balancers notice it going away, then drains in-flight
requests for up to shutdown-timeout. Kubernetes' 30s default SIGKILLs that.
*/}}
{{- define "chart.terminationGracePeriodSeconds" -}}
{{- if .Values.terminationGracePeriodSeconds -}}
{{- .Values.terminationGracePeriodSeconds -}}
{{- else -}}
{{- $delay := include "chart.durationSeconds" (include "chart.flag" (list . "http.shutdown-delay" .Values.shutdownDelay)) | int -}}
{{- $timeout := include "chart.durationSeconds" (include "chart.flag" (list . "http.shutdown-timeout" .Values.shutdownTimeout)) | int -}}
{{- add $delay $timeout 5 -}}
{{- end -}}
{{- end -}}

{{/*
Name of the claim backing storage.path
*/}}
{{- define "chart.storageClaimName" -}}
{{- if .Values.storage.persistence.existingClaim -}}
{{- .Values.storage.persistence.existingClaim -}}
{{- else -}}
{{- include "chart.fullname" . -}}-storage
{{- end -}}
{{- end -}}

{{/*
Volumes shared by proxeus, the config-check init container and the reloader
*/}}
{{- define "chart.volumes" -}}
- name: config
  configMap:
    name: {{ include "chart.configname" . }}
- name: storage
{{- if .Values.storage.persistence.enabled }}
  persistentVolumeClaim:
    claimName: {{ include "chart.storageClaimName" . }}
{{- else if .Values.storage.emptyDir }}
  emptyDir:
    {{- toYaml .Values.storage.emptyDir | nindent 4 }}
{{- else }}
  emptyDir: {}
{{- end }}
{{- if .Values.webConfig.existingSecret }}
- name: web-config
  secret:
    secretName: {{ .Values.webConfig.existingSecret }}
{{- end }}
{{- range .Values.extraSecretMounts }}
- name: {{ .name }}
  secret:
    secretName: {{ .secretName }}
    {{- if .defaultMode }}
    defaultMode: {{ .defaultMode }}
    {{- end }}
    {{- if .optional }}
    optional: {{ .optional }}
    {{- end }}
{{- end }}
{{- with .Values.extraVolumes }}
{{- toYaml . | nindent 0 }}
{{- end }}
{{- end -}}

{{/*
Mounts matching chart.volumes
*/}}
{{- define "chart.volumeMounts" -}}
- mountPath: "/etc/proxeus/"
  name: config
  readOnly: true
- mountPath: {{ .Values.storage.path | quote }}
  name: storage
{{- if .Values.webConfig.existingSecret }}
- mountPath: "/etc/proxeus/web/"
  name: web-config
  readOnly: true
{{- end }}
{{- range .Values.extraSecretMounts }}
- mountPath: {{ .mountPath | quote }}
  name: {{ .name }}
  readOnly: {{ .readOnly | default true }}
{{- end }}
{{- with .Values.extraVolumeMounts }}
{{- toYaml . | nindent 0 }}
{{- end }}
{{- end -}}

{{/*
proxeus' arguments. The flags the chart owns are skipped when extraArgs
already sets them, so a user override replaces them instead of duplicating.
*/}}
{{- define "chart.args" -}}
{{- $extraArgs := default dict .Values.extraArgs -}}
- "--config=/etc/proxeus/config.yaml"
{{- if not (hasKey $extraArgs "storage.path") }}
- "--storage.path={{ .Values.storage.path }}"
{{- end }}
{{- if not (hasKey $extraArgs "http.shutdown-delay") }}
- "--http.shutdown-delay={{ .Values.shutdownDelay }}"
{{- end }}
{{- if not (hasKey $extraArgs "http.shutdown-timeout") }}
- "--http.shutdown-timeout={{ .Values.shutdownTimeout }}"
{{- end }}
{{- if .Values.webLifecycle }}
- "--web.enable-lifecycle"
{{- end }}
{{- if .Values.webConfig.existingSecret }}
- "--web.config.file=/etc/proxeus/web/{{ .Values.webConfig.key }}"
{{- end }}
{{- if .Values.mcp.enabled }}
- "--mcp.enable"
- "--mcp.max-series={{ .Values.mcp.maxSeries }}"
- "--mcp.max-samples={{ .Values.mcp.maxSamples }}"
- "--mcp.query-timeout={{ .Values.mcp.queryTimeout }}"
{{- end }}
{{- range $key, $value := $extraArgs }}
- "--{{ $key }}={{ $value }}"
{{- end }}
{{- end -}}

{{/*
Refuse the combinations that are broken or unsafe instead of shipping them.
*/}}
{{- define "chart.validate" -}}
{{- if and .Values.configmapReloader.enabled (not .Values.webLifecycle) -}}
{{- fail "configmapReloader.enabled needs webLifecycle: true -- without it /-/reload is not served and the sidecar reloads nothing" -}}
{{- end -}}
{{- if .Values.mcp.enabled -}}
{{- $authenticated := or .Values.mcp.authenticatedByProxy .Values.webConfig.existingSecret (dig "proxeus" "auth" false (default dict .Values.config)) -}}
{{- if not $authenticated -}}
{{- fail "mcp.enabled requires authentication: set config.proxeus.auth or webConfig.existingSecret, or mcp.authenticatedByProxy: true when something in front of proxeus authenticates" -}}
{{- end -}}
{{- end -}}
{{- if and .Values.hpa.enabled .Values.verticalAutoscaler.enabled (not (eq (default "Off" .Values.verticalAutoscaler.updateMode) "Off")) -}}
{{- fail "hpa.enabled with an actuating verticalAutoscaler.updateMode fights itself: both resize the same Deployment on the same CPU signal. Keep updateMode: \"Off\" to use the VPA for recommendations only" -}}
{{- end -}}
{{- if and .Values.storage.persistence.enabled (gt (int .Values.replicaCount) 1) (not (has "ReadWriteMany" .Values.storage.persistence.accessModes)) -}}
{{- fail "storage.persistence with replicaCount > 1 needs a ReadWriteMany access mode -- every replica mounts the same claim" -}}
{{- end -}}
{{- end -}}
