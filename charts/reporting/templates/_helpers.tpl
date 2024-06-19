{{/*
Render Env values section
*/}}
{{- define "reporting.baseEnvVars" -}}
{{- $context := .context -}}
{{- range $k, $v := .envVars }}
- name: {{ $k }}
{{- if or (kindIs "int64" $v) (kindIs "float64" $v) (kindIs "bool" $v) }}
  value: {{ $v | quote }}
{{- else if kindIs "string" $v }}
  value: {{ include "common.tplvalues.render" ( dict "value" $v "context" $context ) | squote }}
{{- else }}
  valueFrom: {{- include "common.tplvalues.render" ( dict "value" $v "context" $context ) | nindent 4}}
{{- end }}
{{- end }}
{{- end -}}

{{- define "reporting.kafka-ui.envVars" -}}
{{- $envVars := merge (deepCopy .Values.kafkaUi.envVars) (deepCopy .Values.kafkaUi.envVarsFrom) -}}
{{- include "reporting.baseEnvVars" (dict "envVars" $envVars "context" $) }}
{{- end -}}
