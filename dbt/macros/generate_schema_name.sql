{#
  dbt's default is <profile_schema>_<custom_schema>, which would give us
  RAW_staging and RAW_marts. We want the custom schema used verbatim, so
  models land in the STAGING and MARTS schemas created in setup.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
