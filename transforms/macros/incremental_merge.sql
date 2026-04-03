{% macro incremental_merge(unique_key) -%}
  {{ return({'incremental_strategy': 'merge', 'unique_key': unique_key}) }}
{%- endmacro %}

