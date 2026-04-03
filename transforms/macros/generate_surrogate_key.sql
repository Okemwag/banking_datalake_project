{% macro generate_surrogate_key(columns) -%}
  md5({%- for column in columns -%}
    cast({{ column }} as varchar)
    {%- if not loop.last %} || '|' || {% endif -%}
  {%- endfor -%})
{%- endmacro %}

