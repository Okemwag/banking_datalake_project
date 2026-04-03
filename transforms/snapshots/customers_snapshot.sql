{% snapshot customers_snapshot %}
  {{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_segment', 'country_code']
    )
  }}

  select *
  from {{ ref('dim_customers') }}
{% endsnapshot %}

