{{ config(materialized='table') }}

select distinct
  customer_id,
  customer_segment,
  country_code
from {{ ref('int_customer_transactions') }}

