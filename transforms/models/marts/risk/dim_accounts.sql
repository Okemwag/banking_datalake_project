{{ config(materialized='table') }}

select distinct
  account_id,
  customer_id
from {{ ref('stg_transactions') }}

