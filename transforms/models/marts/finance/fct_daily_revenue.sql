{{ config(unique_key='revenue_sk') }}

select
  {{ generate_surrogate_key(['event_date', 'merchant_category']) }} as revenue_sk,
  event_date as revenue_date,
  merchant_category,
  count(*) as transaction_count,
  sum(amount) as gross_revenue,
  sum(case when status = 'approved' then amount else 0 end) as approved_revenue
from {{ ref('int_customer_transactions') }}
group by 1, 2, 3

