select
  t.transaction_id,
  t.customer_id,
  c.segment as customer_segment,
  c.country_code,
  t.account_id,
  t.amount,
  t.status,
  t.event_date,
  t.merchant_category,
  t.payment_channel,
  t.risk_score
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_customers') }} c
  on t.customer_id = c.customer_id

