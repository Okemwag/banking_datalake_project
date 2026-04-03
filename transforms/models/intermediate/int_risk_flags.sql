select
  transaction_id,
  customer_id,
  account_id,
  event_date,
  risk_score,
  case
    when risk_score >= 0.85 then 'high'
    when risk_score >= 0.60 then 'medium'
    else 'low'
  end as risk_band,
  case
    when status in ('declined', 'chargeback') then true
    else false
  end as is_negative_outcome
from {{ ref('stg_transactions') }}

