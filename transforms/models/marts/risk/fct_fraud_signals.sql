{{ config(unique_key='fraud_signal_sk') }}

select
  {{ generate_surrogate_key(['event_date', 'account_id']) }} as fraud_signal_sk,
  event_date,
  account_id,
  count(*) as transaction_count,
  sum(case when is_negative_outcome then 1 else 0 end) as negative_outcome_count,
  avg(risk_score) as avg_risk_score
from {{ ref('int_risk_flags') }}
group by 1, 2, 3

