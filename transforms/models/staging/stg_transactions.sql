select
  cast(transaction_id as varchar) as transaction_id,
  cast(customer_id as varchar) as customer_id,
  cast(account_id as varchar) as account_id,
  cast(amount as decimal(18, 2)) as amount,
  lower(status) as status,
  cast(event_ts_utc as timestamp) as event_ts_utc,
  cast(event_date as date) as event_date,
  lower(merchant_category) as merchant_category,
  lower(payment_channel) as payment_channel,
  cast(risk_score as double) as risk_score
from {{ source('silver', 'transactions_clean') }}

