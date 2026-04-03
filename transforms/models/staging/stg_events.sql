select
  cast(event_id as varchar) as event_id,
  cast(customer_id as varchar) as customer_id,
  cast(device_id as varchar) as device_id,
  cast(event_ts as timestamp) as event_ts
from {{ source('bronze', 'events') }}

