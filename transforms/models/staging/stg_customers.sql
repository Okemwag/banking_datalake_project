select
  cast(customer_id as varchar) as customer_id,
  cast(full_name as varchar) as full_name,
  cast(segment as varchar) as segment,
  cast(country_code as varchar) as country_code,
  cast(email as varchar) as email
from {{ source('bronze', 'customers') }}

