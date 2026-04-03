select *
from {{ ref('fct_daily_revenue') }}
where gross_revenue < 0

