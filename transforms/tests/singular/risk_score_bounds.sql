select *
from {{ ref('int_risk_flags') }}
where risk_score < 0 or risk_score > 1

