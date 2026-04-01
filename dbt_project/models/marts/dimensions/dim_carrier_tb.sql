select distinct
    carrier_name as carrier_id,
    upper(carrier_name) as carrier_name,
    -- carrier_rating, -- appears to be point in time, not a dimension attribute
    vip_carrier
from {{ ref('int_loads_enriched') }}
where carrier_name is not null