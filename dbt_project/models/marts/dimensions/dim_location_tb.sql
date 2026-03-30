select distinct
    lane as location_id,
    initcap(pickup_city) as pickup_city,
    upper(pickup_state) as pickup_state,
    initcap(delivery_city) as delivery_city,
    upper(delivery_state) as delivery_state
from {{ ref('int_loads_enriched') }}
