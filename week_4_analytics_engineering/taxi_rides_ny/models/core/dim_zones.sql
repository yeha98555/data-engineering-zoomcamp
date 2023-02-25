{{
  config(
    materialized = 'table',
    )
}}

select
    locationid,
    borough
    zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref('tax_zone_lookup') }}