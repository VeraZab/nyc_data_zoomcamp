select * from {{ source('staging', 'fhv_rides') }}
limit 100