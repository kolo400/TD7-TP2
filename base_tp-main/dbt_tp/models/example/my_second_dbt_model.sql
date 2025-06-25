select *
from {{ ref('my_first_dbt_model') }}
where id = 1

{{ config(
    materialized='view',
    description='Top 5 partidos a nivel nacional'
) }}

with resultados_nacionales as (
    select 
        p.nombre_partido,
        sum(r.cantidad_votos) as total_votos,
        row_number() over (order by sum(r.cantidad_votos) desc) as ranking
    from {{ source('raw_election_data', 'raw_results') }} r
    join {{ source('raw_election_data', 'raw_parties') }} p
        on r.id_partido = p.id_partido
    group by p.nombre_partido
)

select 
    ranking,
    nombre_partido as partido,
    total_votos
from resultados_nacionales
where ranking <= 5
