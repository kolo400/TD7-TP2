
/*
{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)


select *
from source_data

where id is not null
*/


{{ config(
    materialized='table',
    description='Partido ganador por provincia'
) }}

with resultados_provincia as (
    select 
        r.provincia,
        p.nombre_partido,
        sum(r.cantidad_votos) as total_votos,
        row_number() over (partition by r.provincia order by sum(r.cantidad_votos) desc) as ranking
    from {{ source('raw_election_data', 'raw_results') }} r
    join {{ source('raw_election_data', 'raw_parties') }} p
        on r.id_partido = p.id_partido
    group by r.provincia, p.nombre_partido
)

select 
    provincia,
    nombre_partido as partido_ganador,
    total_votos as cantidad_votos
from resultados_provincia
where ranking = 1




