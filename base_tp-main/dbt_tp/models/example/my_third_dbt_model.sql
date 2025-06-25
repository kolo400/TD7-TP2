{{ config(
    materialized='view',
    description='Participación electoral por franja etaria'
) }}

with votantes_por_edad as (
    select 
        case 
            when age(current_date, fecha_nacimiento) < interval '25 years' then '18-25'
            when age(current_date, fecha_nacimiento) < interval '40 years' then '26-40'
            when age(current_date, fecha_nacimiento) < interval '60 years' then '41-60'
            else '60+'
        end as rango_edad,
        count(*) as cantidad_votantes
    from {{ source('raw_election_data', 'raw_voters') }}
    group by 1
),
total_votantes as (
    select count(*) as total
    from {{ source('raw_election_data', 'raw_voters') }}
)

select 
    v.rango_edad,
    v.cantidad_votantes,
    round((v.cantidad_votantes::float / t.total) * 100, 2) as porcentaje_participacion
from votantes_por_edad v
cross join total_votantes t
order by v.rango_edad
