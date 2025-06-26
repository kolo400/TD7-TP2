{{ config(materialized='view') }}

-- Ranking de políticos por cantidad de votos en cada elección
with conteo as (
  select
    pel.id_eleccion,
    p.nombre as politico,
    count(*) as votos_recibidos
  from {{ source('electoral_db','voto_eleccion_legislativa') }} vel
  join {{ source('electoral_db','politico_eleccion_pertenece_partido') }} pel
    on vel.num_voto = pel.num_voto
   and vel.id_eleccion = pel.id_eleccion
  join {{ source('electoral_db','politico') }} p
    on pel.dni_politico = p.dni_politico
  group by pel.id_eleccion, p.nombre
)
select
  id_eleccion,
  politico,
  votos_recibidos,
  row_number() over (partition by id_eleccion order by votos_recibidos desc) as ranking
from conteo;