-- Porcentaje de votos en blanco por elección legislativa
with total_leg as (
  select id_eleccion, count(*) as total_votos
  from {{ source('electoral_db','voto_eleccion_legislativa') }}
  group by id_eleccion
), blancos as (
  select
    vcp.id_eleccion,
    count(*) as blancos
  from {{ source('electoral_db','voto_consulta_popular') }} vcp
  join {{ source('electoral_db','opcion_respuesta') }} orp
    on vcp.id_opcion = orp.id_opcion
  where orp.respuesta = 'Blanco'
  group by vcp.id_eleccion
)
select
  t.id_eleccion,
  round(100.0 * coalesce(b.blancos, 0) / t.total_votos, 2) as pct_votos_blanco
from total_leg t
left join blancos b on t.id_eleccion = b.id_eleccion;{{ config(materialized='table') }}

-- Porcentaje de votos en blanco por elección legislativa
with total_leg as (
  select id_eleccion, count(*) as total_votos
  from {{ source('electoral_db','voto_eleccion_legislativa') }}
  group by id_eleccion
), blancos as (
  select
    vcp.id_eleccion,
    count(*) as blancos
  from {{ source('electoral_db','voto_consulta_popular') }} vcp
  join {{ source('electoral_db','opcion_respuesta') }} orp
    on vcp.id_opcion = orp.id_opcion
  where orp.respuesta = 'Blanco'
  group by vcp.id_eleccion
)
select
  t.id_eleccion,
  round(100.0 * coalesce(b.blancos, 0) / t.total_votos, 2) as pct_votos_blanco
from total_leg t
left join blancos b on t.id_eleccion = b.id_eleccion;