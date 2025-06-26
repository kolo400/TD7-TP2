{{ config(
    materialized='incremental',
    unique_key='id_eleccion'
) }}

-- % de cada respuesta en consultas populares
with conteo as (
  select
    vcp.id_eleccion,
    vcp.id_opcion,
    count(*) as votos
  from {{ source('electoral_db','voto_elije_opcion_respuesta') }} vcp
  group by vcp.id_eleccion, vcp.id_opcion
)
select
  c.id_eleccion,
  c.id_opcion,
  orp.respuesta,
  c.votos,
  round(100.0 * c.votos / sum(c.votos) over (partition by c.id_eleccion), 2) as pct_respuesta
from conteo c
join {{ source('electoral_db','opcion_respuesta') }} orp
  on c.id_opcion = orp.id_opcion;