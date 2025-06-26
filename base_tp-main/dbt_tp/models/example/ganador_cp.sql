{{ config(materialized='view') }}

-- Opción ganadora en cada consulta popular
select
  id_eleccion,
  id_opcion as ganador_opcion,
  respuesta as ganador_respuesta,
  pct_respuesta
from (
  select
    id_eleccion,
    id_opcion,
    respuesta,
    pct_respuesta,
    row_number() over (partition by id_eleccion order by pct_respuesta desc) as rn
  from {{ ref('porcentaje_respuestas_por_cp') }}
) sub
where rn = 1;