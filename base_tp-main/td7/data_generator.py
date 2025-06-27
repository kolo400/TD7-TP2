import datetime
import random
from faker import Faker
from faker.providers import address, date_time, internet, phone_number
import uuid
from typing import List, Dict, Any, Optional

from td7.custom_types import Records

class DataGenerator:
    def __init__(self):
        """Instantiates faker instance"""
        self.fake = Faker()
        self.fake.add_provider(address)
        self.fake.add_provider(date_time)
        self.fake.add_provider(internet)
        self.fake.add_provider(phone_number)

    def generate_elector(self, n: int) -> Records:
        """Genera n electores para el sistema.

        Parameters
        ----------
        n : int
            Número de electores a generar.

        Returns
        -------
        List[Dict[str, Any]]
            Lista de diccionarios con información de electores.
        """
        electores = []
        provincias = ['Buenos Aires', 'CABA', 'Córdoba', 'Santa Fe', 'Mendoza']
        for _ in range(n):
            electores.append({
                "dni": self.fake.unique.numerify(text='########'),
                "nombre": self.fake.unique.first_name(),
                "apellido": self.fake.unique.last_name(),
                "fecha_nacimiento": self.fake.date_of_birth(minimum_age=18, maximum_age=90),
                "calle": self.fake.street_name(),
                "altura": random.randint(1, 9999),
                "provincia": random.choice(provincias),
                "codigo_postal": self.fake.numerify(text='####')
            })
        return electores

    def generate_maquina_votos(self, n: int) -> Records:
        """Genera n máquinas de votación."""
        maquinas = []
        for _ in range(n):
            maquinas.append({
                "numero_serie": f"MVM{self.fake.unique.numerify(text='#######')}",
                "info_hardware": f"Modelo {random.choice(['A', 'B', 'C'])}-{random.randint(1, 5)}",
                "info_software": f"v{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
            })
        return maquinas

    def generate_centro_votacion(self, n: int) -> Records:
        """Genera n centros de votación."""
        centros = []
        provincias = ['Buenos Aires', 'CABA', 'Córdoba', 'Santa Fe', 'Mendoza']
        for _ in range(n):
            centros.append({
                "id_centro": f"CV{self.fake.unique.numerify(text='#######')}",
                "nombre": f"Centro de Votación {self.fake.unique.word().capitalize()}",
                "calle": self.fake.street_name(),
                "altura": random.randint(1, 9999),
                "provincia": random.choice(provincias),
                "codigo_postal": self.fake.numerify(text='####')
            })
        return centros

    def generate_eleccion(self, n: int) -> Records:
        """Genera n elecciones base."""
        elecciones = []
        territorios = ['Nacional', 'Provincial', 'Municipal']
        for _ in range(n):
            elecciones.append({
                "id_eleccion": f"EL{self.fake.unique.numerify(text='#######')}",
                "fecha_eleccion": self.fake.date_between(start_date='+1d', end_date='+1y'),
                "territorio": random.choice(territorios)
            })
        return elecciones

    def generate_eleccion_typed(self, n: int) -> tuple[Records, Records, Records]:
        """
        Genera n elecciones base y las asigna aleatoriamente como legislativas o consultas populares,
        de modo que la suma total de elecciones legislativas y de consultas populares sea igual a n,
        y cada elección sea de un solo tipo. Los id_eleccion de las tablas de tipo SIEMPRE provienen de la base.
        """
        elecciones = self.generate_eleccion(n)
        elecciones_legislativas = []
        consultas_populares = []
        tipos = ["legislativa"] * (n // 2) + ["consulta"] * (n - (n // 2))
        random.shuffle(tipos)
        for eleccion, tipo in zip(elecciones, tipos):
            if tipo == "legislativa":
                elecciones_legislativas.append({
                    "id_eleccion": eleccion["id_eleccion"],
                    "cargo": random.choice(["Diputado", "Senador", "Concejal"])
                })
            else:
                consultas_populares.append({
                    "id_eleccion": eleccion["id_eleccion"],
                    "pregunta": random.choice([
                        "¿Está de acuerdo con la implementación de X?",
                        "¿Aprueba la modificación de Y?",
                        "¿Prefiere la opción A o B?"
                    ])
                })
        return elecciones, elecciones_legislativas, consultas_populares

    def generate_eleccion_legislativa(self, elecciones: Records) -> Records:
        """Genera elecciones legislativas a partir de elecciones base."""
        legislativas = []
        cargos = ['Diputado', 'Senador', 'Concejal']
        for eleccion in elecciones:
            legislativas.append({
                "id_eleccion": eleccion["id_eleccion"],
                "cargo": random.choice(cargos)
            })
        return legislativas

    def generate_consulta_popular(self, elecciones: Records) -> Records:
        """Genera consultas populares a partir de elecciones base."""
        consultas = []
        preguntas = [
            "¿Está de acuerdo con la implementación de X?",
            "¿Aprueba la modificación de Y?",
            "¿Prefiere la opción A o B?"
        ]
        for eleccion in elecciones:
            consultas.append({
                "id_eleccion": eleccion["id_eleccion"],
                "pregunta": random.choice(preguntas)
            })
        return consultas

    def generate_mesa_electoral(self, centros: Records, elecciones: Records) -> Records:
        """Genera mesas electorales con sus integrantes."""
        mesas = []
        for centro in centros:
            for eleccion in elecciones:
                n_mesas = random.randint(1, 5)
                for i in range(n_mesas):
                    mesas.append({
                        "nro_mesa": f"M{i+1}",
                        "id_centro": centro["id_centro"],
                        "id_eleccion": eleccion["id_eleccion"]
                    })
        return mesas

    def generate_mesa_utiliza_maquina(self, mesas: Records, maquinas: Records) -> Records:
        """Genera la relación entre mesas y máquinas de votación."""
        relaciones = []
        for mesa in mesas:
            n_maquinas = random.randint(1, 3)
            selected_maquinas = random.sample(maquinas, min(n_maquinas, len(maquinas)))
            for maquina in selected_maquinas:
                relaciones.append({
                    "nro_mesa": mesa["nro_mesa"],
                    "id_centro": mesa["id_centro"],
                    "id_eleccion": mesa["id_eleccion"],
                    "numero_serie": maquina["numero_serie"]
                })
        return relaciones

    def generate_padron_eleccion(self,electores: Records,mesas: Records,elecciones: Records) -> dict:
        """
        Streaming: genera e inserta UN registro de padrón electoral.
        """
        if not electores or not mesas or not elecciones:
            return {}

        # Elijo un elector, una mesa y tomo la elección implícita en la mesa
        elector = random.choice(electores)
        mesa     = random.choice(mesas)

        registro = {
            "dni_elector": elector["dni"],
            "id_eleccion": mesa["id_eleccion"],
            "nro_mesa":    mesa["nro_mesa"],
            "id_centro":   mesa["id_centro"],
            "si_voto":     (random.random() < 0.8),
        }
        return registro

    def generate_padron_eleccion_smart(self, electores: Records, mesas: Records, elecciones: Records, padrones_existentes: Records) -> dict:
        """
        Genera un padrón electoral de forma inteligente, evitando duplicados.
        
        Parameters
        ----------
        electores : Records
            Lista de electores disponibles
        mesas : Records
            Lista de mesas disponibles
        elecciones : Records
            Lista de elecciones disponibles
        padrones_existentes : Records
            Lista de padrones ya existentes (con dni_elector e id_eleccion)
            
        Returns
        -------
        dict
            Registro de padrón único o {} si no se puede generar
        """
        if not electores or not mesas or not elecciones:
            return {}

        # Crear set de combinaciones existentes para búsqueda rápida
        combinaciones_existentes = set()
        for padron in padrones_existentes:
            combinaciones_existentes.add((padron['dni_elector'], padron['id_eleccion']))
        
        # Para cada elección, encontrar electores disponibles
        for eleccion in elecciones:
            electores_disponibles = []
            for elector in electores:
                combinacion = (elector['dni'], eleccion['id_eleccion'])
                if combinacion not in combinaciones_existentes:
                    electores_disponibles.append(elector)
            
            # Si hay electores disponibles para esta elección
            if electores_disponibles:
                # Elegir un elector disponible
                elector = random.choice(electores_disponibles)
                
                # Encontrar una mesa para esta elección
                mesas_eleccion = [m for m in mesas if m['id_eleccion'] == eleccion['id_eleccion']]
                if mesas_eleccion:
                    mesa = random.choice(mesas_eleccion)
                    
                    registro = {
                        "dni_elector": elector["dni"],
                        "id_eleccion": eleccion["id_eleccion"],
                        "nro_mesa":    mesa["nro_mesa"],
                        "id_centro":   mesa["id_centro"],
                        "si_voto":     (random.random() < 0.8),
                    }
                    return registro
        
        # Si no se pudo generar ningún padrón único
        return {}

    def generate_partido_politico(self, n: int) -> Records:
        """Genera n partidos políticos."""
        partidos = []
        for _ in range(n):
            partidos.append({
                "id_partido": f"PP{self.fake.unique.numerify(text='#######')}",
                "nombre": f"Partido {self.fake.unique.word().capitalize()}"
            })
        return partidos

    def generate_politico(self, n: int) -> Records:
        """Genera n políticos."""
        politicos = []
        for _ in range(n):
            politicos.append({
                "dni_politico": self.fake.unique.numerify(text='########'),
                "nombre": self.fake.unique.first_name(),
                "apellido": self.fake.unique.last_name()
            })
        return politicos

    def generate_candidato(self, politicos: Records, elecciones_legislativas: Records) -> Records:
        """Genera candidatos para las elecciones legislativas."""
        candidatos = []
        for eleccion in elecciones_legislativas:
            n_candidatos = random.randint(3, 8)
            selected_politicos = random.sample(politicos, min(n_candidatos, len(politicos)))
            for politico in selected_politicos:
                candidatos.append({
                    "dni_politico": politico["dni_politico"],
                    "id_eleccion": eleccion["id_eleccion"]
                })
        return candidatos

    def generate_politico_eleccion_pertenece_partido(self, candidatos: Records, partidos: Records) -> Records:
        """Genera la relación entre políticos, elecciones y partidos."""
        relaciones = []
        for candidato in candidatos:
            relaciones.append({
                "dni_politico": candidato["dni_politico"],
                "id_eleccion": candidato["id_eleccion"],
                "id_partido": random.choice(partidos)["id_partido"]
            })
        return relaciones


    def generate_opcion_respuesta(self, n: int) -> Records:
        """Genera n opciones de respuesta para consultas populares."""
        opciones = []
        for _ in range(n):
            opciones.append({
                "id_opcion": f"OP{self.fake.unique.numerify(text='#######')}",
                "respuesta": f"Opción {self.fake.unique.word().capitalize()}"
            })
        return opciones

    def generate_cp_tiene_opcion_respuesta(self, consultas: Records, opciones: Records) -> Records:
        """Genera la relación entre consultas populares y opciones de respuesta."""
        relaciones = []
        for consulta in consultas:
            n_opciones = random.randint(2, 4)
            selected_opciones = random.sample(opciones, min(n_opciones, len(opciones)))
            for opcion in selected_opciones:
                relaciones.append({
                    "id_eleccion": consulta["id_eleccion"],
                    "id_opcion": opcion["id_opcion"]
                })
        return relaciones

    def generate_voto(self, padron: Records, mesas_maquinas: Records, n_votos: int) -> Records:
        """Genera votos para las elecciones."""
        votos = []
        selected_padron = random.sample(padron, min(n_votos, len(padron)))
        for registro in selected_padron:
            # Encontrar una máquina asignada a la mesa del votante
            maquinas_mesa = [m for m in mesas_maquinas 
                           if m["nro_mesa"] == registro["nro_mesa"] 
                           and m["id_centro"] == registro["id_centro"]
                           and m["id_eleccion"] == registro["id_eleccion"]]
            
            if maquinas_mesa:
                maquina = random.choice(maquinas_mesa)
                tipo = random.choice(["legislativa", "consulta"])
                votos.append({
                    "num_voto": f"V{self.fake.unique.numerify(text='#######')}",
                    "id_eleccion": registro["id_eleccion"],
                    "nro_mesa": registro["nro_mesa"],
                    "numero_serie": maquina["numero_serie"],
                    "id_centro": registro["id_centro"],
                    "ts": self.fake.date_time_between(start_date='-1d', end_date='now'),
                    "tipo": tipo
                })

        return votos

    def generate_voto_eleccion_legislativa(self, votos: Records, candidatos: Records) -> Records:
        """Genera votos para elecciones legislativas."""
        votos_legislativos = []
        for voto in votos:
            # Encontrar candidatos para esta elección
            candidatos_eleccion = [c for c in candidatos if c["id_eleccion"] == voto["id_eleccion"]]
            if candidatos_eleccion:
                votos_legislativos.append({
                    "num_voto": voto["num_voto"],
                    "id_eleccion": voto["id_eleccion"]
                })
        return votos_legislativos

    def generate_voto_consulta_popular(self, votos: Records, consultas: Records) -> Records:
        """Genera votos para consultas populares."""
        votos_consultas = []
        for voto in votos:
            # Verificar si es una consulta popular
            if any(c["id_eleccion"] == voto["id_eleccion"] for c in consultas):
                votos_consultas.append({
                    "num_voto": voto["num_voto"],
                    "id_eleccion": voto["id_eleccion"]
                })
        return votos_consultas

    def generate_voto_elige_candidato(
        self,
        voto: dict,
        candidatos: Records
    ) -> dict:
        """
        Dado un voto ya insertado en voto_eleccion_legislativa,
        elige un candidato al azar de la misma elección.
        """
        # Filtramos candidatos de esa elección
        mismos = [c for c in candidatos if c["id_eleccion"] == voto["id_eleccion"]]
        if not mismos:
            return {}
        elegido = random.choice(mismos)
        return {
            "num_voto":     voto["num_voto"],
            "id_eleccion":  voto["id_eleccion"],
            "dni_politico": elegido["dni_politico"],
        }

    def generate_voto_elige_opcion_respuesta(
        self,
        voto: dict,
        opciones: Records
    ) -> dict:
        """
        Dado un voto ya insertado en voto_consulta_popular,
        elige una opción de respuesta al azar de la misma elección.
        """
        # Filtramos las opciones válidas para esa consulta
        mismas = [o for o in opciones if o["id_eleccion"] == voto["id_eleccion"]]
        if not mismas:
            return {}
        elegida = random.choice(mismas)
        return {
            "num_voto":    voto["num_voto"],
            "id_eleccion": voto["id_eleccion"],
            "id_opcion":   elegida["id_opcion"],
        }