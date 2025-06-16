/*==============================================================================
 * SISTEMA ELECTORAL - ESTRUCTURA DE LA BASE DE DATOS
 *==============================================================================*/

/*------------------------------------------------------------------------------
 * ENTIDADES PRINCIPALES
 *------------------------------------------------------------------------------*/

/* Personas del sistema */
CREATE TABLE ELECTOR (
    dni                 VARCHAR(20) PRIMARY KEY,
    nombre              VARCHAR(50),
    apellido            VARCHAR(50),
    fecha_nacimiento    DATE,
    calle               VARCHAR(100),
    altura              INTEGER,
    provincia           VARCHAR(50),
    codigo_postal       VARCHAR(50)
);

CREATE TABLE INTEGRANTE (
    dni                 VARCHAR(20) PRIMARY KEY,
    nombre              VARCHAR(50),
    apellido            VARCHAR(50)
);

/* Infraestructura física */
CREATE TABLE MAQUINA_VOTOS (
    numero_serie        VARCHAR(30) PRIMARY KEY,
    info_hardware       VARCHAR(100),
    info_software       VARCHAR(100)
);

CREATE TABLE CENTRO_VOTACION (
    id_centro           VARCHAR(20) PRIMARY KEY,
    nombre              VARCHAR(50),
    calle               VARCHAR(100),
    altura              INTEGER,
    provincia           VARCHAR(50),
    codigo_postal       VARCHAR(50)
);

/* Tipos de Integrantes Obligatorios */
CREATE TABLE TECNICO (
    dni                 VARCHAR(20) PRIMARY KEY,
    nivel_experiencia   VARCHAR(20),
    FOREIGN KEY (dni)   REFERENCES INTEGRANTE(dni)
);

CREATE TABLE PRESIDENTE (
    dni                 VARCHAR(20) PRIMARY KEY,
    FOREIGN KEY (dni)   REFERENCES INTEGRANTE(dni)
);

CREATE TABLE VICEPRESIDENTE (
    dni                 VARCHAR(20) PRIMARY KEY,
    FOREIGN KEY (dni)   REFERENCES INTEGRANTE(dni)
);

CREATE TABLE SUPLENTE (
    dni                 VARCHAR(20) PRIMARY KEY,
    FOREIGN KEY (dni)   REFERENCES INTEGRANTE(dni)
);

/*------------------------------------------------------------------------------
 * ELECCIONES Y TIPOS DE ELECCIONES
 *------------------------------------------------------------------------------*/

CREATE TABLE ELECCION (
    id_eleccion         VARCHAR(20) PRIMARY KEY,
    fecha_eleccion      DATE,
    territorio          VARCHAR(20) 
);

CREATE TABLE ELECCION_LEGISLATIVA (
    id_eleccion         VARCHAR(20) PRIMARY KEY,
    cargo               VARCHAR(50),
    FOREIGN KEY (id_eleccion) REFERENCES ELECCION(id_eleccion)
);

CREATE TABLE CONSULTA_POPULAR (
    id_eleccion         VARCHAR(20) PRIMARY KEY,
    pregunta            TEXT,
    FOREIGN KEY (id_eleccion) REFERENCES ELECCION(id_eleccion)
);

/*------------------------------------------------------------------------------
 * MESAS ELECTORALES Y MESAS QUE UTILIZAN MAQUINAS
 *------------------------------------------------------------------------------*/

CREATE TABLE MESA_ELECTORAL (
    nro_mesa             VARCHAR(20),
    id_centro           VARCHAR(20),
    id_eleccion         VARCHAR(20),
    dni_tecnico         VARCHAR(20) NOT NULL,
    dni_vicepresidente  VARCHAR(20) NOT NULL,
    dni_presidente      VARCHAR(20) NOT NULL,
    dni_suplente        VARCHAR(20) NOT NULL,
    PRIMARY KEY (nro_mesa, id_centro, id_eleccion),
    FOREIGN KEY (id_centro)          REFERENCES CENTRO_VOTACION(id_centro),
    FOREIGN KEY (id_eleccion)        REFERENCES ELECCION(id_eleccion),
    FOREIGN KEY (dni_tecnico)        REFERENCES TECNICO(dni),
    FOREIGN KEY (dni_vicepresidente) REFERENCES VICEPRESIDENTE(dni),
    FOREIGN KEY (dni_presidente)     REFERENCES PRESIDENTE(dni),
    FOREIGN KEY (dni_suplente)       REFERENCES SUPLENTE(dni)
);

CREATE TABLE MESA_UTILIZA_MAQUINA (
    nro_mesa             VARCHAR(20),
    id_centro           VARCHAR(20),
    id_eleccion         VARCHAR(20),
    numero_serie        VARCHAR(30),
    PRIMARY KEY (nro_mesa, id_centro, id_eleccion, numero_serie),
    FOREIGN KEY (nro_mesa, id_centro, id_eleccion)
        REFERENCES MESA_ELECTORAL(nro_mesa, id_centro, id_eleccion),
    FOREIGN KEY (numero_serie)
        REFERENCES MAQUINA_VOTOS(numero_serie)
);

/*------------------------------------------------------------------------------
 * PADRÓN ELECTORAL
 *------------------------------------------------------------------------------*/

CREATE TABLE PADRON_ELECCION ( 
    dni_elector         VARCHAR(20),
    id_eleccion         VARCHAR(20),
    nro_mesa             VARCHAR(20),
    id_centro           VARCHAR(20),
    si_voto             BOOLEAN,
    PRIMARY KEY (dni_elector, id_eleccion), 
    FOREIGN KEY (dni_elector) REFERENCES ELECTOR(dni),
    FOREIGN KEY (id_eleccion) REFERENCES ELECCION(id_eleccion),
    FOREIGN KEY (nro_mesa, id_centro, id_eleccion) 
        REFERENCES MESA_ELECTORAL(nro_mesa, id_centro, id_eleccion)
);

/*------------------------------------------------------------------------------
 * LOGISTICA (CAMIONETA Y RESPONSABLE)
 *------------------------------------------------------------------------------*/
 
CREATE TABLE RESPONSABLE (
    dni                 VARCHAR(20) PRIMARY KEY,
    nombre              VARCHAR(50),
    apellido            VARCHAR(50)
);

CREATE TABLE CAMIONETA (
    patente        VARCHAR(20) PRIMARY KEY,
    marca               VARCHAR(50),
    modelo              VARCHAR(50)
);

/* Relación camioneta-responsable */
CREATE TABLE CAMIONETA_RESPONSABLE (
    patente        VARCHAR(20),
    dni_responsable     VARCHAR(20),
    PRIMARY KEY (patente, dni_responsable),
    FOREIGN KEY (patente)    REFERENCES CAMIONETA(patente),
    FOREIGN KEY (dni_responsable) REFERENCES RESPONSABLE(dni)
);

CREATE TABLE CAMIONETA_CENTRO_ELECCION (
    patente        VARCHAR(20),
    id_eleccion         VARCHAR(20),
    id_centro           VARCHAR(20),
    PRIMARY KEY (patente, id_eleccion),
    FOREIGN KEY (patente) REFERENCES CAMIONETA(patente),
    FOREIGN KEY (id_eleccion)  REFERENCES ELECCION(id_eleccion),
    FOREIGN KEY (id_centro)    REFERENCES CENTRO_VOTACION(id_centro)
);

/*------------------------------------------------------------------------------
 * FISCALES Y PARTIDOS POLÍTICOS
 *------------------------------------------------------------------------------*/

CREATE TABLE FISCAL (
    dni                 VARCHAR(20) PRIMARY KEY,
    FOREIGN KEY (dni)   REFERENCES INTEGRANTE(dni)
);

CREATE TABLE MESA_FISCAL (
    dni_fiscal          VARCHAR(20),
    nro_mesa             VARCHAR(20),
    id_centro           VARCHAR(20),
    id_eleccion         VARCHAR(20),
    PRIMARY KEY (dni_fiscal, nro_mesa, id_centro, id_eleccion),
    FOREIGN KEY (dni_fiscal) REFERENCES FISCAL(dni),
    FOREIGN KEY (nro_mesa, id_centro, id_eleccion) 
        REFERENCES MESA_ELECTORAL(nro_mesa, id_centro, id_eleccion)
);

CREATE TABLE PARTIDO_POLITICO (
    id_partido          VARCHAR(20) PRIMARY KEY,
    nombre              VARCHAR(50)
);

CREATE TABLE FISCAL_PARTIDO (
    dni_fiscal          VARCHAR(20),
    id_partido          VARCHAR(20),
    PRIMARY KEY (dni_fiscal, id_partido),
    FOREIGN KEY (dni_fiscal) REFERENCES FISCAL(dni),
    FOREIGN KEY (id_partido) REFERENCES PARTIDO_POLITICO(id_partido)
);

/*------------------------------------------------------------------------------
 * POLÍTICOS Y CANDIDATURAS
 *------------------------------------------------------------------------------*/

CREATE TABLE POLITICO (
    dni_politico        VARCHAR(20) PRIMARY KEY,
    nombre              VARCHAR(50),
    apellido            VARCHAR(50)
);

CREATE TABLE CANDIDATO (
    dni_politico         VARCHAR(20),
    id_eleccion         VARCHAR(20),
    PRIMARY KEY (dni_politico, id_eleccion),
    FOREIGN KEY (dni_politico)  REFERENCES POLITICO(dni_politico),
    FOREIGN KEY (id_eleccion)  REFERENCES ELECCION_LEGISLATIVA(id_eleccion)
);

CREATE TABLE POLITICO_ELECCION_PERTENECE_PARTIDO (
    dni_politico         VARCHAR(20),
    id_eleccion         VARCHAR(20),
    id_partido          VARCHAR(20),
    PRIMARY KEY (dni_politico, id_eleccion),
    FOREIGN KEY (dni_politico, id_eleccion) 
        REFERENCES CANDIDATO(dni_politico, id_eleccion),
    FOREIGN KEY (id_partido) REFERENCES PARTIDO_POLITICO(id_partido)
);

/*------------------------------------------------------------------------------
 * SISTEMA DE VOTACIÓN
 *------------------------------------------------------------------------------*/

CREATE TABLE VOTO (
    num_voto            VARCHAR(20),
    id_eleccion         VARCHAR(20),
    nro_mesa             VARCHAR(20),
    numero_serie        VARCHAR(30),
    id_centro           VARCHAR(20),
    ts                  TIMESTAMP,
    PRIMARY KEY (num_voto, id_eleccion),
    FOREIGN KEY (nro_mesa, id_centro, id_eleccion, numero_serie) 
        REFERENCES MESA_UTILIZA_MAQUINA(nro_mesa, id_centro, id_eleccion, numero_serie)
);

CREATE TABLE VOTO_ELECCION_LEGISLATIVA (
    num_voto            VARCHAR(20),
    id_eleccion         VARCHAR(20),
    PRIMARY KEY (num_voto, id_eleccion),
    FOREIGN KEY (num_voto, id_eleccion) REFERENCES VOTO(num_voto, id_eleccion)
);

CREATE TABLE VOTO_CONSULTA_POPULAR (
    num_voto            VARCHAR(20),
    id_eleccion         VARCHAR(20),
    PRIMARY KEY (num_voto, id_eleccion),
    FOREIGN KEY (num_voto, id_eleccion) REFERENCES VOTO(num_voto, id_eleccion)
);

CREATE TABLE OPCION_RESPUESTA (
    id_opcion           VARCHAR(20) PRIMARY KEY,
    respuesta           VARCHAR(100)
);

CREATE TABLE VOTO_ELIJE_OPCION_RESPUESTA (
    num_voto            VARCHAR(20),
    id_eleccion         VARCHAR(20),
    id_opcion           VARCHAR(20),
    PRIMARY KEY (num_voto, id_eleccion),
    FOREIGN KEY (num_voto, id_eleccion) 
        REFERENCES VOTO_CONSULTA_POPULAR(num_voto, id_eleccion),
    FOREIGN KEY (id_opcion) REFERENCES OPCION_RESPUESTA(id_opcion)
);

CREATE TABLE CP_TIENE_OPCION_RESPUESTA (
    id_eleccion         VARCHAR(20),
    id_opcion           VARCHAR(20),
    PRIMARY KEY (id_eleccion, id_opcion),
    FOREIGN KEY (id_opcion)    REFERENCES OPCION_RESPUESTA(id_opcion),
    FOREIGN KEY (id_eleccion)  REFERENCES CONSULTA_POPULAR(id_eleccion)
);

CREATE TABLE VOTO_ELIJE_CANDIDATO (
    num_voto            VARCHAR(20),
    id_eleccion         VARCHAR(20),
    dni_politico         VARCHAR(20),
    PRIMARY KEY (num_voto, id_eleccion),
    FOREIGN KEY (num_voto, id_eleccion) 
        REFERENCES VOTO_ELECCION_LEGISLATIVA(num_voto, id_eleccion),
    FOREIGN KEY (dni_politico) REFERENCES POLITICO(dni_politico)
);