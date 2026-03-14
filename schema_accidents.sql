-- ============================================================
-- SCHEMA PostgreSQL - Accidents de la route
-- ============================================================

CREATE TABLE DIM_PAYS (
    id_pays   INT         PRIMARY KEY,
    code_pays VARCHAR(10),
    nom_pays  VARCHAR(100)
);

CREATE TABLE DIM_DATE (
    id_date        INT  PRIMARY KEY,
    date           DATE UNIQUE,     
    annee          INT,
    mois           INT,
    jour           INT,
    heure          INT,
    saison         VARCHAR(20),
    est_weekend    BOOLEAN,
    est_jour_ferie BOOLEAN
);

CREATE TABLE DIM_LOCALISATION (
    id_lieu        INT PRIMARY KEY,
    id_pays        INT,
    CONSTRAINT fk_loc_pays FOREIGN KEY (id_pays) REFERENCES DIM_PAYS(id_pays),
    commune        VARCHAR(50),
    departement    VARCHAR(20),
    district       VARCHAR(50),
    type_route     VARCHAR(50),
    vitesse_limite INT,
    latitude       FLOAT,
    longitude      FLOAT
);

CREATE TABLE DIM_METEO (
    id_meteo       INT PRIMARY KEY,
    id_pays        INT,
    CONSTRAINT fk_meteo_pays FOREIGN KEY (id_pays) REFERENCES DIM_PAYS(id_pays),
    date           DATE,
    temp_max       FLOAT,
    temp_min       FLOAT,
    precipitations FLOAT,
    vent           FLOAT,
    conditions     VARCHAR(50),
    UNIQUE (id_pays, date)         
);

CREATE TABLE DIM_USAGER (
    id_usager      INT PRIMARY KEY,
    id_pays        INT,
    CONSTRAINT fk_usager_pays FOREIGN KEY (id_pays) REFERENCES DIM_PAYS(id_pays),
    age            INT,
    sexe           VARCHAR(20),
    place_vehicule VARCHAR(50),
    gravite        VARCHAR(50),
    cat_usager     VARCHAR(50)
);

CREATE TABLE DIM_VEHICULE (
    id_vehicule   INT PRIMARY KEY,
    id_pays       INT,
    CONSTRAINT fk_vehicule_pays FOREIGN KEY (id_pays) REFERENCES DIM_PAYS(id_pays),
    type_vehicule VARCHAR(50),
    manoeuvre     VARCHAR(100),
    nb_occupants  INT,
    motorisation  VARCHAR(30)
);

-- FAIT_ACCIDENT: one row per accident
-- date stored directly — join to DIM_DATE and DIM_METEO on (id_pays, date)
CREATE TABLE FAIT_ACCIDENT (
    id_accident       INT PRIMARY KEY,
    id_pays           INT,
    CONSTRAINT fk_fait_pays     FOREIGN KEY (id_pays)     REFERENCES DIM_PAYS(id_pays),
    date              DATE,        
    id_lieu           INT,
    CONSTRAINT fk_fait_lieu     FOREIGN KEY (id_lieu)     REFERENCES DIM_LOCALISATION(id_lieu),
    id_usager         INT,
    CONSTRAINT fk_fait_usager   FOREIGN KEY (id_usager)   REFERENCES DIM_USAGER(id_usager),
    id_vehicule       INT,
    CONSTRAINT fk_fait_vehicule FOREIGN KEY (id_vehicule) REFERENCES DIM_VEHICULE(id_vehicule),
    nb_tues           INT,
    nb_blesses_graves INT,
    nb_blesses_legers INT,
    nb_victimes_total INT,
    nb_vehicules      INT,
    indice_gravite    FLOAT
);

