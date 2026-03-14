-- ============================================================
-- SCHEMA PostgreSQL - Accidents de la route
-- ============================================================

CREATE TABLE DIM_PAYS (
    id_pays   INT         PRIMARY KEY,
    code_pays VARCHAR(10),
    nom_pays  VARCHAR(100)
);

CREATE TABLE DIM_DATE (
    id_date        INT     PRIMARY KEY,
    date           DATE,
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
    conditions     VARCHAR(50)
);

CREATE TABLE DIM_USAGER (
    id_usager      INT PRIMARY KEY,
    id_pays        INT,
    CONSTRAINT fk_usager_pays FOREIGN KEY (id_pays) REFERENCES DIM_PAYS(id_pays),
    age            INT,
    sexe           VARCHAR(20),     -- "Male", "Female", "Unknown"
    place_vehicule VARCHAR(50),     
    gravite        VARCHAR(50),     -- "Killed", "Seriously injured", etc.
    cat_usager     VARCHAR(50)      -- "Driver", "Passenger", "Pedestrian"
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


CREATE TABLE FAIT_ACCIDENT (
    id_accident       INT PRIMARY KEY,
    id_pays           INT,
    CONSTRAINT fk_fait_pays  FOREIGN KEY (id_pays)  REFERENCES DIM_PAYS(id_pays),
    id_date           INT,
    CONSTRAINT fk_fait_date  FOREIGN KEY (id_date)  REFERENCES DIM_DATE(id_date),
    id_meteo          INT,
    CONSTRAINT fk_fait_meteo FOREIGN KEY (id_meteo) REFERENCES DIM_METEO(id_meteo),
    nb_tues           FLOAT,       
    nb_blesses_graves FLOAT,
    nb_blesses_legers FLOAT,
    nb_victimes_total FLOAT,
    nb_usagers        FLOAT,
    indice_gravite    FLOAT
);