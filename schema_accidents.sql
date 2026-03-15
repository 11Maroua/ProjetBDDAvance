-- SCHEMA PostgreSQL - Entrepot de données accidents et météo FR vs UK

-- 2 lignes : France (id=1) et Royaume-Uni (id=2)
CREATE TABLE DIM_PAYS (
    id_pays   INT         PRIMARY KEY,
    code_pays VARCHAR(10),
    nom_pays  VARCHAR(100)
);

-- UNIQUE sur date : garantit 1 seule ligne par jour + accélère les jointures
CREATE TABLE DIM_TEMPS (
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

-- departement rempli pour FR, NULL pour UK
-- district rempli pour UK, NULL pour FR
-- latitude/longitude converties en WGS84 (conversion Lambert93 faite dans buildfait.py)
-- vitesse_limite en km/h (conversion mph→km/h faite dans buildfait.py pour UK)
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

-- UNIQUE (id_pays, date) : clé de jointure avec FAIT_ACCIDENT
-- on veut recupérer un la météo d'un accident mais propre à un pays.
CREATE TABLE DIM_METEO (
    id_meteo       INT PRIMARY KEY,
    id_pays        INT,
    CONSTRAINT fk_meteo_pays FOREIGN KEY (id_pays) REFERENCES DIM_PAYS(id_pays),
    date           DATE,
    temp_max       FLOAT,-- temp_max = TSUP_H (FR) ou temp (UK)
    temp_min       FLOAT,-- temp_min = TINF_H (FR) ou temp (UK)
    precipitations FLOAT,
    vent           FLOAT,
    conditions     VARCHAR(50),-- conditions : neige / pluie / vent_fort / ensoleille / nuageux
    UNIQUE (id_pays, date)
);

-- Décodage des  codes numériques des sources:
-- FR : grav=2 → "Killed", sexe=1 → "Male" via FR_GRAV, FR_SEXE
-- UK : casualty_severity=1 → "Killed", sex_of_casualty=1 → "Male" via UK_SEV, UK_SEXE
CREATE TABLE DIM_USAGER (
    id_usager      INT PRIMARY KEY,
    id_pays        INT,
    CONSTRAINT fk_usager_pays FOREIGN KEY (id_pays) REFERENCES DIM_PAYS(id_pays),
    age            INT, -- age calculé depuis an_nais pour FR, directement disponible pour UK
    sexe           VARCHAR(20),
    place_vehicule VARCHAR(50),
    gravite        VARCHAR(50),
    cat_usager     VARCHAR(50)
);

-- type_vehicule, manoeuvre, motorisation décodés via FR_CATV, FR_MANV, UK_VEH, UK_MANV
-- nb_occupants spécifique FR (occutc), 0 pour UK (non disponible)
CREATE TABLE DIM_VEHICULE (
    id_vehicule   INT PRIMARY KEY,
    id_pays       INT,
    CONSTRAINT fk_vehicule_pays FOREIGN KEY (id_pays) REFERENCES DIM_PAYS(id_pays),
    type_vehicule VARCHAR(50),
    manoeuvre     VARCHAR(100),
    nb_occupants  INT,
    motorisation  VARCHAR(30)
);



-- Table centrale de notre schéma en étoile : 1 ligne par accident
-- Jointure vers DIM_METEO via (id_pays, date)
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
    indice_gravite    FLOAT -- indice_gravite = nb_tues×3 + nb_blesses_graves×2 + nb_blesses_legers×1
);
