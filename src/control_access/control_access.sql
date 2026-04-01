-- Création des rôles
CREATE ROLE USER1;
CREATE ROLE USER2;
CREATE ROLE MEDECIN;
CREATE ROLE POLICIER;
CREATE ROLE ADMIN_USER;
CREATE ROLE ADMIN_GLOBAL LOGIN BYPASSRLS;

-- Création d'une table indiquant la nationalité de chaque user
CREATE TABLE UTILISATEUR_PAYS (
    utilisateur VARCHAR(20) PRIMARY KEY,
    pays        INTEGER
);

-- Instanciation de la table UTILISATEUR_PAYS
INSERT INTO UTILISATEUR_PAYS VALUES
('user1',   1),   -- français
('user2',   2),   -- anglais
('medecin', 1),
('policier',2);

-- DROITS
GRANT USAGE ON SCHEMA public TO USER1, USER2, MEDECIN, POLICIER, ADMIN_USER;
GRANT SELECT ON UTILISATEUR_PAYS TO ADMIN_GLOBAL;
GRANT SELECT ON UTILISATEUR_PAYS TO USER1, USER2, MEDECIN, POLICIER, ADMIN_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ADMIN_GLOBAL;

-- Médecin
GRANT SELECT ON fait_accident        TO MEDECIN;
GRANT SELECT ON dim_usager           TO MEDECIN;
GRANT SELECT ON dim_temps            TO MEDECIN;
GRANT SELECT ON dim_pays             TO MEDECIN;
GRANT SELECT ON dim_meteo            TO MEDECIN;
GRANT SELECT ON dim_localisation     TO MEDECIN;

-- Policier
GRANT SELECT ON fait_accident        TO POLICIER;
GRANT SELECT ON dim_vehicule         TO POLICIER;
GRANT SELECT ON dim_temps            TO POLICIER;
GRANT SELECT ON dim_pays             TO POLICIER;
GRANT SELECT ON dim_meteo            TO POLICIER;
GRANT SELECT ON dim_localisation     TO POLICIER;

-- User1 et User2
GRANT SELECT ON fait_accident        TO USER1, USER2;
GRANT SELECT ON dim_temps            TO USER1, USER2;
GRANT SELECT ON dim_pays             TO USER1, USER2;
GRANT SELECT ON dim_localisation     TO USER1, USER2;

-- Admin user
GRANT SELECT ON fait_accident        TO ADMIN_USER;
GRANT SELECT ON dim_temps            TO ADMIN_USER;
GRANT SELECT ON dim_pays             TO ADMIN_USER;

-- ACTIVATION DU ROW LEVEL SECURITY
ALTER TABLE fait_accident    ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_usager       ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_vehicule     ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_localisation ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_temps        ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_pays         ENABLE ROW LEVEL SECURITY;
ALTER TABLE dim_meteo        ENABLE ROW LEVEL SECURITY;

-- FONCTION — retourne le pays de l'utilisateur courant
CREATE OR REPLACE FUNCTION pays_utilisateur_courant()
RETURNS INTEGER
LANGUAGE SQL
SECURITY INVOKER
STABLE
AS $$
    SELECT pays
    FROM UTILISATEUR_PAYS
    WHERE utilisateur = current_user;
$$;

GRANT EXECUTE ON FUNCTION pays_utilisateur_courant() TO USER1, USER2, MEDECIN, POLICIER, ADMIN_USER;

-- POLITIQUES RLS
DROP POLICY IF EXISTS acces_pays ON fait_accident;
CREATE POLICY acces_pays ON fait_accident
FOR SELECT USING (id_pays = pays_utilisateur_courant());

DROP POLICY IF EXISTS acces_pays ON dim_usager;
CREATE POLICY acces_pays ON dim_usager
FOR SELECT USING (id_pays = pays_utilisateur_courant());

DROP POLICY IF EXISTS acces_pays ON dim_vehicule;
CREATE POLICY acces_pays ON dim_vehicule
FOR SELECT USING (id_pays = pays_utilisateur_courant());

DROP POLICY IF EXISTS acces_pays ON dim_localisation;
CREATE POLICY acces_pays ON dim_localisation
FOR SELECT USING (id_pays = pays_utilisateur_courant());

DROP POLICY IF EXISTS acces_pays ON dim_meteo;
CREATE POLICY acces_pays ON dim_meteo
FOR SELECT USING (id_pays = pays_utilisateur_courant());

-- dim_temps et dim_pays : pas de filtre pays, accès libre car c'est juste un calendrier
DROP POLICY IF EXISTS tout_voir ON dim_temps;
CREATE POLICY tout_voir ON dim_temps FOR SELECT USING (true);

DROP POLICY IF EXISTS tout_voir ON dim_pays;
CREATE POLICY tout_voir ON dim_pays FOR SELECT USING (true);

-- PROTECTION UTILISATEUR_PAYS
-- Chaque utilisateur ne voit que sa propre ligne
ALTER TABLE UTILISATEUR_PAYS ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS acces_propre_ligne ON UTILISATEUR_PAYS;
CREATE POLICY acces_propre_ligne ON UTILISATEUR_PAYS
FOR SELECT USING (utilisateur = current_user);