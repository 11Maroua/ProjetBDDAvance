-- Indexation sur DIM_TEMPS et FAIT_ACCIDENT

-- Index sur DIM_TEMPS
CREATE INDEX IF NOT EXISTS idx_dim_temps_annee    ON dim_temps(annee);
CREATE INDEX IF NOT EXISTS idx_dim_temps_mois     ON dim_temps(mois);
CREATE INDEX IF NOT EXISTS idx_dim_temps_saison   ON dim_temps(saison);
CREATE INDEX IF NOT EXISTS idx_dim_temps_weekend  ON dim_temps(est_weekend);

-- Index sur FAIT_ACCIDENT (clés étrangères + date)
CREATE INDEX IF NOT EXISTS idx_fait_accident_id_pays     ON fait_accident(id_pays);
CREATE INDEX IF NOT EXISTS idx_fait_accident_id_lieu     ON fait_accident(id_lieu);
CREATE INDEX IF NOT EXISTS idx_fait_accident_id_usager   ON fait_accident(id_usager);
CREATE INDEX IF NOT EXISTS idx_fait_accident_id_vehicule ON fait_accident(id_vehicule);
CREATE INDEX IF NOT EXISTS idx_fait_accident_date        ON fait_accident(date);