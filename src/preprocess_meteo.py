import os
import glob
import pandas as pd

RAW_FR_DIR = "data/raw/meteo_fr"
RAW_UK_DIR = "data/raw/meteo_uk"
OUT_FILE = "data/processed/dim_meteo.csv"

ANNEES_CIBLES = {2005, 2007, 2009, 2011, 2013, 2015, 2017, 2019, 2021}

os.makedirs("data/processed", exist_ok=True)


def trouver_colonne(df, candidats, obligatoire=True):
    cols = {c.lower().strip(): c for c in df.columns}

    for cand in candidats:
        if cand.lower() in cols:
            return cols[cand.lower()]

    for col in df.columns:
        col_l = col.lower().strip()
        for cand in candidats:
            if cand.lower() in col_l:
                return col

    if obligatoire:
        raise ValueError(f"Colonne introuvable parmi {candidats}. Colonnes disponibles: {list(df.columns)}")
    return None


def normaliser_date(serie):
    s = serie.astype(str).str.strip()

    # cas 20050115
    d1 = pd.to_datetime(s, format="%Y%m%d", errors="coerce")

    # cas 2005-01-15
    masque = d1.isna()
    if masque.any():
        d1.loc[masque] = pd.to_datetime(s[masque], format="%Y-%m-%d", errors="coerce")

    # fallback général
    masque = d1.isna()
    if masque.any():
        d1.loc[masque] = pd.to_datetime(s[masque], errors="coerce")

    return d1


def construire_conditions(tmin, tmax, precip, vent):
    tmin = pd.to_numeric(tmin, errors="coerce")
    tmax = pd.to_numeric(tmax, errors="coerce")
    precip = pd.to_numeric(precip, errors="coerce").fillna(0)
    vent = pd.to_numeric(vent, errors="coerce")

    temp_moy = (tmin + tmax) / 2
    conditions = []

    for i in range(len(precip)):
        p = precip.iloc[i]
        tn = tmin.iloc[i]
        tm = temp_moy.iloc[i]
        v = vent.iloc[i] if i < len(vent) else None

        if pd.notna(p) and p > 0:
            if pd.notna(tn) and tn <= 0:
                conditions.append("neige")
            else:
                conditions.append("pluie")
        elif pd.notna(v) and v >= 40:
            conditions.append("brouillard")
        elif pd.notna(tm) and tm >= 20:
            conditions.append("ensoleille")
        else:
            conditions.append("nuageux")

    return conditions


def preprocess_fr():
    fichiers = sorted(glob.glob(os.path.join(RAW_FR_DIR, "meteo_fr_*.csv")))
    if not fichiers:
        raise FileNotFoundError("Aucun fichier meteo_fr_*.csv trouvé dans data/raw/meteo_fr")

    frames = []

    for fichier in fichiers:
        print(f"[FR] Lecture : {fichier}")

        # lecture robuste : essayer ; puis , puis fallback
        try:
            df = pd.read_csv(fichier, sep=";", low_memory=False)
            if len(df.columns) == 1:
                df = pd.read_csv(fichier, sep=",", low_memory=False)
        except Exception:
            df = pd.read_csv(fichier, sep=",", low_memory=False)

        print("[FR] Colonnes détectées :", list(df.columns))

        # colonnes spécifiques SIM
        col_date = trouver_colonne(df, ["DATE", "AAAAMMJJ", "date"])
        col_lambx = trouver_colonne(df, ["LAMBX", "lambx"], obligatoire=False)
        col_lamby = trouver_colonne(df, ["LAMBY", "lamby"], obligatoire=False)

        # température
        col_t = trouver_colonne(df, ["T", "temp", "temperature"], obligatoire=False)

        # vent
        col_vent = trouver_colonne(df, ["FF", "vent", "wind"], obligatoire=False)

        # précipitations = liquide + neige
        col_preliq = trouver_colonne(df, ["PRELIQ", "preliq"], obligatoire=False)
        col_prenei = trouver_colonne(df, ["PRENEI", "preneur", "prenei"], obligatoire=False)

        if col_t is None:
            raise ValueError(f"[FR] Température introuvable dans {fichier}")

        dates = normaliser_date(df[col_date])

        precip_liq = pd.to_numeric(df[col_preliq], errors="coerce") if col_preliq else 0
        precip_nei = pd.to_numeric(df[col_prenei], errors="coerce") if col_prenei else 0
        precip_total = precip_liq + precip_nei

        # Comme SIM est par grille, on crée un identifiant de zone à partir des coordonnées
        if col_lambx and col_lamby:
            region_series = (
                df[col_lambx].astype(str).str.strip() + "_" + df[col_lamby].astype(str).str.strip()
            )
        else:
            region_series = "FR_ZONE_INCONNUE"

        out = pd.DataFrame({
            "date": dates.dt.date,
            "region": region_series,
            "T_min": pd.to_numeric(df[col_t], errors="coerce"),
            "T_max": pd.to_numeric(df[col_t], errors="coerce"),
            "precipitations": precip_total,
            "vent": pd.to_numeric(df[col_vent], errors="coerce") if col_vent else pd.NA,
        })

        out = out.dropna(subset=["date", "region"])
        out = out[out["date"].apply(lambda d: d.year in ANNEES_CIBLES)]

        # Agrégation région / jour
        out = (
            out.groupby(["date", "region"], as_index=False)
            .agg({
                "T_min": "mean",
                "T_max": "mean",
                "precipitations": "mean",
                "vent": "mean",
            })
        )

        out["id_pays"] = 1
        frames.append(out)

    fr = pd.concat(frames, ignore_index=True)

    fr["conditions"] = construire_conditions(
        fr["T_min"], fr["T_max"], fr["precipitations"], fr["vent"]
    )

    return fr


def preprocess_uk():
    fichiers = sorted(glob.glob(os.path.join(RAW_UK_DIR, "meteo_uk_*.csv")))
    if not fichiers:
        raise FileNotFoundError("Aucun fichier meteo_uk_*.csv trouvé dans data/raw/meteo_uk")

    frames = []

    for fichier in fichiers:
        print(f"[UK] Lecture : {fichier}")
        df = pd.read_csv(fichier, low_memory=False)

        print("[UK] Colonnes détectées :", list(df.columns))

        col_date = trouver_colonne(df, ["date"])
        col_temp = trouver_colonne(df, ["temp", "temperature"], obligatoire=False)
        col_precip = trouver_colonne(df, ["precipitation", "precip", "rainfall", "rain"], obligatoire=False)
        col_vent = trouver_colonne(df, ["wind_speed", "windspeed", "wind"], obligatoire=False)

        if col_temp is None or col_precip is None:
            raise ValueError(
                f"[UK] Colonnes insuffisantes dans {fichier}. "
                f"Trouvées: temp={col_temp}, precip={col_precip}, vent={col_vent}"
            )

        dates = normaliser_date(df[col_date])

        out = pd.DataFrame({
            "date": dates.dt.date,
            "region": "UK_GLOBAL",
            "T_min": pd.to_numeric(df[col_temp], errors="coerce"),
            "T_max": pd.to_numeric(df[col_temp], errors="coerce"),
            "precipitations": pd.to_numeric(df[col_precip], errors="coerce"),
            "vent": pd.to_numeric(df[col_vent], errors="coerce") if col_vent else pd.NA,
        })

        out = out.dropna(subset=["date"])
        out = out[out["date"].apply(lambda d: d.year in ANNEES_CIBLES)]

        # Agrégation région / jour
        out = (
            out.groupby(["date", "region"], as_index=False)
            .agg({
                "T_min": "mean",
                "T_max": "mean",
                "precipitations": "mean",
                "vent": "mean",
            })
        )

        out["id_pays"] = 2
        frames.append(out)

    uk = pd.concat(frames, ignore_index=True)

    uk["conditions"] = construire_conditions(
        uk["T_min"], uk["T_max"], uk["precipitations"], uk["vent"]
    )

    return uk

def nettoyer_final(df):
    df = df.copy()

    # Valeurs manquantes
    df["precipitations"] = df["precipitations"].fillna(0)
    if df["vent"].dropna().empty:
        df["vent"] = 0
    else:
        df["vent"] = df["vent"].fillna(df["vent"].median())

    # Doublons
    df = df.drop_duplicates(subset=["date", "region", "id_pays"])

    # Clé surrogate
    df = df.sort_values(["id_pays", "region", "date"]).reset_index(drop=True)
    df["id_meteo"] = range(1, len(df) + 1)

    # Ordre final
    df = df[
        ["id_meteo", "date", "region", "T_min", "T_max", "precipitations", "vent", "conditions", "id_pays"]
    ]

    return df


def verifier_jointure(df):
    print("\n[CHECK] Exemple de clés de jointure (date, region, id_pays) :")
    print(df[["date", "region", "id_pays"]].head(10))

    nb_doublons = df.duplicated(["date", "region", "id_pays"]).sum()
    print(f"\n[CHECK] Nombre de lignes : {len(df)}")
    print(f"[CHECK] Doublons sur (date, region, id_pays) : {nb_doublons}")

    if nb_doublons == 0:
        print("[CHECK] Jointure possible avec les accidents via (date, region, id_pays)")
    else:
        print("[CHECK] Attention : il reste des doublons pour la jointure")


def main():
    print("=" * 60)
    print("PREPROCESSING DIM_METEO")
    print("=" * 60)

    fr = preprocess_fr()
    uk = preprocess_uk()

    dim_meteo = pd.concat([fr, uk], ignore_index=True)
    dim_meteo = nettoyer_final(dim_meteo)
    verifier_jointure(dim_meteo)

    dim_meteo.to_csv(OUT_FILE, index=False)
    print(f"\n[OK] Fichier créé : {OUT_FILE}")


if __name__ == "__main__":
    main()