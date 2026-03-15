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
    d1 = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
    masque = d1.isna()
    if masque.any():
        d1.loc[masque] = pd.to_datetime(s[masque], format="%Y-%m-%d", errors="coerce")
    masque = d1.isna()
    if masque.any():
        d1.loc[masque] = pd.to_datetime(s[masque], errors="coerce")
    return d1


def construire_conditions(precip, vent, tmax):
    """
    4 conditions homogènes FR + UK :
    - pluie      : précipitations > 0
    - vent_fort  : vent >= 40 m/s (et pas de précip)
    - ensoleille : temp max >= 20°C (et pas de précip, pas de vent fort)
    - nuageux    : tous les autres cas
    """
    precip = pd.to_numeric(precip, errors="coerce").fillna(0)
    vent   = pd.to_numeric(vent,   errors="coerce")
    tmax   = pd.to_numeric(tmax,   errors="coerce")

    conditions = []
    for i in range(len(precip)):
        p = precip.iloc[i]
        v = vent.iloc[i]
        tx = tmax.iloc[i]

        if pd.notna(p) and p > 2.0:
            conditions.append("pluie")
        elif pd.notna(v) and v >= 40:
            conditions.append("vent_fort")
        elif pd.notna(tx) and tx >= 20:
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

        try:
            df = pd.read_csv(fichier, sep=";", low_memory=False)
            if len(df.columns) == 1:
                df = pd.read_csv(fichier, sep=",", low_memory=False)
        except Exception:
            df = pd.read_csv(fichier, sep=",", low_memory=False)

        print("[FR] Colonnes détectées :", list(df.columns))

        col_date   = trouver_colonne(df, ["DATE", "AAAAMMJJ", "date"])
        col_t      = trouver_colonne(df, ["T", "temp", "temperature"], obligatoire=False)
        col_tmin   = trouver_colonne(df, ["TINF_H"], obligatoire=False)
        col_tmax   = trouver_colonne(df, ["TSUP_H"], obligatoire=False)
        col_vent   = trouver_colonne(df, ["FF", "vent", "wind"], obligatoire=False)
        col_preliq = trouver_colonne(df, ["PRELIQ", "preliq"], obligatoire=False)

        if col_t is None and col_tmin is None:
            raise ValueError(f"[FR] Température introuvable dans {fichier}")

        dates = normaliser_date(df[col_date])

        # Précipitations : uniquement liquides (PRELIQ) — on ignore PRENEI
        # car on ne peut pas faire la même chose pour UK
        precip = pd.to_numeric(df[col_preliq], errors="coerce") if col_preliq else 0

        t_min_serie = (
            pd.to_numeric(df[col_tmin], errors="coerce") if col_tmin
            else pd.to_numeric(df[col_t], errors="coerce")
        )
        t_max_serie = (
            pd.to_numeric(df[col_tmax], errors="coerce") if col_tmax
            else pd.to_numeric(df[col_t], errors="coerce")
        )

        out = pd.DataFrame({
            "date":           dates.dt.date,
            "T_min":          t_min_serie,
            "T_max":          t_max_serie,
            "precipitations": precip,
            "vent":           pd.to_numeric(df[col_vent], errors="coerce") if col_vent else pd.NA,
        })

        out = out.dropna(subset=["date"])
        out = out[out["date"].apply(lambda d: d.year in ANNEES_CIBLES)]

        out = (
            out.groupby("date", as_index=False)
            .agg({
                "T_min":          "mean",
                "T_max":          "mean",
                "precipitations": "mean",
                "vent":           "mean",
            })
        )

        out["id_pays"] = 1
        frames.append(out)

    fr = pd.concat(frames, ignore_index=True)
    fr["conditions"] = construire_conditions(fr["precipitations"], fr["vent"], fr["T_max"])
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

        col_date   = trouver_colonne(df, ["date"])
        col_temp   = trouver_colonne(df, ["temp", "temperature"], obligatoire=False)
        col_precip = trouver_colonne(df, ["precipitation", "precip", "rainfall", "rain"], obligatoire=False)
        col_vent   = trouver_colonne(df, ["wind_speed", "windspeed", "wind"], obligatoire=False)

        if col_temp is None or col_precip is None:
            raise ValueError(
                f"[UK] Colonnes insuffisantes dans {fichier}. "
                f"Trouvées: temp={col_temp}, precip={col_precip}, vent={col_vent}"
            )

        dates = normaliser_date(df[col_date])

        out = pd.DataFrame({
            "date":           dates.dt.date,
            "T_min":          pd.to_numeric(df[col_temp], errors="coerce"),
            "T_max":          pd.to_numeric(df[col_temp], errors="coerce"),
            "precipitations": pd.to_numeric(df[col_precip], errors="coerce"),
            "vent":           pd.to_numeric(df[col_vent], errors="coerce") if col_vent else pd.NA,
        })

        out = out.dropna(subset=["date"])
        out = out[out["date"].apply(lambda d: d.year in ANNEES_CIBLES)]

        out = (
            out.groupby("date", as_index=False)
            .agg({
                "T_min":          "mean",
                "T_max":          "mean",
                "precipitations": "mean",
                "vent":           "mean",
            })
        )

        out["id_pays"] = 2
        frames.append(out)

    uk = pd.concat(frames, ignore_index=True)
    uk["conditions"] = construire_conditions(uk["precipitations"], uk["vent"], uk["T_max"])
    return uk


def nettoyer_final(df):
    df = df.copy()
    df["precipitations"] = df["precipitations"].fillna(0)
    if df["vent"].dropna().empty:
        df["vent"] = 0
    else:
        df["vent"] = df["vent"].fillna(df["vent"].median())
    df = df.drop_duplicates(subset=["date", "id_pays"])
    df = df.sort_values(["id_pays", "date"]).reset_index(drop=True)
    df["id_meteo"] = range(1, len(df) + 1)
    df = df[["id_meteo", "date", "T_min", "T_max", "precipitations", "vent", "conditions", "id_pays"]]
    return df


def verifier_jointure(df):
    print("\n[CHECK] Exemple de clés de jointure (date, id_pays) :")
    print(df[["date", "id_pays"]].head(10))
    nb_doublons = df.duplicated(["date", "id_pays"]).sum()
    print(f"\n[CHECK] Nombre de lignes : {len(df)}")
    print(f"[CHECK] Doublons sur (date, id_pays) : {nb_doublons}")
    if nb_doublons == 0:
        print("[CHECK] Jointure possible avec les accidents via (date, id_pays)")
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