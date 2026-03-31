import pandas as pd
import matplotlib.pyplot as plt
import psycopg2

# ─────────────────────────────────────────────────────────────
# CONNEXION
# ─────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    dbname="accidents_db",
    user="marouanaitslimani",
    host="localhost",
    port=5432
)

# ─────────────────────────────────────────────────────────────
# REQUÊTE 1 — Proportion d'accidents graves par sexe et pays
# accidents graves = indice_gravite > 4
# ─────────────────────────────────────────────────────────────
q1 = """
    SELECT
        p.nom_pays,
        u.sexe,
        COUNT(DISTINCT f.id_accident) AS nb_accidents_graves
    FROM FAIT_ACCIDENT f
    JOIN DIM_PAYS   p ON p.id_pays   = f.id_pays
    JOIN DIM_USAGER u ON u.id_usager = f.id_usager
    WHERE u.sexe IN ('Male', 'Female')
      AND f.indice_gravite > 4
    GROUP BY p.nom_pays, u.sexe
    ORDER BY p.nom_pays, u.sexe;
"""
df1 = pd.read_sql(q1, conn)

# ─────────────────────────────────────────────────────────────
# REQUÊTE 2 — Gravité selon météo et jours fériés
# ─────────────────────────────────────────────────────────────
q2 = """
    SELECT
        m.conditions,
        t.est_jour_ferie,
        COUNT(DISTINCT f.id_accident)              AS nb_accidents,
        ROUND(AVG(f.indice_gravite)::numeric, 2)   AS gravite_moyenne
    FROM FAIT_ACCIDENT f
    JOIN DIM_TEMPS t ON t.date = f.date
    JOIN DIM_METEO m ON m.id_pays = f.id_pays AND m.date = f.date
    GROUP BY m.conditions, t.est_jour_ferie
    ORDER BY gravite_moyenne DESC;
"""
df2 = pd.read_sql(q2, conn)
conn.close()

# ─────────────────────────────────────────────────────────────
# FIGURE 1 — Camemberts proportion accidents graves Homme vs Femme
# ─────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(12, 6))
fig.patch.set_facecolor("#ffffff")
fig.suptitle("Proportion d'accidents graves : Hommes vs Femmes",
             fontsize=15, fontweight="bold", color="#2C2C2A", y=0.98)

couleurs = ["#E8813A", "#2ECC9A"]  # orange = Male, vert = Female

for ax, pays in zip(axes, ["France", "Royaume-Uni"]):
    subset = df1[df1["nom_pays"] == pays].set_index("sexe")
    valeurs = [
        subset.loc["Male",   "nb_accidents_graves"] if "Male"   in subset.index else 0,
        subset.loc["Female", "nb_accidents_graves"] if "Female" in subset.index else 0,
    ]
    labels  = ["Hommes", "Femmes"]
    total   = sum(valeurs)

    wedges, texts, autotexts = ax.pie(
        valeurs,
        labels=labels,
        colors=couleurs,
        autopct="%1.1f%%",
        startangle=90,
        pctdistance=0.75,
        wedgeprops=dict(edgecolor="white", linewidth=2),
        explode=(0.05, 0),   # légère mise en avant des hommes
    )

    for text in texts:
        text.set_fontsize(12)
        text.set_color("#2C2C2A")
    for autotext in autotexts:
        autotext.set_fontsize(12)
        autotext.set_fontweight("bold")
        autotext.set_color("white")

    ax.set_title(f"{pays}\n({total:,} accidents graves)",
                 fontsize=12, fontweight="bold", color="#2C2C2A", pad=16)

plt.tight_layout(pad=2.0)
plt.savefig("docs/images/gravite_sexe_pays.png", dpi=150, bbox_inches="tight")
plt.show()
print("Sauvegardé : docs/images/gravite_sexe_pays.png")

# ─────────────────────────────────────────────────────────────
# FIGURE 2 — Gravité selon conditions météo et jours fériés
# ─────────────────────────────────────────────────────────────
df2["est_jour_ferie"] = df2["est_jour_ferie"].map({True: "Jour férié", False: "Jour normal"})
df2["gravite_moyenne"] = df2["gravite_moyenne"].astype(float)

conditions = sorted(df2["conditions"].unique())
jours      = ["Jour férié", "Jour normal"]
couleurs2  = {"Jour férié": "#F4A261", "Jour normal": "#5DCAA5"}
x          = range(len(conditions))
width      = 0.35

fig2, ax2 = plt.subplots(figsize=(10, 5))
fig2.patch.set_facecolor("#ffffff")

for i, jour in enumerate(jours):
    subset = df2[df2["est_jour_ferie"] == jour]
    vals   = [subset[subset["conditions"] == c]["gravite_moyenne"].values[0]
              if len(subset[subset["conditions"] == c]) > 0 else 0
              for c in conditions]
    offset = (i - 0.5) * width
    bars   = ax2.bar([xi + offset for xi in x], vals, width,
                     color=couleurs2[jour], edgecolor="white", linewidth=0.8,
                     label=jour)
    for bar, val in zip(bars, vals):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                 f"{val:.2f}", ha="center", va="bottom", fontsize=10,
                 color="#444444")

ax2.set_xticks(list(x))
ax2.set_xticklabels(conditions, fontsize=11)
ax2.set_ylabel("Gravité moyenne (indice)", fontsize=11, color="#5F5E5A")
ax2.set_title("Gravité des accidents selon les conditions météo et les jours fériés",
              fontsize=13, fontweight="bold", color="#2C2C2A", pad=14)
ax2.set_facecolor("#ffffff")
ax2.spines["top"].set_visible(False)
ax2.spines["right"].set_visible(False)
ax2.spines["left"].set_color("#D3D1C7")
ax2.spines["bottom"].set_color("#D3D1C7")
ax2.tick_params(colors="#888780")
ax2.grid(axis="y", color="#D3D1C7", linewidth=0.5, linestyle="--")
ax2.legend(fontsize=10, framealpha=0)

plt.tight_layout(pad=2.0)
plt.savefig("docs/images/gravite_meteo_ferie.png", dpi=150, bbox_inches="tight")
plt.show()
print("Sauvegardé : docs/images/gravite_meteo_ferie.png")