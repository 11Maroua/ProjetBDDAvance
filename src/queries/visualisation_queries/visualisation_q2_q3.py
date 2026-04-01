import pandas as pd
import matplotlib.pyplot as plt
import psycopg2

conn = psycopg2.connect(
    dbname="accidents_db",
    user="user_name",
    host="localhost",
    port=5432
)

q1 = """
    SELECT
        p.nom_pays,
        u.sexe,
        COUNT(DISTINCT f.id_accident)                AS nb_accidents,
        ROUND(AVG(f.indice_gravite)::numeric, 2)     AS gravite_moyenne,
        RANK() OVER (
            PARTITION BY p.nom_pays
            ORDER BY AVG(f.indice_gravite) DESC
        ) AS rang_gravite,
        ROUND(
            COUNT(DISTINCT f.id_accident)::numeric /
            SUM(COUNT(DISTINCT f.id_accident)) OVER (PARTITION BY p.nom_pays) * 100
        , 1) AS pct_accidents_pays,
        ROUND((
            AVG(f.indice_gravite) -
            AVG(AVG(f.indice_gravite)) OVER (PARTITION BY p.nom_pays)
        )::numeric, 2) AS ecart_moyenne_pays
    FROM FAIT_ACCIDENT f
    JOIN DIM_PAYS   p ON p.id_pays   = f.id_pays
    JOIN DIM_USAGER u ON u.id_usager = f.id_usager
    WHERE u.sexe IN ('Male', 'Female')
    GROUP BY p.nom_pays, u.sexe
    ORDER BY p.nom_pays, rang_gravite;
"""
df1 = pd.read_sql(q1, conn)

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

couleurs  = {"Male": "#E8813A", "Female": "#2ECC9A"}
pays_list = ["France", "Royaume-Uni"]

# ─────────────────────────────────────────────────────────────
# FIGURE 1 — Proportion des accidents par sexe
# ─────────────────────────────────────────────────────────────
fig1, axes1 = plt.subplots(1, 2, figsize=(12, 6))
fig1.patch.set_facecolor("#ffffff")
fig1.suptitle("Proportion des accidents par sexe — FR vs UK",
              fontsize=14, fontweight="bold", color="#2C2C2A", y=0.98)

for i, pays in enumerate(pays_list):
    ax      = axes1[i]
    subset  = df1[df1["nom_pays"] == pays]
    valeurs = subset["pct_accidents_pays"].tolist()
    labels  = subset["sexe"].tolist()
    colors  = [couleurs[s] for s in labels]

    wedges, texts, autotexts = ax.pie(
        valeurs,
        labels=["Hommes" if s == "Male" else "Femmes" for s in labels],
        colors=colors,
        autopct="%1.1f%%",
        startangle=90,
        pctdistance=0.75,
        wedgeprops=dict(edgecolor="white", linewidth=2),
        explode=[0.05 if s == "Male" else 0 for s in labels],
    )
    for t in texts:
        t.set_fontsize(11); t.set_color("#2C2C2A")
    for at in autotexts:
        at.set_fontsize(11); at.set_fontweight("bold"); at.set_color("white")
    ax.set_title(pays, fontsize=12, fontweight="bold", color="#2C2C2A", pad=12)

plt.tight_layout(pad=2.0)
plt.savefig("docs/images/proportion_sexe_pays.png", dpi=150, bbox_inches="tight")
plt.show()
print("Sauvegardé : docs/images/proportion_sexe_pays.png")

# ─────────────────────────────────────────────────────────────
# FIGURE 2 — Gravité moyenne par sexe et pays
# ─────────────────────────────────────────────────────────────
fig2, ax2 = plt.subplots(figsize=(8, 5))
fig2.patch.set_facecolor("#ffffff")
fig2.suptitle("Gravité moyenne des accidents par sexe — FR vs UK",
              fontsize=14, fontweight="bold", color="#2C2C2A", y=0.98)

pays_x = [0, 1]
width  = 0.3
for j, sexe in enumerate(["Male", "Female"]):
    subset = df1[df1["sexe"] == sexe]
    vals   = [subset[subset["nom_pays"] == p]["gravite_moyenne"].values[0]
              if len(subset[subset["nom_pays"] == p]) > 0 else 0
              for p in pays_list]
    offset = (j - 0.5) * width
    bars   = ax2.bar([x + offset for x in pays_x], vals, width,
                     color=couleurs[sexe], edgecolor="white",
                     label="Hommes" if sexe == "Male" else "Femmes")
    for bar, val in zip(bars, vals):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.005,
                 f"{val:.2f}", ha="center", va="bottom", fontsize=10, color="#444444")

ax2.set_xticks(pays_x)
ax2.set_xticklabels(pays_list, fontsize=11)
ax2.set_ylabel("Gravité moyenne", fontsize=10, color="#5F5E5A")
ax2.set_ylim(0, df1["gravite_moyenne"].max() * 1.2)
ax2.set_facecolor("#ffffff")
ax2.spines["top"].set_visible(False)
ax2.spines["right"].set_visible(False)
ax2.spines["left"].set_color("#D3D1C7")
ax2.spines["bottom"].set_color("#D3D1C7")
ax2.grid(axis="y", color="#D3D1C7", linewidth=0.5, linestyle="--")
ax2.legend(fontsize=10, framealpha=0)

plt.tight_layout(pad=2.0)
plt.savefig("docs/images/gravite_sexe_pays.png", dpi=150, bbox_inches="tight")
plt.show()
print("Sauvegardé : docs/images/gravite_sexe_pays.png")

# ─────────────────────────────────────────────────────────────
# FIGURE 3 — Gravité selon conditions météo et jours fériés
# ─────────────────────────────────────────────────────────────
df2["est_jour_ferie"] = df2["est_jour_ferie"].map({True: "Jour férié", False: "Jour normal"})
df2["gravite_moyenne"] = df2["gravite_moyenne"].astype(float)

conditions = sorted(df2["conditions"].unique())
jours      = ["Jour férié", "Jour normal"]
couleurs2  = {"Jour férié": "#F4A261", "Jour normal": "#5DCAA5"}
x          = range(len(conditions))
width      = 0.35

fig3, ax3 = plt.subplots(figsize=(10, 5))
fig3.patch.set_facecolor("#ffffff")

for i, jour in enumerate(jours):
    subset = df2[df2["est_jour_ferie"] == jour]
    vals   = [subset[subset["conditions"] == c]["gravite_moyenne"].values[0]
              if len(subset[subset["conditions"] == c]) > 0 else 0
              for c in conditions]
    offset = (i - 0.5) * width
    bars   = ax3.bar([xi + offset for xi in x], vals, width,
                     color=couleurs2[jour], edgecolor="white", linewidth=0.8,
                     label=jour)
    for bar, val in zip(bars, vals):
        ax3.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                 f"{val:.2f}", ha="center", va="bottom", fontsize=10, color="#444444")

ax3.set_xticks(list(x))
ax3.set_xticklabels(conditions, fontsize=11)
ax3.set_ylabel("Gravité moyenne (indice)", fontsize=11, color="#5F5E5A")
ax3.set_title("Gravité des accidents selon les conditions météo et les jours fériés",
              fontsize=13, fontweight="bold", color="#2C2C2A", pad=14)
ax3.set_facecolor("#ffffff")
ax3.spines["top"].set_visible(False)
ax3.spines["right"].set_visible(False)
ax3.spines["left"].set_color("#D3D1C7")
ax3.spines["bottom"].set_color("#D3D1C7")
ax3.tick_params(colors="#888780")
ax3.grid(axis="y", color="#D3D1C7", linewidth=0.5, linestyle="--")
ax3.legend(fontsize=10, framealpha=0)

plt.tight_layout(pad=2.0)
plt.savefig("docs/images/gravite_meteo_ferie.png", dpi=150, bbox_inches="tight")
plt.show()
print("Sauvegardé : docs/images/gravite_meteo_ferie.png")