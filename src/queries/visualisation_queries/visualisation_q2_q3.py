import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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
# REQUÊTE 1 — Gravité moyenne par sexe et pays (sans LIMIT ni ROLLUP)
# ─────────────────────────────────────────────────────────────
q1 = """
    SELECT
        p.nom_pays,
        u.sexe,
        ROUND(AVG(f.indice_gravite)::numeric, 2) AS gravite_moyenne,
        COUNT(DISTINCT f.id_accident)             AS nb_accidents
    FROM FAIT_ACCIDENT f
    JOIN DIM_PAYS   p ON p.id_pays   = f.id_pays
    JOIN DIM_USAGER u ON u.id_usager = f.id_usager
    WHERE u.sexe IN ('Male', 'Female')
    GROUP BY p.nom_pays, u.sexe
    ORDER BY p.nom_pays, gravite_moyenne DESC;
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
# FIGURE 1 — Gravité moyenne par sexe et pays
# But : montrer que les hommes ont des accidents plus graves
# ─────────────────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=False)
fig.patch.set_facecolor("#ffffff")
fig.suptitle("Les hommes sont-ils responsables d'accidents plus graves ?",
             fontsize=14, fontweight="bold", color="#2C2C2A", y=1.02)

couleurs = {"Male": "#E05C5C", "Female": "#5B9BD5"}

for ax, pays in zip(axes, ["France", "Royaume-Uni"]):
    subset = df1[df1["nom_pays"] == pays].sort_values("gravite_moyenne", ascending=True)
    bars = ax.barh(
        subset["sexe"],
        subset["gravite_moyenne"],
        color=[couleurs[s] for s in subset["sexe"]],
        edgecolor="white",
        height=0.5
    )
    # Valeurs dans les barres
    for bar, val in zip(bars, subset["gravite_moyenne"]):
        ax.text(val - 0.02, bar.get_y() + bar.get_height() / 2,
                f"{val:.2f}", ha="right", va="center",
                fontsize=13, fontweight="bold", color="white")

    # Différence en annotation
    vals = subset.set_index("sexe")["gravite_moyenne"]
    if "Male" in vals and "Female" in vals:
        diff = vals["Male"] - vals["Female"]
        ax.annotate(f"Écart : +{diff:.2f} pour les hommes",
                    xy=(vals["Male"], 1),
                    xytext=(vals["Male"] * 0.5, 1.35),
                    fontsize=9, color="#E05C5C",
                    arrowprops=dict(arrowstyle="->", color="#E05C5C", lw=1.2))

    ax.set_title(pays, fontsize=12, fontweight="bold", color="#2C2C2A")
    ax.set_xlabel("Gravité moyenne (indice)", fontsize=10, color="#5F5E5A")
    ax.set_xlim(0, df1["gravite_moyenne"].max() * 1.2)
    ax.set_facecolor("#ffffff")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#D3D1C7")
    ax.spines["bottom"].set_color("#D3D1C7")
    ax.tick_params(colors="#888780")
    ax.grid(axis="x", color="#D3D1C7", linewidth=0.5, linestyle="--")

patches = [mpatches.Patch(color=couleurs[s], label=s) for s in ["Male", "Female"]]
fig.legend(handles=patches, fontsize=10, framealpha=0,
           loc="lower center", ncol=2, bbox_to_anchor=(0.5, -0.05))

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
ax2.set_title("Gravité des accidents selon les conditions météo\net les jours fériés",
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