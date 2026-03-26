import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ─────────────────────────────────────────────────────────────
# DONNÉES
# ─────────────────────────────────────────────────────────────
requetes_labels = [
    "Gravité moyenne\npar météo et pays",
    "Accidents graves\npar conditions météo",
    "Zones les plus\naccidentogènes"
]
sans_vue = [660, 57, 534]
avec_vue = [0.4, 0.017, 0.020]
gains    = [f"×{int(s/a):,}" for s, a in zip(sans_vue, avec_vue)]

# ─────────────────────────────────────────────────────────────
# FIGURE
# ─────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(10, 6))
fig.patch.set_facecolor("#ffffff")

x     = np.arange(len(requetes_labels))
width = 0.35

bars1 = ax.bar(x - width/2, sans_vue, width,
               color="#F09595", edgecolor="#E24B4A", linewidth=0.8)
bars2 = ax.bar(x + width/2, avec_vue, width,
               color="#5DCAA5", edgecolor="#1D9E75", linewidth=0.8)

ax.set_yscale("log")
ax.set_ylabel("Temps d'exécution (ms) — échelle logarithmique", fontsize=11, color="#5F5E5A")
ax.set_xticks(x)
ax.set_xticklabels(requetes_labels, fontsize=11)
ax.set_title("Impact des vues matérialisées sur les temps d'exécution",
             fontsize=13, fontweight="bold", color="#2C2C2A", pad=16)
ax.set_facecolor("#ffffff")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_color("#D3D1C7")
ax.spines["bottom"].set_color("#D3D1C7")
ax.tick_params(colors="#888780")
ax.grid(axis="y", color="#D3D1C7", linewidth=0.5, linestyle="--")

# Valeurs au-dessus des barres
for bar in bars1:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, h * 1.4,
            f"{h} ms", ha="center", va="bottom", fontsize=9, color="#A32D2D")

for bar in bars2:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, h * 1.4,
            f"{h} ms", ha="center", va="bottom", fontsize=9, color="#0F6E56")

# Légende
patch1 = mpatches.Patch(color="#F09595", label="Sans vue matérialisée")
patch2 = mpatches.Patch(color="#5DCAA5", label="Avec vue matérialisée")
ax.legend(handles=[patch1, patch2], fontsize=10, framealpha=0)


# EXPORT
plt.tight_layout(pad=2.0)
plt.savefig("docs/images/impact_vues_materialisees.png", dpi=150, bbox_inches="tight")
plt.show()
print("Sauvegardé dans docs/images/impact_vues_materialisees.png")