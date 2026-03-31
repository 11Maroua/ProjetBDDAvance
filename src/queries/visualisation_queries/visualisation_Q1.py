import pandas as pd
import matplotlib.pyplot as plt


SAVE_FILE = "docs/images/"
# Create the dataset
data = {
    "annee": [2005, 2005, 2007, 2007, 2009, 2009, 2011, 2011,
              2013, 2013, 2015, 2015, 2017, 2017, 2019, 2019,
              2021, 2021],
    "nom_pays": ["France", "United Kingdom"] * 9,
    "nb_accidents_mortels": [5068, 2913, 4466, 2714, 4115, 2057,
                             3788, 1797, 3171, 1608, 3306, 1616,
                             3360, 1676, 3284, 1658, 3032, 1474]
}

# Convert to DataFrame
df = pd.DataFrame(data)

# Pivot the data to have countries as columns
df_pivot = df.pivot(index="annee", columns="nom_pays", values="nb_accidents_mortels")

# Plot
plt.figure(figsize=(10, 6))
for country in df_pivot.columns:
    plt.plot(df_pivot.index, df_pivot[country], marker='o', label=country)

# Customize the plot
plt.title("Evolution des accidents mortels (2005–2021)")
plt.xlabel("Année")
plt.ylabel("Nombre d'accidents mortels")
plt.grid(True)
plt.legend()
plt.tight_layout()

# Show plot
plt.show()
plt.savefig(SAVE_FILE +"evolutionaccidents.png")