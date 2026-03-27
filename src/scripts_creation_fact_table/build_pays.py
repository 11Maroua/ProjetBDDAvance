import pandas as pd


def build_pays():
    # DIM_PAYS
    dim_pays = pd.DataFrame([
        {"id_pays": 1, "code_pays": "FR", "nom_pays": "France"},
        {"id_pays": 2, "code_pays": "UK", "nom_pays": "Royaume-Uni"},
    ])
    dim_pays.to_csv("data/dims/dim_pays.csv", index=False)
    print("DIM_PAYS :")
    print(dim_pays)