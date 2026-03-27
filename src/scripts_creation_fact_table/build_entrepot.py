from build_usager_vehicule_localisation_fait import build_fait
from preprocess_meteo import build_meteo
from build_pays import build_pays

def main():
    build_fait()
    build_meteo()
    build_pays()



if __name__ == "__main__":
    main()