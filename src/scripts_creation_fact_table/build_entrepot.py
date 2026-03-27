from build_usager_vehicule_localisation_fait import build_fait
from preprocess_meteo import build_meteo
from build_pays import build_pays
from build_temps import build_temps


def main():
    build_fait()
    build_meteo()
    build_pays()
    build_temps()


if __name__ == "__main__":
    main()