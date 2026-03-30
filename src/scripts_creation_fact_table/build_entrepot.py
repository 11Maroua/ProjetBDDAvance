from build_usager_vehicule_localisation_fait import build_fait
from preprocess_meteo import build_meteo
from build_pays import build_pays
from build_temps import build_temps

from pathlib import Path


def main():
    Path("data/dims").mkdir(parents=True, exist_ok=True)
    build_meteo()
    build_pays()
    build_temps()
    build_fait()


if __name__ == "__main__":
    main()