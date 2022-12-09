# Travaux sur la refonte de l'app DVF
Ce repo contient les fichiers issus des travaux pour une nouvelle version de l'[app DVF](https://app.dvf.etalab.gouv.fr/).

## Pipeline
Le fichier pipeline.py permet de générer des statistiques à partir des [données des demandes de valeurs foncières](https://files.data.gouv.fr/geo-dvf/latest/csv/), agrégées à différentes échelles, et leur évolution dans le temps (au mois). Le choix a été fait de calculer les indicateurs suivants :
* nombre de mutations
* moyenne des prix au m²
* médiane des prix au m²

pour chaque type de bien sélectionné (parmi : maisons, appartements, locaux, dépendances). Pour plus de cohérence, les mutations "multitypes" sont retirées pour le calcul des prix au m², mais conservées pour le dénombrement.

_NB : pour l'échelle [EPCI](https://www.collectivites-locales.gouv.fr/institutions/les-epci), il est nécessaire de télécharger également [ces données](https://www.collectivites-locales.gouv.fr/institutions/liste-et-composition-des-epci-fiscalite-propre)_

## DAG
Le répertoire [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html) contient les scripts du DAG Airflow qui sera utilisé pour alimenter l'app DVF en production. Le processus suit les étapes suivantes :
* récupération des données DVF à date
* récupération des données EPCI à date
* traitement des données brutes pour créer les indicateurs
* export et exposition en postgres pour accessibilité

## API
Le fichier api_dvf.py permet de setup une API Flask sur la base de données des statistiques créée par le DAG.