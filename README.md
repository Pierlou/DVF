# Travaux sur la refonte de l'app DVF
Ce repo contient les fichiers issus des travaux pour une nouvelle version de l'[app DVF](https://app.dvf.etalab.gouv.fr/).

## Pipeline
Le fichier pipeline.py permet de générer des statistiques à partir des [données des demandes de valeurs foncières](https://files.data.gouv.fr/geo-dvf/latest/csv/), agrégées à différentes échelles, et leur évolution dans le temps (au mois). Le choix a été fait de calculer les indicateurs suivants :
* nombre de mutations
* moyenne des prix au m²
* médiane des prix au m²

pour chaque type de bien sélectionné (parmi : maisons, appartements, locaux). Lorsqu'une mutation comporte plusieurs types de biens (par exemple un appartement et une maison), il est impossible d'évaluer précisément le poids de chaque bien dans le prix total de la mutation. Toutes ces mutations dites "multitypes" sont donc écartées pour le calcul des prix au m², mais elles restent comptabilisées pour leur dénombrement. Le choix a été fait de ne pas considérer les terres ni les dépendances comme des types de biens à part entière pour qualifier les mutations de multitypes ou non. En conséquence, une mutation qui comporte, par exemple, trois appartements et deux dépendances, est considérée comme monotype, et le prix au m² est calculé comme suit :

$$prix\ m²\ de\ la\ mutation = \frac{prix\ total\ de\ la\ mutation}{\sum{surfaces\ des\ biens\ de\ la\ mutation}}$$
_NB : les surfaces des terres et dépendances ne sont comptabilisées dans le calcul du prix au m²._

Cela affecte les prix calculés dans la mesure où, par exemple, une maison avec ou sans garage est considérée comme le même type de mutation, mais permet de garder une quantité bien supérieure de mutations pour les calculs, donc une plus grande précision. Ce choix d'exclure les mutations multitypes conduit également à des cas où, pour une échelle et un mois donné, il peut y avoir un nombre non nul de ventes d'un certain type de bien, mais aucune information sur le prix au m² pour ce type de bien : c'est que toutes les mutations incluant ce type de bien contenaient également d'autres types de biens.

_NB : pour l'échelle [EPCI](https://www.collectivites-locales.gouv.fr/institutions/les-epci), il est nécessaire de télécharger également [ces données](https://www.collectivites-locales.gouv.fr/institutions/liste-et-composition-des-epci-fiscalite-propre)._

## DAG
Le répertoire DAGs contient les scripts du [DAG Airflow](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html) qui sera utilisé pour alimenter l'app DVF en production. Le processus suit les étapes suivantes :
* récupération des données DVF à date
* récupération des données EPCI à date
* traitement des données brutes pour créer les indicateurs (basé sur pipeline.py)
* export et exposition en postgres pour accessibilité

## API
Le fichier api_dvf.py permet de setup une API Flask sur la base de données des statistiques créée par le DAG.
Les informations retournées par endpoint de l'API sont :

* la moyenne du prix au m² et le nombre de mutations sur un an glissant, maisons et appartements confondus, et le nombre total de mutations sur 5 ans (antériorité maximale des données DVF), pour les niveaux d'échelle suivants :
    - /departement (tous les départements)
    - /epci (toutes les EPCI, ou /departement/<code_departement>/epci pour une sélection restreinte)
    - /epci/<code_epci>/communes (toutes les communes de l'EPCI sélectionnée)
    - /commune/<code_commune>/sections (toutes les sections de la commune sélectionnée)

Par exemple, _/epci/246300701/communes_ renvoit une liste d'éléments comme suit :
```
{
    "code_geo": "63075",
    "code_parent": "246300701",
    "libelle_geo": "Chamalieres",
    "moy_prix_m2_rolling_year": 2563.0,
    "nb_mutations_all_5_ans": 2292.0,
    "nb_mutations_apparts_maisons_rolling_year": 104.0
},
{
    "code_geo": "63113",
    "code_parent": "246300701",
    "libelle_geo": "Clermont-Ferrand",
    "moy_prix_m2_rolling_year": 2456.0,
    "nb_mutations_all_5_ans": 13370.0,
    "nb_mutations_apparts_maisons_rolling_year": 503.0
},
...
```

* la moyenne, la médiane et le nombre de mutations, pour chaque type de biens (maison, appartement et local), pour les mois où au moins une mutation a eu lieu, pour les niveaux d'échelle suivants :
    - /nation
    - /departement/<code_departement>
    - /epci/<code_epci>
    - /commune/<code_commune>

Par exemple, _/departement/44_ renvoit une liste d'éléments comme suit :
```
{
    "annee_mois": "2017-07",
    "code_geo": "44",
    "code_parent": "all",
    "echelle_geo": "departement",
    "libelle_geo": "Loire-Atlantique",
    "med_prix_m2_appartement": 2715.0,
    "med_prix_m2_local": 1311.0,
    "med_prix_m2_maison": 2287.0,
    "moy_prix_m2_appartement": 2909.0,
    "moy_prix_m2_local": 1727.0,
    "moy_prix_m2_maison": 2416.0,
    "nb_ventes_appartement": 1066.0,
    "nb_ventes_local": 184.0,
    "nb_ventes_maison": 1651.0
},
{
    "annee_mois": "2017-08",
    "code_geo": "44",
    "code_parent": "all",
    "echelle_geo": "departement",
    "libelle_geo": "Loire-Atlantique",
    "med_prix_m2_appartement": 2632.0,
    "med_prix_m2_local": 1113.0,
    "med_prix_m2_maison": 2286.0,
    "moy_prix_m2_appartement": 2793.0,
    "moy_prix_m2_local": 1609.0,
    "moy_prix_m2_maison": 2416.0,
    "nb_ventes_appartement": 843.0,
    "nb_ventes_local": 113.0,
    "nb_ventes_maison": 1338.0
},
...
```

## Cohérence et statistiques
Le notebook Jupyter inconsistencies.ipynb contient des tests et visualisations sur les données DVF brutes. Ce travail a pour but de remonter de potentielles anomalies dans les données afin d'en améliorer la qualité après retour aux services compétents.