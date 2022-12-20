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