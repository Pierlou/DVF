from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import numpy as np
import pandas as pd
import os
from datetime import date
from unidecode import unidecode
import swifter
import gc
import psycopg2
from bs4 import BeautifulSoup
import requests
import DVF.config as config

DATADIR= config.DATADIR

def get_epci():
    page = requests.get('https://unpkg.com/@etalab/decoupage-administratif/data/epci.json')
    epci = page.json()
    data = {e['code']: [m['code'] for m in e['membres']] for e in epci}
    epci_list = [[k, m]for k in list(data.keys()) for m in data[k]]
    pd.DataFrame(epci_list, columns=['code_epci', 'code_commune']).to_csv(DATADIR+'/epci.csv', sep=',', encoding='utf8', index=False)

def pipeline(ti):
    export = pd.DataFrame(None)
    epci= pd.read_csv(DATADIR+'/epci.csv', sep=',', encoding= 'utf8', dtype=str)
    to_keep = [
        'id_mutation',
        'date_mutation',
        'code_departement',
        'code_commune',
        'id_parcelle',
        'nature_mutation',
        'code_type_local',
        'type_local',
        'valeur_fonciere',
        'surface_reelle_bati'
    ]
    for year in range(date.today().year-5, date.today().year+1):
        df_ = pd.read_csv(
            DATADIR+f'/full_{year}.csv', 
            # DATADIR+'f'/full_{year}.csv.gz', 
            # compression='gzip',
            sep=',',
            encoding= 'utf8',
            dtype={"code_commune" : str,
                    "code_postal" : str,
                    "code_departement" : str,
                    "ancien_nom_commune" : str,
                    "code_nature_culture_speciale" : str,
                    "nature_culture_speciale" : str,},
#                nrows=500000
                    )
        df = df_[to_keep]
        ## certaines communes ne sont pas dans des EPCI
        df = pd.merge(df, epci, on='code_commune', how='left')
        df['code_section'] = df['id_parcelle'].str[:10]
        df = df.drop('id_parcelle', axis=1)
        
        natures_of_interest = ['Vente', "Vente en l'état futur d'achèvement", 'Adjudication']
        types_bien = {k: v for k,v in df_[['code_type_local', 'type_local']].value_counts().to_dict().keys()}
        del(df_)

        # types_bien = {
        #     1: "Maison",
        #     2: "Appartement",
        #     3: "Dépendance",
        #     4: "Local industriel. commercial ou assimilé",
        # }

        ## filtres
        ventes = df.loc[df['nature_mutation'].isin(natures_of_interest)]
        del(df)
        ventes['month'] = ventes['date_mutation'].swifter.progress_bar(False).apply(lambda x: int(x.split('-')[1]))
        print(len(ventes))

        ## drop mutations multitypes pour les prix au m², impossible de classer une mutation qui contient X maisons et Y appartements par exemple
        ## à voir pour la suite : quid des mutations avec dépendances, dont le le prix est un prix de lot ? prix_m2 = prix_lot/surface_bien ?
        multitypes = ventes[['id_mutation', 'code_type_local']].value_counts()
        multitypes_ = multitypes.unstack()
        mutations_drop = multitypes_.loc[sum([multitypes_[c].isna() for c in multitypes_.columns])<len(multitypes_.columns)-1].index
        ventes_nodup = ventes.loc[~(ventes['id_mutation'].isin(mutations_drop)) & ~(ventes['code_type_local'].isna())]
        print(len(ventes_nodup))

        ## group par mutation, on va chercher la surface totale de la mutation pour le prix au m²
        surfaces = ventes_nodup.groupby(['id_mutation'])['surface_reelle_bati'].sum().reset_index()
        surfaces.columns = ['id_mutation', 'surface_totale_mutation']
        ## avec le inner merge sur surfaces on se garantit aucune ambiguïté sur les type_local
        ventes_nodup = ventes.drop_duplicates(subset = 'id_mutation')
        ventes_nodup = pd.merge(ventes_nodup, surfaces, on='id_mutation', how='inner')
        print(len(ventes_nodup))
        
        ## pour une mutation donnée la valeur foncière est globale, on la divise par la surface totale, sachant qu'on n'a gardé que les mutations monotypes
        ventes_nodup['prix_m2'] = ventes_nodup['valeur_fonciere']/ventes_nodup['surface_totale_mutation']
        ventes_nodup['prix_m2'] = ventes_nodup['prix_m2'].replace([np.inf, -np.inf], np.nan)
        
        ## pas de prix ou pas de surface
        ventes_nodup = ventes_nodup.dropna(subset=['prix_m2'])

        types_of_interest = [1, 2, 4]
        echelles_of_interest = ['departement', 'epci', 'commune', 'section']
        
        for m in range(1, 13):
            dfs_dict= {}
            for echelle in echelles_of_interest:
                ## ici on utilise bien le df ventes, qui contient l'eneemble des ventes sans filtres autres que les types de mutations d'intérêt
                nb = ventes.loc[ventes['code_type_local'].isin(types_of_interest)].groupby([f'code_{echelle}', 'month', 'type_local'])['valeur_fonciere'].count()
                nb_ = nb.loc[nb.index.get_level_values(1)==m].unstack().reset_index().drop('month', axis=1)
                nb_.columns = ['nb_ventes_'+unidecode(c.split(' ')[0].lower()) if c != f'code_{echelle}' else c for c in nb_.columns]
                
                ## pour mean et median on utlise le df nodup, dans lequel on a drop_dup sur les mutations
                mean = ventes_nodup.loc[ventes_nodup['code_type_local'].isin(types_of_interest)].groupby([f'code_{echelle}', 'month', 'type_local'])['prix_m2'].mean()
                mean_ = mean.loc[mean.index.get_level_values(1)==m].unstack().reset_index().drop('month', axis=1)
                mean_.columns = ['moy_prix_m2_'+unidecode(c.split(' ')[0].lower()) if c != f'code_{echelle}' else c for c in mean_.columns]

                median = ventes_nodup.loc[ventes_nodup['code_type_local'].isin(types_of_interest)].groupby([f'code_{echelle}', 'month', 'type_local'])['prix_m2'].median()
                median_ = median.loc[median.index.get_level_values(1)==m].unstack().reset_index().drop('month', axis=1)
                median_.columns = ['med_prix_m2_'+unidecode(c.split(' ')[0].lower()) if c != f'code_{echelle}' else c for c in median_.columns]

                merged = pd.merge(nb_, mean_, on=[f'code_{echelle}'])
                merged = pd.merge(merged, median_, on=[f'code_{echelle}'])
                for c in merged.columns:
                    if any([k in c for k in ['moy_', 'med_']]):
                        merged[c] = merged[c].round()
                
                merged.rename(columns={f'code_{echelle}':'code_geo'}, inplace=True)
                merged['echelle_geo'] = echelle
                dfs_dict[echelle] = merged

            general = {'code_geo': 'all', 'echelle_geo': 'nation'}
            for t in types_of_interest:
                        general['nb_ventes_'+unidecode(types_bien[t].split(' ')[0].lower())] = len(ventes.loc[(ventes['code_type_local']==t) & (ventes['month']==m)])
                        general['moy_prix_m2_'+unidecode(types_bien[t].split(' ')[0].lower())] = np.round(ventes_nodup.loc[(ventes_nodup['code_type_local']==t) & (ventes_nodup['month']==m)]['prix_m2'].mean())
                        general['med_prix_m2_'+unidecode(types_bien[t].split(' ')[0].lower())] = np.round(ventes_nodup.loc[(ventes_nodup['code_type_local']==t) & (ventes_nodup['month']==m)]['prix_m2'].median())

            all_month = pd.concat(list(dfs_dict.values()) + [pd.DataFrame([general])])
            all_month['annee_mois'] = f'{year}-{"0"+str(m) if m<10 else m}'
            export = pd.concat([export, all_month])
        del(ventes)
        del(ventes_nodup)
        #os.remove(DATADIR+f'/full_{year}.csv')
        gc.collect()
        print("Done with", year)
    columns_for_sql = [c for c in export.columns if any([pref in c for pref in ['nb_', 'moy_', 'med_']])]
    rows = ' FLOAT,\n'.join(columns_for_sql) +  ' FLOAT,'
    query = f"""
    DROP TABLE IF EXISTS stats_dvf CASCADE;
    CREATE UNLOGGED TABLE stats_dvf (
    code_geo VARCHAR(50),
    echelle_geo VARCHAR(15),
    {rows}
    annee_mois VARCHAR(7),
    PRIMARY KEY (echelle_geo, code_geo, annee_mois));
    """
    ti.xcom_push(key='query', value=query)
    export.to_csv(DATADIR+'/stats_dvf.csv', sep=',', encoding='utf8', index=False)

credentials = {
    'user' : config.PG_ID,
    'password' : config.PG_PWD,
    'host' : config.DOCKER_HOST,
    'database' : config.PG_DB,
    'port' : config.PG_PORT,
}

def send_csv_to_psql(connection, csv, table_):
    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    file = open(csv, "r")
    table = table_
    with connection.cursor() as cur:
        cur.execute("truncate " + table + ";")  #avoiding uploading duplicate data!
        cur.copy_expert(sql=sql % table, file=file)
        connection.commit()
        # cur.close()
        # connection.close()
    return connection.commit()

def upload():
    conn = psycopg2.connect(**credentials)
    cur = conn.cursor()
    conn.autocommit = True
    send_csv_to_psql(conn, DATADIR+'/stats_dvf.csv', 'stats_dvf')

################################################################################

default_args = {
    'owner': 'me',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_dvf',
    default_args=default_args,
    description='DAG DVF',
    start_date=datetime(2022, 11, 29),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='download_dvf',
        bash_command="sh /opt/airflow/dags/DVF/DAGs/script_dl_dvf.sh "
    )

    task2 = PythonOperator(
        task_id='get_epci',
        python_callable=get_epci,
    )

    task3 = PythonOperator(
        task_id='process_dvf',
        python_callable=pipeline,
    )

    task4 = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_localhost',
    ## à changer dans l'idéal pour adapter les colonnes selon les paramètres dans la process_dvf, mais actuellement pas de solution
    # sql="""(%s, '{{ ti.xcom_pull(task_ids= 'process_dvf', key='query') }}', %s)"""
    sql="""
        DROP TABLE IF EXISTS stats_dvf CASCADE;
        CREATE UNLOGGED TABLE stats_dvf (
        code_geo VARCHAR(50),
        echelle_geo VARCHAR(15),
        nb_ventes_maison FLOAT,
        moy_prix_m2_maison FLOAT,
        med_prix_m2_maison FLOAT,
        nb_ventes_appartement FLOAT,
        moy_prix_m2_appartement FLOAT,
        med_prix_m2_appartement FLOAT,
        nb_ventes_local FLOAT,
        moy_prix_m2_local FLOAT,
        med_prix_m2_local FLOAT,
        annee_mois VARCHAR(7),
        PRIMARY KEY (echelle_geo, code_geo, annee_mois));
        """ 
    )

    task5 = PythonOperator(
        task_id='upload_table',
        python_callable=upload,
    )

    task1 >> task2 >> task3 >> task4 >> task5