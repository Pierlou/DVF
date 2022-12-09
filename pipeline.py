import numpy as np
import pandas as pd
import os
import time
from unidecode import unidecode
import swifter

export = pd.DataFrame(None)
for year in [2017, 2018, 2019, 2020, 2021, 2022]:
    df_ = pd.read_csv(f'full_{year}.csv', sep=',', encoding= 'utf8',
                     dtype={"code_commune" : str,
                            "code_postal" : str,
                            "code_departement" : str,
                            "ancien_nom_commune" : str,
                            "code_nature_culture_speciale" : str,
                            "nature_culture_speciale" : str,},
    #                nrows=500000
                   )
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
    df = df_[to_keep]
    df['code_section'] = df['id_parcelle'].str[:10]
    df = df.drop('id_parcelle', axis=1)

    natures_of_interest = ['Vente', "Vente en l'état futur d'achèvement", 'Adjudication']
    types_bien = {k: v for k,v in df_[['code_type_local', 'type_local']].value_counts().to_dict().keys()}

    # types_bien = {
    #     1: "Maison",
    #     2: "Appartement",
    #     3: "Dépendance",
    #     4: "Local industriel. commercial ou assimilé",
    # }

    ## filtres
    ventes = df.loc[df['nature_mutation'].isin(natures_of_interest)]
    ventes['month'] = ventes['date_mutation'].swifter.apply(lambda x: int(x.split('-')[1]))
    print(len(ventes))

    ## drop mutations multitypes pour les prix au m², impossible de classr une mutation qui contient X maisons et Y appartements par exemple
    multitypes = ventes[['id_mutation', 'code_type_local']].value_counts()
    multitypes = multitypes.loc[multitypes>1].unstack()
    mutations_drop = multitypes.loc[sum([multitypes[c].isna() for c in multitypes.columns])<len(multitypes.columns)-1].index
    ventes_nodup = ventes.loc[~ventes['id_mutation'].isin(mutations_drop)]
    print(len(ventes_nodup))

    ## group par mutation, on va chercher la surface totale de la mutation pour le prix au m²
    surfaces = ventes_nodup.groupby(['id_mutation'])['surface_reelle_bati'].sum().reset_index()
    surfaces.columns = ['id_mutation', 'surface_totale_mutation']
    ventes_nodup = ventes.drop_duplicates(subset = 'id_mutation')
    print(len(ventes_nodup))
    ventes_nodup = pd.merge(ventes_nodup, surfaces, on='id_mutation')
    
    ## pour une mutation donnée la valeur foncière est globale, on la divise par la surface totale, sachant qu'on n'a gardé que les mutations monotypes
    ventes_nodup['prix_m2'] = ventes_nodup['valeur_fonciere']/ventes_nodup['surface_totale_mutation']
    ventes_nodup['prix_m2'] = ventes_nodup['prix_m2'].replace([np.inf, -np.inf], np.nan)

    ## pas de prix ou pas de surface
    ventes_nodup = ventes_nodup.dropna(subset=['prix_m2'])

    types_of_interest = [1, 2, 4]
    echelles_of_interest = ['departement', 'commune', 'section']
    
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
    print("Done with", year)
    
export.to_csv('files/stats_dvf.csv', sep=',', encoding='utf8', index=False)