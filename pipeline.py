import numpy as np
import pandas as pd
import os
import time
from unidecode import unidecode
import swifter
from datetime import date

years = [2017,2018,2019,2020,2021,2022]
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
epci= pd.read_csv('epci.csv', sep=',', encoding= 'utf8', dtype=str)
export = pd.DataFrame(None)
sections_from_dvf = set()
communes_from_dvf = set()
for year in range(min(years), max(years)+1):
    df_ = pd.read_csv(
        f'full_{year}.csv', 
        sep=',', 
        encoding= 'utf8',
        dtype={"code_commune" : str,
               "code_departement" : str,},
        usecols=to_keep,
        )
    df = df_.drop_duplicates()
    df = pd.merge(df, epci[['code_commune', 'code_epci']], on='code_commune', how='left')
    df['code_section'] = df['id_parcelle'].str[:10]
    sections_from_dvf = sections_from_dvf | set(df['code_section'].unique())
    communes_from_dvf = communes_from_dvf | set(df['code_commune'].unique())
    df = df.drop('id_parcelle', axis=1)

    natures_of_interest = ['Vente', "Vente en l'état futur d'achèvement", 'Adjudication']
    types_bien = {k: v for k,v in df_[['code_type_local', 'type_local']].value_counts().to_dict().keys()}
    del df_
    # types_bien = {
    #     1: "Maison",
    #     2: "Appartement",
    #     3: "Dépendance",
    #     4: "Local industriel. commercial ou assimilé",
    #     NaN: terres (cf nature_culture)
    # }

    ## filtres sur les ventes et les types de biens à considérer
    ## choix : les terres et/ou dépendances ne rendent pas une mutation multitype
    ventes = df.loc[(df['nature_mutation'].isin(natures_of_interest)) & (df['code_type_local'].isin([1,2,4]))]
    ventes['month'] = ventes['date_mutation'].swifter.apply(lambda x: int(x.split('-')[1]))
    print(len(ventes))

    ## drop mutations multitypes pour les prix au m², impossible de classer une mutation qui contient X maisons et Y appartements par exemple
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
            grouped = ventes_nodup.loc[ventes_nodup['code_type_local'].isin(types_of_interest)].groupby([f'code_{echelle}', 'month', 'type_local'])['prix_m2']
            mean = grouped.mean()
            mean_ = mean.loc[mean.index.get_level_values(1)==m].unstack().reset_index().drop('month', axis=1)
            mean_.columns = ['moy_prix_m2_'+unidecode(c.split(' ')[0].lower()) if c != f'code_{echelle}' else c for c in mean_.columns]

            median = grouped.median()
            median_ = median.loc[median.index.get_level_values(1)==m].unstack().reset_index().drop('month', axis=1)
            median_.columns = ['med_prix_m2_'+unidecode(c.split(' ')[0].lower()) if c != f'code_{echelle}' else c for c in median_.columns]

            merged = pd.merge(nb_, mean_, on=[f'code_{echelle}'])
            merged = pd.merge(merged, median_, on=[f'code_{echelle}'])
            for c in merged.columns:
                if any([k in c for k in ['moy_', 'med_']]):
                    merged[c] = merged[c].round()
            
            merged.rename(columns={f'code_{echelle}':'code_geo'}, inplace=True)
            dfs_dict[echelle] = merged

        general = {'code_geo': 'nation'}
        for t in types_of_interest:
                    general['nb_ventes_'+unidecode(types_bien[t].split(' ')[0].lower())] = len(ventes.loc[(ventes['code_type_local']==t) & (ventes['month']==m)])
                    general['moy_prix_m2_'+unidecode(types_bien[t].split(' ')[0].lower())] = np.round(ventes_nodup.loc[(ventes_nodup['code_type_local']==t) & (ventes_nodup['month']==m)]['prix_m2'].mean())
                    general['med_prix_m2_'+unidecode(types_bien[t].split(' ')[0].lower())] = np.round(ventes_nodup.loc[(ventes_nodup['code_type_local']==t) & (ventes_nodup['month']==m)]['prix_m2'].median())

        all_month = pd.concat(list(dfs_dict.values()) + [pd.DataFrame([general])])
        all_month['annee_mois'] = f'{year}-{"0"+str(m) if m<10 else m}'
        export = pd.concat([export, all_month])
        del all_month
    del ventes
    del ventes_nodup
    print("Done with", year)
with open("sections.txt", 'r') as f:
    sections = [s.replace('\n', '') for s in f.readlines()]
    f.close()
sections = pd.DataFrame(set(sections) | sections_from_dvf, columns=['code_geo'])
sections['code_geo'] = sections['code_geo'].str.slice(0,10)
sections = sections.drop_duplicates()
sections['code_parent'] = sections['code_geo'].str.slice(0,5)
sections['libelle_geo'] = sections['code_geo']
sections['echelle_geo'] = 'section'
print("Done with sections")
departements = pd.read_csv('departement_2022.csv', dtype=str, usecols=['DEP', 'LIBELLE'])
departements = departements.rename({'DEP':'code_geo', 'LIBELLE': 'libelle_geo'}, axis=1)
departements['code_parent'] = 'nation'
departements['echelle_geo'] = 'departement'
print("Done with departements")
epci_communes = epci[['code_commune', 'code_epci']]
epci['code_parent'] = epci['code_commune'].apply(lambda code: code[:2] if code[:2]!='97' else code[:3])
epci = epci.drop('code_commune', axis=1)
epci = epci.drop_duplicates(subset=['code_epci', 'code_parent']).rename({'code_epci': 'code_geo'}, axis=1)
epci['echelle_geo'] = 'epci'
print("Done with EPCI")
communes = pd.read_csv('commune_2022.csv', dtype=str, usecols=['TYPECOM', 'COM', 'LIBELLE'])
communes = communes.loc[communes['TYPECOM'].isin(['COM', 'ARM'])].rename({'COM':'code_geo', 'LIBELLE': 'libelle_geo'}, axis=1).drop('TYPECOM', axis=1)
communes = pd.merge(communes, pd.DataFrame(communes_from_dvf, columns=['code_geo']), on='code_geo', how='outer')
epci_communes = epci_communes.rename({'code_commune': 'code_geo', 'code_epci': 'code_parent'}, axis=1)
communes = pd.merge(communes, epci_communes, on='code_geo', how='outer')
communes.loc[communes['code_geo'].str.startswith('75'), 'code_parent'] = '200054781'
communes.loc[communes['code_geo'].str.startswith('13'), 'code_parent'] = '200054807'
communes.loc[communes['code_geo'].str.startswith('69'), 'code_parent'] = '200046977'
communes['code_parent'].fillna(communes['code_geo'].str.slice(0,2), inplace=True)
communes['libelle_geo'].fillna('NA', inplace=True)
communes['echelle_geo'] = 'commune'
print("Done with communes")
libelles_parents = pd.concat([departements, epci, communes, sections])
libelles_parents['libelle_geo'] = libelles_parents['libelle_geo'].fillna("NA").apply(unidecode)
# on crée l'ensemble des occurrences échelles X mois pour le merge
dup_libelle = libelles_parents.append([libelles_parents]*(12*(max(years)-min(years)+1)-1),ignore_index=True).sort_values('code_geo')
dup_libelle['annee_mois']=[f'{y}-{"0"+str(m) if m<10 else m}' for y in range(min(years), max(years)+1) for m in range(1,13)]*len(libelles_parents)
del libelles_parents

# export = pd.merge(export, dup_libelle, on=['code_geo', 'annee_mois'], how='outer')
dup_libelle.set_index(['code_geo', 'annee_mois'], inplace=True)
export = export.join(dup_libelle, on=['code_geo', 'annee_mois'], how='outer')
print("Done with big merge")
del dup_libelle
# si on est à cheval sur 6 ans, suppression des lignes vides
if len(years)>5:
     export = export.loc[export['annee_mois'].between(f'{min(years)}-07', f'{max(years)}-06')]
mask = export['code_geo']=='nation'
export.loc[mask, ['code_parent', 'libelle_geo', 'echelle_geo']] = [['-', 'nation', 'nation'] for k in range(sum(mask))]

export.to_csv('files/stats_dvf.csv', sep=',', encoding='utf8', index=False, float_format='%.0f')