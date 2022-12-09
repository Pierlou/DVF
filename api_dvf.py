from flask import Flask, jsonify
import json
from sqlalchemy import create_engine
import pandas as pd
from ast import literal_eval
from unidecode import unidecode
from markupsafe import escape

app = Flask(__name__)

config = pd.read_csv('config.csv', sep=',')
id = config['id'][0]
pwd = config['pwd'][0]
host = config['host'][0]
db = config['db'][0]

engine = create_engine(f'postgresql://{id}:{pwd}@{host}/{db}')


@app.route("/")
def hello_world():
    return "<p>Données DVF agrégées</p>"


@app.route('/nation')
def get_nation():
    mutations = pd.read_sql("""SELECT * FROM stats_dvf WHERE echelle_geo='nation'""", engine)
    dict_mutations = {'data': json.loads(mutations.to_json(orient = 'records'))}
    return jsonify(dict_mutations)


@app.route('/geo')
@app.route('/geo/<echelle_geo>')
@app.route('/geo/<echelle_geo>/<code_geo>/')
@app.route('/geo/<echelle_geo>/<code_geo>/from=<dateminimum>&to=<datemaximum>')
def get_echelle(echelle_geo= None, code_geo=None, dateminimum=None, datemaximum= None):
    if echelle_geo is None:
        echelle_query = ''
    else:
        echelle_query = f"echelle_geo='{escape(echelle_geo)}'"
        
    if code_geo is None:
        code_query = ''
    else:
        code_query = f"code_geo='{escape(code_geo)}'"
        
    if dateminimum is None or datemaximum is None :
        date_query = ''
    else:
        date_query = f"annee_mois>='{escape(dateminimum)}' AND annee_mois<='{escape(datemaximum)}'"
    
    queries = [echelle_query, code_query, date_query]
    queries = [q for q in queries if q!='']
    
    if len(queries)==0:
        mutations = pd.read_sql("""SELECT * FROM stats_dvf""",
            engine)
    else:
        mutations = pd.read_sql(f"""SELECT * FROM stats_dvf WHERE """ + ' AND '.join(queries),
            engine)
    dict_mutations = {'data': json.loads(mutations.to_json(orient = 'records'))}
    return jsonify(dict_mutations)
    

# @app.route('/geo/<echelle_geo>')
# @app.route('/geo/<echelle_geo>/<code_geo>')
# def get_echelle(echelle_geo, code_geo=None):
#     if code_geo is None:
#         mutations = pd.read_sql(f"""
#         SELECT * FROM stats_dvf 
#         WHERE echelle_geo='{escape(echelle_geo)}'""",
#             engine)
#         dict_mutations = {'data': json.loads(mutations.to_json(orient = 'records'))}
#         return jsonify(dict_mutations)
#     else:
#         mutations = pd.read_sql(f"""
#         SELECT * FROM stats_dvf WHERE
#         echelle_geo='{escape(echelle_geo)}' AND
#         code_geo='{escape(code_geo)}'
#         """,
#             engine)
#         dict_mutations = {'data': json.loads(mutations.to_json(orient = 'records'))}
#         return jsonify(dict_mutations)


# @app.route('/geo/<echelle_geo>&<code_geo>&from=<dateminimum>&to=<datemaximum>')
# def get_echelle_code_mois(echelle_geo, code_geo, dateminimum, datemaximum):
#     mutations = pd.read_sql(f"""
#     SELECT * FROM stats_dvf WHERE 
#     echelle_geo='{escape(echelle_geo)}' AND
#     code_geo='{escape(code_geo)}' AND
#     annee_mois>='{escape(dateminimum)}' AND
#     annee_mois<='{escape(datemaximum)}'
#     """,
#         engine)
#     dict_mutations = {'data': json.loads(mutations.to_json(orient = 'records'))}
#     return jsonify(dict_mutations)
