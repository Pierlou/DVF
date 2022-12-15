from flask import Flask, jsonify
import json
from unidecode import unidecode
from markupsafe import escape
import psycopg2
import pandas as pd
from datetime import date

app = Flask(__name__)

config = pd.read_csv('config.csv', sep=',', dtype=str)
id = config['id'][0]
pwd = config['pwd'][0]
host = config['host'][0]
db = config['db'][0]
port = config['port'][0]

start_year = date.today().year - 1
start_month = '01' if date.today().month <= 6 else '06'
start_date = str(start_year) + "-" + start_month

conn = psycopg2.connect(
    host=host,
    database=db,
    user=id,
    password=pwd,
    port=port)


def create_moy_rolling_year(echelle_geo, code = None):
    with conn as connexion:
        sql = f"""
            SELECT
                code_geo,
                ROUND(SUM(tot) / NULLIF(SUM(nb), 0)) as prix_m2 
            FROM 
                (
                    SELECT 
                        (COALESCE(moy_prix_m2_maison * nb_ventes_maison, 0) + COALESCE(moy_prix_m2_appartement * nb_ventes_appartement, 0)) as tot,
                        COALESCE(nb_ventes_maison, 0) + COALESCE(nb_ventes_appartement, 0) as nb,
                        annee_mois,
                        code_geo
                    FROM stats_dvf 
                    WHERE 
                        echelle_geo='{echelle_geo}'
                    AND 
                        annee_mois > '{start_date}'
        """

        if echelle_geo == "commune":
            sql += f" AND SUBSTRING(code_geo, 1, 2) = '{code}'"

        if echelle_geo == "section":
            sql += f" AND SUBSTRING(code_geo, 1, 5) = '{code}'"

        sql += """
                ) tbl1
            GROUP BY code_geo;
        """

        with connexion.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data=cursor.fetchall()
    return jsonify({"data": [{k:v for k,v in zip(columns, d)} for d in data]})

def process_geo(echelle_geo, code):
    with conn as connexion:
        sql = f"SELECT * FROM stats_dvf WHERE echelle_geo='{echelle_geo}' AND code_geo = '{code}'"
        with connexion.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data=cursor.fetchall()
    return jsonify({"data": [{k:v for k,v in zip(columns, d)} for d in data]})


@app.route("/")
def hello_world():
    return "<p>Données DVF agrégées</p>"


@app.route('/nation')
def get_nation():
    with conn as connexion:
        with connexion.cursor() as cursor:
            cursor.execute("""SELECT * FROM stats_dvf WHERE echelle_geo='nation'""")
            columns = [desc[0] for desc in cursor.description]
            data=cursor.fetchall()
    return jsonify({"data": [{k:v for k,v in zip(columns, d)} for d in data]})


@app.route('/departement')
@app.route('/departement/<code>')
def get_departement(code = None):
    if code:
       return process_geo("departement", code)
    else:
       return create_moy_rolling_year("departement")


@app.route('/epci')
@app.route('/epci/<code>')
def get_epci(code = None):
    if code:
        return process_geo("epci", code)
    else:
       return create_moy_rolling_year("epci")


@app.route('/commune')
@app.route('/commune/<code>')
def get_commune(code = None):
    if code:
        return process_geo("commune", code)
    else:
        return jsonify({"message": "Veuillez rentrer un numéro de commune."})


@app.route('/departement/<code>/communes')
def get_commune_from_dep(code = None):
    return create_moy_rolling_year("commune", code)

@app.route('/commune/<code>/sections')
def get_section_from_commune(code = None):
    return create_moy_rolling_year("section", code)


@app.route('/section')
@app.route('/section/<code>')
def get_section(code = None):
    if code:
        return process_geo("section", code)
    else:
        return jsonify({"message": "Veuillez rentrer un numéro de section."})


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
    
    with conn as connexion:
        with connexion.cursor() as cursor:
            if len(queries)==0:
                cursor.execute("""SELECT * FROM stats_dvf""")
            else:
                cursor.execute(f"""SELECT * FROM stats_dvf WHERE """ + ' AND '.join(queries))
            columns = [desc[0] for desc in cursor.description]
            data=cursor.fetchall()
    return jsonify({"data": [{k:v for k,v in zip(columns, d)} for d in data]})
    

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=3030, debug=True)
