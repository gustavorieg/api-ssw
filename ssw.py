import requests
import pandas as pd
import json
import mysql.connector
import datetime

url = "https://ssw.inf.br/api/trackingdanfe"
headers = {"Content-Type": "application/json"}
banco = mysql.connector.connect(
                user = 'root',
                passwd = '',
                host = 'localhost',
                database = 'python_bot'
                )

conexao = banco.cursor(buffered = True)
csv = pd.read_csv('C:\\temp\\emb0481.csv',header=None, sep=";")

for index, row in csv.iterrows():
    try:
        if row[2] == 'N':
            
            data = {"chave_nfe": f"{row[1]}"}
            response = requests.post(url, headers=headers, data=json.dumps(data))
            response_dados = response.json()
            documento = response_dados["documento"]
            header = documento["header"]
            tracking = documento["tracking"]
            nro_nf = header["nro_nf"]
            

            comando_data = datetime.datetime.now()
            data = comando_data.strftime("%Y/%m/%d, %H:%M:%S")

            for item in tracking:
                ocorrencia = item["ocorrencia"].split(' (')[0]
                select = f"SELECT situacao FROM rastreiodados WHERE nfe = '{nro_nf}' AND situacao = '{ocorrencia}' AND nome = 'QFAZ'"
                conexao.execute(select)

                print(ocorrencia)
 
                if conexao.rowcount == 0:                                     
                    insert = f"INSERT INTO rastreiodados (nome, cod, serie, nfe, situacao, dataRegistro) VALUES ('Qfaz', '2', '4', '{nro_nf}', '{ocorrencia}', '{data}')"
                    conexao.execute(insert)
                    banco.commit()
    except:
        continue