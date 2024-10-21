import time
import pandas as pd
import numpy as np
import datetime as dt
from datetime import date
import mysql.connector
import os
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import delete
from datetime import timedelta
from pathlib import Path
from sqlalchemy import delete,text
from dotenv import load_dotenv
import pytz
from sqlalchemy import insert
import psycopg2
from io import StringIO
import pandas as pd
from urllib.parse import urlunparse
import io
from prefect import flow, task
import calendar
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.edge.options import Options
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.common.keys import Keys
import win32com.client as win32
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import sys
import numpy as np
from datetime import date
from bs4 import BeautifulSoup
import html5lib
import msedgedriver
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from tqdm import tqdm
import json
import requests
from prefect.client.schemas.schedules import IntervalSchedule
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.blocks.webhook import Webhook

env_path = R".\.env"
load_dotenv(env_path)

horario_atual = dt.datetime.now()

@task
def obter_matricula():
    return os.getlogin()

@task
def diasMesAno():
    ano = dt.datetime.now().date().year
    mes = dt.datetime.now().date().month
    dia = dt.datetime.now().date().day
    
    if dia ==1:
        if mes==1:
            mes=12
            ano-=1
        else:
            mes-=1

    data_acordo_inicio=pd.to_datetime(f'{ano}-{mes:02d}-01').date().strftime("%d/%m/%Y")
    data_acordo_fim=dt.datetime.now().date().strftime("%d/%m/%Y")

    data_acordo_fim_receb=(dt.datetime.now().date()+timedelta(days=0)).strftime("%d/%m/%Y")
    hoje = date.today()
    diaSeman=hoje.weekday()
    if diaSeman==4:
        dataAmanha=hoje+timedelta(days=3)
    else:
        dataAmanha=hoje+timedelta(days=1)
    
    dataAmanha=dataAmanha.strftime("%d/%m/%Y")
    return data_acordo_inicio,data_acordo_fim,data_acordo_fim_receb,dia,mes,ano,dataAmanha

@task
def fazer_download(navegador):  
    matricula=obter_matricula()

    while len(navegador.find_elements(By.XPATH,'//*[@id="DataTables_Table_0"]')) < 1:
        time.sleep(1)

    table = navegador.find_element(By.XPATH,'//*[@id="DataTables_Table_0"]')
    table_html = table.get_attribute('outerHTML')
    listaArquivos = pd.read_html(StringIO(table_html),header=0)[0]
    listaArquivos=listaArquivos.query(f"@listaArquivos['Matrícula']=={matricula}").reset_index()
    idLinha=listaArquivos.loc[0,'ID']
    listaArquivos=listaArquivos.query("ID==@idLinha")

    while listaArquivos['Data Finalização'].isnull()[0]:
        table = navegador.find_element(By.XPATH,'//*[@id="DataTables_Table_0"]')
        table_html = table.get_attribute('outerHTML')
        listaArquivos = pd.read_html(StringIO(table_html),header=0)[0]
        listaArquivos=listaArquivos.query(f"@listaArquivos['Matrícula']=={matricula}")
        # idLinha=listaArquivos.loc[0,'ID']
        listaArquivos=listaArquivos.query("ID==@idLinha")
        navegador.refresh()
    
    tbody=navegador.find_elements(By.XPATH,'//*[@id="DataTables_Table_0"]/tbody')

    tr=navegador.find_elements(By.TAG_NAME,'tr')

    for i in tr:
        if str(idLinha) in i.text:
            td=i.find_elements(By.TAG_NAME,'td')
            for b in td:
                try:
                    c=b.find_element(By.TAG_NAME,"a")
                    print(c.get_attribute('href'))
                    listaArquivos['Arquivo']=c.get_attribute('href')
                    navegador.get(c.get_attribute('href'))
                except:
                    pass

@task
def ultimo_dia_do_mes(ano, mes):
    # Encontrar o último dia do mês
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    
    # Retornar a data no formato 'DD/MM/YYYY'
    return f'{ultimo_dia:02d}/{mes:02d}/{ano:04d}'

@task
def inicializar_navegador():

    msedgedriver.install()
    caminho_chromedriver = "msedgedriver.exe"
    servico = Service(caminho_chromedriver)
    options = Options()
    # options.add_argument('--headless=new')
    navegador = webdriver.Edge(service=servico)
    alerta=Alert(navegador)
    # navegador.minimize_window()
    navegador.get(f'https://sicob.uninter.com/Login')
    navegador.get(f'https://sicob.uninter.com/Login')
    navegador.find_element(By.XPATH,'/html/body/div/div/form/div/a/button').click()
    return navegador,alerta

@task
def gerar_dfNegociado(navegador):

    data_acordo_inicio,data_acordo_fim,data_acordo_fim_receb,dia,mes,ano,dataAmanha=diasMesAno()
    ultimo_dia=ultimo_dia_do_mes(ano,mes)
    hora1=dt.datetime.now().strftime("%H:%M:%S")

    navegador.get(f'https://sicob.uninter.com/Titulos/Negociados')

    navegador.find_element(By.ID,'data_vencimento_inicio').send_keys(dataAmanha)
    navegador.find_element(By.ID,'data_vencimento_fim').send_keys(dataAmanha)
    navegador.find_element(By.ID,'situacao_titulo').send_keys('Aberto')

    time.sleep(2)
    navegador.find_element(By.CLASS_NAME,'btn-success').send_keys(Keys.ENTER)
    # navegador.find_element(By.XPATH,'//*[@id="menu-lateral"]/div[2]/div/div[3]/div/div/div[2]/div/div[2]/form/div[3]/div/button').click()
    hora=time.strftime("%H:%M:%S")

    # for _ in range(5):
    #     webdriver.ActionChains(navegador).send_keys(Keys.TAB).perform()
    # webdriver.ActionChains(navegador).send_keys(Keys.ENTER).perform()

    while len(navegador.find_elements(By.CLASS_NAME,'fa-list')) < 1:
        time.sleep(1)

    fazer_download(navegador)

    time.sleep(2)

    usuario = os.environ['USERPROFILE']
    # Selecionar a pasta de downloads
    pasta_downloads = Path(rf"{usuario}\Downloads")

    # Verificar se a pasta de downloads existe e é um diretório
    if pasta_downloads.exists() and pasta_downloads.is_dir():

        arquivos_na_pasta = []
        for nome_arquivo in os.listdir(pasta_downloads):
            caminho_completo = os.path.join(pasta_downloads, nome_arquivo)
            if os.path.isfile(caminho_completo):
                arquivos_na_pasta.append((nome_arquivo, os.path.getctime(caminho_completo)))

        arquivos_na_pasta.sort(key=lambda arquivo: arquivo[1])  # Ordena pela data de criação
        arquivos_na_pasta=[arquivo[0] for arquivo in arquivos_na_pasta]

        # Obter uma lista de todos os arquivos na pasta de downloads
        # arquivos_na_pasta = sorted(pasta_downloads.iterdir(), key=lambda x: x.stat().st_birthtime)

        # Se houver arquivos na pasta
        if arquivos_na_pasta:
            # Pegar o último arquivo criado
            ultimo_arquivo = arquivos_na_pasta[-1]
            ultimo_arquivo=os.path.join(pasta_downloads, ultimo_arquivo)

        # Verificar se o arquivo é realmente um arquivo CSV
        if os.path.isfile(ultimo_arquivo):
            # Ler o arquivo CSV usando pandas
            dfNegociado = pd.read_csv(ultimo_arquivo,sep=";")
            dfNegociado['Primeiro_nome']=dfNegociado['Nome'].apply(lambda x: x.split()[0])
        
            dfNegociado['Data Acordo']=pd.to_datetime(dfNegociado['Data Acordo'],dayfirst=True)

            dfNegociado['Valor Negociado']=dfNegociado['Valor Negociado'].str.replace(",",".").astype(float)

            dfNegociado['Data Vencimento']=pd.to_datetime(dfNegociado['Data Vencimento'],dayfirst=True)

            dfNegociado=dfNegociado[['Primeiro_nome','Numero Acordo','Data Acordo','Data Vencimento','Criado Por','Metodo Pagamento','Valor Negociado', 'Titulo','Parcela','E-mail', 'Telefone Residencial', 'Telefone Comercial',
       'Telefone Celular','Telefone Contato','Codigo Aluno','Codigo Local', 'Nome Local'
            ]]

            dfNegociado.columns=[i.replace(" ","_") for i in dfNegociado.columns]

            dfNegociado=dfNegociado.query("Criado_Por!='Acordo Online'")
            dfNegociado.columns
            dfNegociado=dfNegociado.groupby('Codigo_Aluno',as_index=False).agg({'Primeiro_nome':'first', 'Numero_Acordo':'first', 'Data_Acordo':'first', 'Data_Vencimento':'first',
       'Criado_Por':'first', 'Metodo_Pagamento':'first', 'Valor_Negociado':'first', 'Titulo':'first',
       'Parcela':'first', 'E-mail':'first', 'Telefone_Residencial':'first','Telefone_Comercial':'first','Telefone_Celular':'first', 'Telefone_Contato':'first','Codigo_Local':'first',
       'Nome_Local':'first'})
        else:
            print(f"{ultimo_arquivo} não é um arquivo válido.")
    else:
        print("Nenhum arquivo CSV encontrado na pasta de downloads.")
    return dfNegociado

@flow(log_prints=True)
def gerarBaseWhats():
    matricula=obter_matricula()
    data_acordo_inicio,data_acordo_fim,data_acordo_fim_receb,dia,mes,ano,dataAmanha=diasMesAno()  

    try:
        navegador,alerta=inicializar_navegador()

        start_time = dt.datetime.now().strftime("%H:%M:%S")

        dfNegociado= gerar_dfNegociado(navegador)

        def unificaTel(row):
            if pd.notnull(row['Telefone_Celular']):
                return row['Telefone_Celular']
            elif pd.notnull(row['Telefone_Residencial']):
                return row['Telefone_Residencial']
            elif pd.notnull(row['Telefone_Comercial']):
                return row['Telefone_Comercial']
            else:
                return None
        def process_item(item):
            item_str = str(item)
            if len(item_str) > 2 and item_str[2].isdigit() and int(item_str[2]) <= 5:
                return ''
            else:
                if len(item_str) > 2 and item_str[2].isdigit() and int(item_str[3]) <= 5:
                        return ''
            return item

        def ajustaCel(item):
            item_str = str(item)
            if len(item_str) > 2 and item_str[2].isdigit() and int(item_str[0:2])==41 and len(item_str[2:]) == 9:
                return item_str[2:]
            elif len(item_str) > 2 and item_str[2].isdigit():
                # Remover caracteres não numéricos do início
                numeric_part = ''.join(filter(str.isdigit, item_str[0:2])) 
                if numeric_part and int(numeric_part) == 41 and len(item_str[2:]) < 9:
                    return (f"9{item_str[2:]}")
                else:
                    if len(item_str) == 12:
                        return item_str
                    elif len(item_str) == 11 and item_str[0]!=0:
                        return (f"0{item_str}")
                    elif len(item_str) >= 14 and item_str[0:2]==000:
                        return item_str[2:]
                    else:
                        ddd=item_str[0:2]
                        num=item_str[2:]
                        if len(num) == 9:  # Se o número tem 8 dígitos, adicione "9" antes do número
                            num = f"{num}"
                        return (f"0{ddd}{num}")  # Adicione "0" antes do DDD
                
        dfNegociado['Telefones'] = dfNegociado.apply(lambda row: unificaTel(row), axis=1)

        dfNegociado=dfNegociado.query("Telefones.notnull()")
        dfNegociado['Telefones']=dfNegociado['Telefones'].str.replace("(","").str.replace(")","")
        dfNegociado['Telefones2'] = dfNegociado['Telefones'].apply(process_item)
        dfNegociado['Telefones2'] = dfNegociado['Telefones2'].apply(ajustaCel)
        # dataframes = {
        #     "negociado_parcial": dfNegociado,
        # }

        dfNegociado=dfNegociado.query("Telefones2.notnull()")
        # dfNegociado.to_excel("titulos_a_vencer.xlsx",index=False)
        # for nomeBase, base in dataframes.items():
        #     enviaBD(base, nomeBase, dia)
        data_dict = []
        payload_json=[]
        # Iterar sobre as linhas do DataFrame
        for row in dfNegociado.values:
            data_dict.append(row)
            chave = row[0]
            # Converter Timestamp para string ISO 8601
            row['Data_Acordo'] = row['Data_Acordo'].isoformat()
            valores = row.drop('Primeiro_nome').to_dict()
            
            # data_dict.append(f"Aluno:" + "{" + f"{chave}:{valores}" + "}")
            payload_json.append(json.dumps("Aluno:" + "{" + f"{chave}:{valores}" + "}"))
            # data_dict[chave] = valores
        data_dict=data_dict.to_json(orient='records',date_format='iso')
        # Serializar o dicionário
        payload_json = json.dumps(data_dict)
        webhook_url = 'https://n8n-n8n.8t1f5e.easypanel.host/webhook-test/2006f633-aa9c-4759-b0f7-2dbf49ddb26e'

        # Enviar a requisição POST
        response = requests.post(
            webhook_url,
            data=data_dict,
            headers={'Content-Type': 'application/json'},verify=False
        )

        # Verificar a resposta
        if response.status_code == 200:
            print('Dataframe enviado com sucesso!')
        else:
            print(f'Erro ao enviar o dataframe: {response.status_code} - {response.text}')

    except Exception as e:
        print(f"Erro na função: {e}")

# if __name__ == "__main__":
#     # creates a deployment and stays running to monitor for work instructions 
#     # generated on the server
    
#     gerarBaseWhats.serve(name="Atualiza_Metas",
#         tags=["atualiza_quebras"],
#         # schedules=[
#         #     IntervalSchedule(
#         #     interval=timedelta(minutes=60))]
#     )

gerarBaseWhats()