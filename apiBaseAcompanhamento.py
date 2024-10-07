import time
import pandas as pd
import numpy as np
import datetime as dt
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
import html5lib
import msedgedriver
from webdriver_manager.microsoft import EdgeChromiumDriverManager
import logging
import dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from logging.handlers import RotatingFileHandler
msedgedriver.install()
caminho_chromedriver = "msedgedriver.exe"
servico = Service(caminho_chromedriver)
options = webdriver.EdgeOptions()
# options.add_argument('--headless=new')
options.add_argument('--ignore-certificate-errors')
navegador = webdriver.Edge(service=servico,options=options)
alerta=Alert(navegador)
navegador.minimize_window()

loadenv = dotenv.find_dotenv()
dotenv.load_dotenv(loadenv)

horario_atual = dt.datetime.now()

matricula=os.getlogin()

# locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

# Configuração do logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Cria um handler para arquivo (opcional)
file_handler = RotatingFileHandler('meu_app.log', maxBytes=10000, backupCount=1)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)

# Configuração do banco de dados PostgreSQL
DB_HOST = "77.37.40.212"
DB_NAME = "db_cobranca"
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
PORT=5433

def heartbeat():
    with open("heartbeat.txt", "w") as f:
        f.write(f"Heartbeat: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
def conectar_db():
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=PORT)
        return conn
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Erro ao conectar ao banco de dados: {error}")
        return None

def registrar_log(mensagem, nivel="INFO"):
    conn = conectar_db()
    if conn:
        try:
            cursor = conn.cursor()
            sql = "INSERT INTO logs_sheduler (data_hora, nivel, mensagem) VALUES (%s, %s, %s)"
            cursor.execute(sql, (dt.datetime.now(), nivel, mensagem))
            conn.commit()
            logging.info("Log registrado no banco de dados com sucesso.")
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Erro ao registrar log no banco de dados: {error}")
        finally:
            if conn:
                cursor.close()
                conn.close()

def limparDownloads():
    pasta_download = os.path.expanduser("~/Downloads")  # Caminho para a pasta Downloads

    # Verificar se o caminho existe e é uma pasta
    if os.path.exists(pasta_download) and os.path.isdir(pasta_download):
        # Listar todos os arquivos na pasta
        arquivos_na_pasta = os.listdir(pasta_download)

        # Excluir cada arquivo
        for arquivo in arquivos_na_pasta:
            caminho_completo = os.path.join(pasta_download, arquivo)
            try:
                if os.path.isfile(caminho_completo):
                    os.remove(caminho_completo)
                    print(f"Arquivo {arquivo} removido com sucesso.")
            except Exception as e:
                print(f"Erro ao remover {arquivo}: {e}")
    else:
        print("O caminho fornecido não é uma pasta válida.")

# @task(retries=2)
def fazer_download(navegador,matricula):  
    
    while len(navegador.find_elements(By.XPATH,'//*[@id="DataTables_Table_0"]')) < 1:
        time.sleep(1)

    table = navegador.find_element(By.XPATH,'//*[@id="DataTables_Table_0"]')
    table_html = table.get_attribute('outerHTML')
    listaArquivos = pd.read_html(StringIO(table_html),header=0)[0]
    listaArquivos=listaArquivos.query(f"@listaArquivos['Matrícula']=={matricula}")
    listaArquivos=listaArquivos.query("@listaArquivos['Data Finalização'].isnull()")
    cont=len(listaArquivos)

    while cont >= 1:
        table = navegador.find_element(By.XPATH,'//*[@id="DataTables_Table_0"]')
        table_html = table.get_attribute('outerHTML')
        listaArquivos = pd.read_html(StringIO(table_html),header=0)[0]
        listaArquivos=listaArquivos.query(f"@listaArquivos['Matrícula']=={matricula}")
        listaArquivos=listaArquivos.query("@listaArquivos['Data Finalização'].isnull()")
        cont=len(listaArquivos)
        navegador.refresh()

    table = navegador.find_element(By.XPATH,'//*[@id="DataTables_Table_0"]')
    table_html = table.get_attribute('outerHTML')

    listaArquivos = pd.read_html(StringIO(table_html),header=0)[0]
    
    tbody=navegador.find_elements(By.XPATH,'//*[@id="DataTables_Table_0"]/tbody')
    # tr=tbody.find_elements(By.TAG_NAME,'tr')

    listaLinks=[]
    for i in tbody:
        td=i.find_elements(By.TAG_NAME,'td')
        a=i.find_elements(By.TAG_NAME,'a')
        for x,y in zip(a, td):
            href=x.get_attribute('href')
            if href is not None:
                if 'Download' in href:
                    listaLinks.append(href)
                else:
                    pass
    try:
        listaArquivos['Arquivo'] = listaLinks
    except:
        navegador.refresh()
        table = navegador.find_element(By.XPATH,'//*[@id="DataTables_Table_0"]')
        table_html = table.get_attribute('outerHTML')

        listaArquivos = pd.read_html(StringIO(table_html),header=0)[0]
        
        tbody=navegador.find_elements(By.XPATH,'//*[@id="DataTables_Table_0"]/tbody')
        # tr=tbody.find_elements(By.TAG_NAME,'tr')

        listaLinks=[]
        for i in tbody:
            td=i.find_elements(By.TAG_NAME,'td')
            a=i.find_elements(By.TAG_NAME,'a')
            for x,y in zip(a, td):
                href=x.get_attribute('href')
                if href is not None:
                    if 'Download' in href:
                        listaLinks.append(href)
                    else:
                        pass
        listaArquivos['Arquivo'] = listaLinks

    listaArquivosFiltro=listaArquivos[listaArquivos['Matrícula']==int(matricula)]
    prim=listaArquivosFiltro[0:1]
    navegador.get(prim['Arquivo'][0])
        # break
        # return True
        # else:
        #     navegador.refresh()

# @task(retries=2)
def listaArquivos(navegador):
        
    # Aguarde até que a tabela seja carregada (você pode ajustar conforme necessário)
    table_xpath = '//*[@id="DataTables_Table_0"]'
    navegador.find_element(By.XPATH, table_xpath)

    # Obtenha o HTML da tabela
    table = navegador.find_element(By.XPATH, table_xpath)
    table_html = table.get_attribute('outerHTML')

    # Use o pandas para ler a tabela HTML
    dfRel = pd.read_html(table_html, header=0)[0]

    # Adicione uma coluna 'links' ao DataFrame
    dfRel['links'] = None

    # Obtenha os elementos da tabela (linhas)
    rows = table.find_elements(By.TAG_NAME, "tr")

    for index, row in enumerate(rows):
        # Obtenha todas as células na linha
        cells = row.find_elements(By.TAG_NAME, "td")

        # Verifique se há células suficientes na linha
        if len(cells) > 0:
            # A última célula contém os links
            cell_links = cells[-1]

            # Obtenha todos os elementos de âncora (links) dentro da célula
            link_elements = cell_links.find_elements(By.TAG_NAME, "a")

            # Itere sobre os links
            for link in link_elements:
                # Obtenha o atributo 'href' (link)
                href = link.get_attribute('href')

                # Adicione o link à linha correspondente no DataFrame
                dfRel.at[index - 1, 'links'] = href if href is not None else dfRel.at[index - 1, 'links']

# @task(retries=2)
def ultimo_dia_do_mes(ano, mes):
    # Encontrar o último dia do mês
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    
    # Retornar a data no formato 'DD/MM/YYYY'
    return f'{ultimo_dia:02d}/{mes:02d}/{ano:04d}'

# @task(retries=2)
def inicializar_navegador(caminho_chromedriver):

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

# @task(retries=2)
def delRows (nomebs,dia):
    mes=dt.datetime.now().month
    conn = f"postgresql+psycopg2://postgres:Gaby030686@77.37.40.212:5433/db_cobranca"
    engine = create_engine(conn)

    with engine.begin() as connection:  # Use begin() para criar uma transação
        if  dia==1:
            if nomebs=="Liquidado":
                connection.execute(text(f'delete from "{nomebs}" where extract(month from "Data_Liquidacao")={mes-1}'))
                connection.execute(text(f'delete from "{nomebs}" where extract(month from "Data_Liquidacao")={mes}'))
            elif nomebs=="A_Receber":
                connection.execute(text(f'delete from "{nomebs}" where extract(month from "Data_Vencimento")={mes-1}'))
                connection.execute(text(f'delete from "{nomebs}" where extract(month from "Data_Vencimento")={mes}'))
            else:
                connection.execute(text(f'delete from "{nomebs}" where extract(month from "criada_em")={mes-1}'))
                connection.execute(text(f'delete from "{nomebs}" where extract(month from "criada_em")={mes}'))
        else:
            if nomebs=="Liquidado":
                connection.execute(text(f'delete from "{nomebs}" where extract(month from "Data_Liquidacao")={mes}'))
            elif nomebs=="A_Receber":
                connection.execute(text(f'delete from "{nomebs}" where extract(month from "Data_Vencimento")={mes}'))
            else:
                connection.execute(text(f'delete from "{nomebs}" where extract(month from "criada_em")={mes}'))

def enviaBD(base, nomeBase,dia):
    # Componentes da conexão
    scheme = 'postgresql' 
    user = DB_USER
    password = DB_PASS
    host = '77.37.40.212'
    port = '5433'
    database = 'db_cobranca'

    # Construir a URL de conexão
    url = urlunparse((scheme, f'{user}:{password}@{host}:{port}', database, '', '', ''))

    # Salvar o DataFrame em um buffer CSV
    buffer = io.StringIO()
    base.to_csv(buffer, sep='\t', header=False, index=False)
    buffer.seek(0) 

    if nomeBase == "Liquidado":
        if len(base)!=0:
            delRows(nomeBase,dia)
        # Conectar diretamente ao PostgreSQL usando psycopg2 e realizar a inserção com COPY
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.copy_expert(f'COPY "Liquidado" FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\')', buffer)
                
                dataAtu = horario_atual.strftime("%d/%m/%Y %H:%M:%S")
                connect = f"postgresql+psycopg2://postgres:Gaby030686@77.37.40.212:5433/db_cobranca"
                engine = create_engine(connect)

                with engine.begin() as connection:
                    connection.execute(text(f"update atualizacao set atualizado_em ='{dataAtu}' where user_id=1"))
            conn.commit()

        print(f"Dados inseridos com sucesso em {nomeBase}")

    elif nomeBase == "A_Receber":
        if len(base)!=0:
            delRows(nomeBase,dia)
       # Salvar o DataFrame em um buffer CSV
        # buffer = io.StringIO()
        # base.to_csv(buffer, sep='\t', header=False, index=False)
        # buffer.seek(0)

        # Conectar diretamente ao PostgreSQL usando psycopg2 e realizar a inserção com COPY
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.copy_expert(f'COPY "A_Receber" FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\')', buffer)

            conn.commit()

        print(f"Dados inseridos com sucesso em {nomeBase}")
    else :
        if len(base)!=0:
            delRows(nomeBase,dia)
    # Salvar o DataFrame em um buffer CSV
        # buffer = io.StringIO()
        # base.to_csv(buffer, sep='\t', header=False, index=False)
        # buffer.seek(0)

        # Conectar diretamente ao PostgreSQL usando psycopg2 e realizar a inserção com COPY
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.copy_expert(f'COPY negociado_parcial FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\')', buffer)

            conn.commit()

        print(f"Dados inseridos com sucesso em {nomeBase}")

def gerarBases(matri: str = matricula,camin: str = caminho_chromedriver):
    try:
        # servico = Service(caminho_chromedriver)
        # options = Options()
        # options = webdriver.EdgeOptions()
        # options.add_argument('--headless=new')
        # options.add_argument('--ignore-certificate-errors')
        # options.add_argument('--headless=new')
        # navegador = webdriver.Edge(service=servico)
        # alerta=Alert(navegador)
        # navegador.minimize_window()
        navegador.get(f'https://sicob.uninter.com/Login')
        navegador.get(f'https://sicob.uninter.com/Login')
        navegador.find_element(By.XPATH,'/html/body/div/div/form/div/a/button').click()

        start_time = dt.datetime.now().strftime("%H:%M:%S")

        ano = dt.datetime.now().date().year
        mes = dt.datetime.now().date().month
        dia = dt.datetime.now().date().day
        
        if dia ==1:
            if mes==1:
                mes=12
                ano-=1
            else:
                mes-=1

        ultimo_dia = ultimo_dia_do_mes(ano, mes)

        data_acordo_inicio=pd.to_datetime(f'{ano}-{mes:02d}-01').date().strftime("%d/%m/%Y")
        data_acordo_fim=dt.datetime.now().date().strftime("%d/%m/%Y")

        data_acordo_fim_receb=(dt.datetime.now().date()+timedelta(days=0)).strftime("%d/%m/%Y")

        datahj=dt.datetime.now()
        datontem=datahj-timedelta(days=10)
        datontem=datontem.date().strftime("%d/%m/%Y")

        ##GERAR RELATÓRIO LIQUIDADOS
        hora1=dt.datetime.now().strftime("%H:%M:%S")
    
        registrar_log(f"Iniciando a importação do Relatório de Liquidados - {hora1}")

        navegador.get('https://sicob.uninter.com/Titulos/Negociados')

        navegador.find_element(By.ID,'data_liquidacao_inicio').send_keys(data_acordo_inicio)
        navegador.find_element(By.ID,'data_liquidacao_fim').send_keys(data_acordo_fim)
        navegador.find_element(By.ID,'situacao_titulo').send_keys('Liquidado')

        for _ in range(5):
            webdriver.ActionChains(navegador).send_keys(Keys.TAB).perform()
        webdriver.ActionChains(navegador).send_keys(Keys.ENTER).perform()

        while len(navegador.find_elements(By.CLASS_NAME,'fa-list')) < 1:
            time.sleep(1)

        while len(navegador.find_elements(By.CLASS_NAME,'fa-list')) < 1:
            time.sleep(1)

        time.sleep(2)

        fazer_download(navegador, matricula)

        time.sleep(7)
        # Caminho para a pasta de downloads

        # usuario = os.environ['USERPROFILE']
        usuario = os.getenv('USERPROFILE')
        # Selecionar a pasta de downloads
        
        if usuario is None:
            usuario = "default_username"  # Provide a default value or handle it as necessary

        pasta_downloads = Path(rf"{usuario}\Downloads")

        # Verificar se a pasta de downloads existe e é um diretório
        if pasta_downloads.exists() and pasta_downloads.is_dir():
            # Obter uma lista de todos os arquivos na pasta de downloads
            arquivos_na_pasta = sorted(pasta_downloads.iterdir(), key=lambda x: x.stat().st_ctime)

            # Se houver arquivos na pasta
            if arquivos_na_pasta:
                # Pegar o último arquivo criado
                ultimo_arquivo = arquivos_na_pasta[-1]

            # Verificar se o arquivo é realmente um arquivo CSV
            if os.path.isfile(ultimo_arquivo):
                # Ler o arquivo CSV usando pandas
                dfLiquidado = pd.read_csv(ultimo_arquivo,sep=";")
                dfLiquidado['Primeiro_nome']=dfLiquidado['Nome'].apply(lambda x: x.split()[0])

                dfLiquidado=dfLiquidado[['Cliente','Primeiro_nome','Numero Acordo','Data Acordo','Criado Por','Titulo','Data Vencimento','Parcela','Nr Parcela','Data Liquidacao','Situacao','Valor Original','Descontos','Juros','Multa','Valor Atualizado','Valor Negociado','Valor Liquidado','Finalidade','Codigo Aluno','Codigo Local','Nome Local','Ultimo Numero Acordo'
                ]]

                dfLiquidado['Data Liquidacao']=pd.to_datetime(dfLiquidado['Data Liquidacao'],dayfirst=True)
                dfLiquidado['Valor Liquidado']=dfLiquidado['Valor Liquidado'].str.replace(",",".").astype(float)

                dfLiquidado.columns=[i.replace(" ","_") for i in dfLiquidado.columns]

            else:
                print(f"{ultimo_arquivo} não é um arquivo válido.")
        else:
            print("Nenhum arquivo CSV encontrado na pasta de downloads.")

        enviaBD(dfLiquidado,"Liquidado",dia)

        hora1=dt.datetime.now().strftime("%H:%M:%S")
        
        print(f"Final da importação do Relatório de Liquidados - {hora1}")

        ##GERAR RELATÓRIO A_LIQUIDAR
        print(f"Iniciando a importação do Relatório do a Liquidar - {hora1}")

        navegador.get(f'https://sicob.uninter.com/Login')  
        navegador.get(f'https://sicob.uninter.com/Login')  
        navegador.find_element(By.XPATH,'/html/body/div/div/form/div/a/button').click()
        navegador.get(f'https://sicob.uninter.com/Titulos/Negociados')  

        # ultimo_dia = ultimo_dia_do_mes(ano, mes)

        data_acordo_inicio=pd.to_datetime(f'{ano}-{mes:02d}-01').date().strftime("%d/%m/%Y")
        navegador.find_element(By.ID,'data_vencimento_inicio').send_keys(data_acordo_fim_receb)
        navegador.find_element(By.ID,'data_vencimento_fim').send_keys(ultimo_dia)
        navegador.find_element(By.ID,'situacao_titulo').send_keys('Aberto')

        for _ in range(5):
            webdriver.ActionChains(navegador).send_keys(Keys.TAB).perform()
        webdriver.ActionChains(navegador).send_keys(Keys.ENTER).perform()

        while len(navegador.find_elements(By.CLASS_NAME,'fa-list')) < 1:
            time.sleep(1)

        time.sleep(3)

        fazer_download(navegador, matricula)

        time.sleep(2)

        usuario = os.environ['USERPROFILE']
        # Selecionar a pasta de downloads
        pasta_downloads = Path(rf"{usuario}\Downloads")

        # Verificar se a pasta de downloads existe e é um diretório
        if pasta_downloads.exists() and pasta_downloads.is_dir():
            # Obter uma lista de todos os arquivos na pasta de downloads
            arquivos_na_pasta = sorted(pasta_downloads.iterdir(), key=lambda x: x.stat().st_ctime)

            # Se houver arquivos na pasta
            if arquivos_na_pasta:
                # Pegar o último arquivo criado
                ultimo_arquivo = arquivos_na_pasta[-1]

            # Verificar se o arquivo é realmente um arquivo CSV
            if os.path.isfile(ultimo_arquivo):
                # Ler o arquivo CSV usando pandas
                dfAreceber = pd.read_csv(ultimo_arquivo,sep=";")
                dfAreceber['Primeiro_nome']=dfAreceber['Nome'].apply(lambda x: x.split()[0])

                dfAreceber=dfAreceber[['Cliente','Primeiro_nome','Numero Acordo','Data Acordo','Criado Por','Titulo','Data Vencimento','Parcela','Nr Parcela','Data Liquidacao','Situacao','Valor Original','Valor Atualizado','Valor Negociado','Valor Liquidado','Finalidade','Codigo Aluno','Codigo Local','Nome Local','Ultimo Numero Acordo'
                ]]

                dfAreceber['Data Vencimento']=pd.to_datetime(dfAreceber['Data Vencimento'],dayfirst=True)
                dfAreceber['Valor Original']=dfAreceber['Valor Original'].str.replace(",",".").astype(float)

                dfAreceber.columns=[i.replace(" ","_") for i in dfAreceber.columns]
        
            else:
                print(f"{ultimo_arquivo} não é um arquivo válido.")
        else:
            print("Nenhum arquivo CSV encontrado na pasta de downloads.")

        enviaBD(dfAreceber,"A_Receber",dia)
        hora1=dt.datetime.now().strftime("%H:%M:%S")
        registrar_log(f"Final da importação do Relatório de Negociados - {hora1}")

        ##GERAR RELATÓRIO NEGOCIADO
        hora1=dt.datetime.now().strftime("%H:%M:%S")
        registrar_log(f"Iniciando a importação do Relatório do NEGOCIADO - {hora1}")

        navegador.get(f'https://sicob.uninter.com/Login')  
        navegador.get(f'https://sicob.uninter.com/Login')  
        navegador.find_element(By.XPATH,'/html/body/div/div/form/div/a/button').click()
        navegador.get(f'https://sicob.uninter.com/AcordosFinanceiros/Exportar')  

        data_acordo_inicio=pd.to_datetime(f'{ano}-{mes:02d}-01').date().strftime("%d/%m/%Y")
        navegador.find_element(By.ID,'data_acordo_inicio').send_keys(data_acordo_fim_receb)
        navegador.find_element(By.ID,'data_acordo_fim').send_keys(data_acordo_fim_receb)

        navegador.find_element(By.CLASS_NAME,'btn-success').click()

        # for _ in range(5):
        #     webdriver.ActionChains(navegador).send_keys(Keys.TAB).perform()
        # webdriver.ActionChains(navegador).send_keys(Keys.ENTER).perform()

        while len(navegador.find_elements(By.CLASS_NAME,'fa-list')) < 1:
            time.sleep(1)

        time.sleep(3)

        fazer_download(navegador, matricula)

        time.sleep(2)

        usuario = os.environ['USERPROFILE']
        # Selecionar a pasta de downloads
        pasta_downloads = Path(rf"{usuario}\Downloads")

        # Verificar se a pasta de downloads existe e é um diretório
        if pasta_downloads.exists() and pasta_downloads.is_dir():
            # Obter uma lista de todos os arquivos na pasta de downloads
            arquivos_na_pasta = sorted(pasta_downloads.iterdir(), key=lambda x: x.stat().st_ctime)

            # Se houver arquivos na pasta
            if arquivos_na_pasta:
                # Pegar o último arquivo criado
                ultimo_arquivo = arquivos_na_pasta[-1]

            # Verificar se o arquivo é realmente um arquivo CSV
            if os.path.isfile(ultimo_arquivo):
                # Ler o arquivo CSV usando pandas
                dfNegociado = pd.read_csv(ultimo_arquivo,sep=";")
                dfNegociado['Primeiro_nome']=dfNegociado['Nome'].apply(lambda x: x.split()[0])

                dfNegociado['Situação'].value_counts()

                dfNegociado=dfNegociado.query("Situação!='Cancelada' and Situação!='Rejeitada'")

                dfNegociado['Criada em']=pd.to_datetime(dfNegociado['Criada em'],dayfirst=True)
                dfNegociado['Valor da negociação']=dfNegociado['Valor da negociação'].str.replace(",",".").astype(float)
                dfNegociado['Entrada']=dfNegociado['Entrada'].str.replace(",",".").astype(float)

                dfNegociado['Valor_Acordo']= dfNegociado.apply(
                    lambda x: x['Valor da negociação'] 
                    if x['Condição de pagamento'].lower() == "a vista" 
                    else x['Entrada'], 
                    axis=1 
                )

                dfNegociado=dfNegociado[['Primeiro_nome','Numero da negociação','Criada em','Usuário','Valor_Acordo','Condição de pagamento', 'Primeiro vencimento', 'Entrada','Quantidade de parcelas', 'Valor parcelas','Código aluno','cd_local', 'nome_local'
                ]]

                dfNegociado.columns=[i.replace(" ","_") for i in dfNegociado.columns]

                dfNegociado['Primeiro_vencimento']=pd.to_datetime(dfNegociado['Primeiro_vencimento'],dayfirst=True)
                dfNegociado['Valor_parcelas']=dfNegociado['Valor_parcelas'].str.replace(",",".").astype(float)

            else:
                print(f"{ultimo_arquivo} não é um arquivo válido.")
        else:
            print("Nenhum arquivo CSV encontrado na pasta de downloads.")

        enviaBD(dfNegociado,"negociado_parcial",dia)

        data=dt.datetime.now().date()
        hora=dt.datetime.now().strftime("%H:%M:%S")

        hora1=dt.datetime.now().strftime("%H:%M:%S")

        registrar_log(f"Processo Finalizado - {hora1}")
        heartbeat()
        navegador.close()
    except Exception as e:
        logging.error(f"Erro na função: {e}")
        registrar_log(f"Erro na função: {e}", nivel="ERROR")
        raise

if __name__ == "__main__":
    scheduler = BlockingScheduler(executors={'default': ProcessPoolExecutor(5)})
    scheduler.add_job(gerarBases, 'interval', minutes=1)
                 
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

    # matricula = input("Digite sua matrícula: ")
    # gerarBases.serve(name="Atualiza_Acompanhamento",
    # parameters={"camin": caminho_chromedriver},
    # tags=["acompanhamento"],
    # pause_on_shutdown=False,
    # interval=3600)
    
    # Adicionar a tarefa periódica
    # # scheduler.add_job(minha_tarefa_periodica, 'interval', minutes=30, args=[matricula])
    
    # try:
    #     scheduler.start()
    # except Exception as e:
    #     print(f"Erro no scheduler: {e}")


    

     