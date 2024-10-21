import time
import pandas as pd
import datetime as dt
import os
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import delete
from datetime import timedelta
from pathlib import Path
from sqlalchemy import delete,text
from dotenv import load_dotenv
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
from selenium.webdriver.common.by import By
from selenium import webdriver
import msedgedriver
import logging
import dotenv
from logging.handlers import RotatingFileHandler
import requests
import json
from prefect import task,flow
from datetime import timedelta, datetime
from prefect.client.schemas.schedules import IntervalSchedule
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.blocks.webhook import Webhook

# webhook_block = Webhook.load("notificacaoerro")
# teams_webhook_block = MicrosoftTeamsWebhook.load("avisateams")

caminho_env = os.path.join(os.getcwd(), "enviroments", ".env") 
loadenv = dotenv.find_dotenv(filename=caminho_env)

dotenv.load_dotenv(loadenv)

horario_atual = dt.datetime.now()

# locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

# Configuração do logging
# logging.basicConfig(level=logging.INFO, 
#                     format='%(asctime)s - %(levelname)s - %(message)s')

# Cria um handler para arquivo (opcional)
# file_handler = RotatingFileHandler('meu_app.log', maxBytes=10000, backupCount=1)
# file_handler.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# file_handler.setFormatter(formatter)
# logging.getLogger().addHandler(file_handler)

# Configuração do banco de dados PostgreSQL
DB_HOST = "77.37.40.212"
DB_NAME = "db_cobranca"
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
PORT=5433

@task
def heartbeat():
    with open("heartbeat.txt", "w") as f:
        f.write(f"Heartbeat: {time.strftime('%Y-%m-%d %H:%M:%S')}")

@task
def inicializar_navegador():

    msedgedriver.install()
    caminho_chromedriver = "msedgedriver.exe"
    servico = Service(caminho_chromedriver)
    options = webdriver.EdgeOptions()
    # options.add_argument('--headless=new')
    options.add_argument('--ignore-certificate-errors')
    navegador = webdriver.Edge(service=servico,options=options)
    alerta=Alert(navegador)
    navegador.minimize_window()
    return navegador,alerta

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

    datahj=dt.datetime.now()
    datontem=datahj-timedelta(days=10)
    datontem=datontem.date().strftime("%d/%m/%Y")
    return data_acordo_inicio,data_acordo_fim,data_acordo_fim_receb,dia,mes,ano

@task
def obter_matricula():
    return os.getlogin()

@task         
def conectar_db():
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=PORT)
        return conn
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Erro ao conectar ao banco de dados: {error}")
        return None

# @asset(deps=[conectar_db]) 
# def registrar_log(mensagem, nivel="INFO"):
#     conn = conectar_db()
#     if conn:
#         try:
#             cursor = conn.cursor()
#             sql = "INSERT INTO logs_sheduler (data_hora, nivel, mensagem) VALUES (%s, %s, %s)"
#             cursor.execute(sql, (dt.datetime.now(), nivel, mensagem))
#             conn.commit()
#             logging.info("Log registrado no banco de dados com sucesso.")
#         except (Exception, psycopg2.Error) as error:
#             logging.error(f"Erro ao registrar log no banco de dados: {error}")
#         finally:
#             if conn:
#                 cursor.close()
#                 conn.close()

@task           
def registrar_atu():
    conn = conectar_db()
    data=dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    if conn:
        try:
            cursor = conn.cursor()
            sql = "UPDATE atualizacao VALUES SET atualizado_em= %s WHERE user_id=1 "
            cursor.execute(sql, data)
            conn.commit()
            logging.info("Atualização registrada no banco de dados com sucesso.")
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Erro ao registrar log atualização no banco de dados: {error}")
        finally:
            if conn:
                cursor.close()
                conn.close()
        return dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
@task
def post_webhook(url):
    """
    Envia um POST request para um webhook.

    Args:
        url: A URL do webhook.
        data: Um dicionário contendo os dados a serem enviados.

    Returns:
        A resposta do servidor.
    """
    
    atualizado_em=registrar_atu()

    data = {
        "mensagem": "Atualizado em: ",
        "valor": atualizado_em
    }

    headers = {'Content-type': 'application/json'}
    payload = json.dumps(data)
        
    try:
        response = requests.post(url, data=payload, headers=headers,verify=False)
        response.raise_for_status()  # Lança uma exceção para códigos de status de erro
        return response
    except requests.exceptions.RequestException as e:
        print(f"Erro ao enviar POST request para o webhook: {e}")
        return None
    
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
def ultimo_dia_do_mes():

    data_acordo_inicio,data_acordo_fim,data_acordo_fim_receb,dia,mes,ano=diasMesAno()

    if dia ==1:
        if mes==1:
            mes=12
            ano-=1
        else:
            mes-=1
    # Encontrar o último dia do mês
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    
    # Retornar a data no formato 'DD/MM/YYYY'
    return f'{ultimo_dia:02d}/{mes:02d}/{ano:04d}'

@task
def delRows (nomebs, dia):
    mes = dt.datetime.now().month
    conn = f"postgresql+psycopg2://postgres:Gaby030686@77.37.40.212:5433/db_cobranca"
    engine = create_engine(conn)

    with engine.begin() as connection:
        coluna_data = {
            "Liquidado": "Data_Liquidacao",
            "A_Receber": "Data_Vencimento"
        }.get(nomebs, "criada_em")  # Define a coluna de data dinamicamente

        mes_condicao = mes - 1 if dia == 1 else mes  # Simplifica a condição do mês

        connection.execute(text(
            f'DELETE FROM "{nomebs}" WHERE extract(month from "{coluna_data}") IN ({mes_condicao}, {mes})'
        ))

@task
def enviaBD(base, nomeBase,dia):

    base=base
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

@task
def gerar_dfLiquidado(navegador):

    data_acordo_inicio,data_acordo_fim,data_acordo_fim_receb,dia,mes,ano=diasMesAno()
    ultimo_dia=ultimo_dia_do_mes()
    ##GERAR RELATÓRIO LIQUIDADOS
    hora1=dt.datetime.now().strftime("%H:%M:%S")

    # registrar_log(f"Iniciando a importação do Relatório de Liquidados - {hora1}")

    navegador.get('https://sicob.uninter.com/Titulos/Negociados')

    navegador.find_element(By.ID,'data_liquidacao_inicio').send_keys(data_acordo_inicio)
    navegador.find_element(By.ID,'data_liquidacao_fim').send_keys(data_acordo_fim)
    navegador.find_element(By.ID,'situacao_titulo').send_keys('Liquidado')

    for _ in range(5):
        webdriver.ActionChains(navegador).send_keys(Keys.TAB).perform()
    webdriver.ActionChains(navegador).send_keys(Keys.ENTER).perform()

    while len(navegador.find_elements(By.CLASS_NAME,'fa-list')) < 1:
        time.sleep(1)

    time.sleep(2)

    fazer_download(navegador)

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

    hora1=dt.datetime.now().strftime("%H:%M:%S")
    
    print(f"Final da importação do Relatório de Liquidados - {hora1}")
    return dfLiquidado

@task
def gerar_dfAreceber(navegador):
    data_acordo_inicio,data_acordo_fim,data_acordo_fim_receb,dia,mes,ano=diasMesAno()
    ultimo_dia=ultimo_dia_do_mes()
    hora1=dt.datetime.now().strftime("%H:%M:%S")
    ##GERAR RELATÓRIO A_LIQUIDAR
    print(f"Iniciando a importação do Relatório do a Liquidar - {hora1}")

    # navegador.get(f'https://sicob.uninter.com/Login')  
    # navegador.get(f'https://sicob.uninter.com/Login')  
    # navegador.find_element(By.XPATH,'/html/body/div/div/form/div/a/button').click()
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


    hora1=dt.datetime.now().strftime("%H:%M:%S")
    return dfAreceber

@task
def gerar_dfNegociado(navegador):

    data_acordo_inicio,data_acordo_fim,data_acordo_fim_receb,dia,mes,ano=diasMesAno()
    ultimo_dia=ultimo_dia_do_mes()
    hora1=dt.datetime.now().strftime("%H:%M:%S")

    # registrar_log(f"Iniciando a importação do Relatório do NEGOCIADO - {hora1}")

    # navegador.get(f'https://sicob.uninter.com/Login')  
    # navegador.get(f'https://sicob.uninter.com/Login')  
    # navegador.find_element(By.XPATH,'/html/body/div/div/form/div/a/button').click()
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
    return dfNegociado

@flow(log_prints=True)
def gerarBases():
    matricula=obter_matricula()
    data_acordo_inicio,data_acordo_fim,data_acordo_fim_receb,dia,mes,ano=diasMesAno()  

    try:
        navegador,alerta=inicializar_navegador()

        navegador.get(f'https://sicob.uninter.com/Login')
        navegador.get(f'https://sicob.uninter.com/Login')
        navegador.find_element(By.XPATH,'/html/body/div/div/form/div/a/button').click()

        start_time = dt.datetime.now().strftime("%H:%M:%S")

        dfLiquidado=gerar_dfLiquidado(navegador)

        dfAreceber=gerar_dfAreceber(navegador)

        dfNegociado= gerar_dfNegociado(navegador)

        dataframes = {
            "Liquidado": dfLiquidado,
            "A_Receber": dfAreceber,
            "negociado_parcial": dfNegociado,
        }

        for nomeBase, base in dataframes.items():
            enviaBD(base, nomeBase, dia)

        data=dt.datetime.now().date()
        hora=dt.datetime.now().strftime("%H:%M:%S")

        hora1=dt.datetime.now().strftime("%H:%M:%S")

        # registrar_log(f"Processo Finalizado - {hora1}")
                        # Teste
        # webhook_url ="https://n8n-n8n.8t1f5e.easypanel.host/webhook-test/ce0f6eed-840b-459b-89fd-c8769d1853ce"
        
        #Produção
        url = "https://n8n-n8n.8t1f5e.easypanel.host/webhook/ce0f6eed-840b-459b-89fd-c8769d1853ce"
        urlteams="https://prod-153.westus.logic.azure.com:443/workflows/11ca64e420a54df58932132a55e3bd28/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=fh6WUPvgGfb-A_xSqKOlawGdW_koctIrdzOYwOqVCB0"
        response = post_webhook(url)
        response2 = post_webhook(urlteams)
        if response or response2:
            print(f"Resposta do webhook: {response.status_code} - {response.text}")

        # teams_webhook_block.notify("Hello from Prefect!")
        # heartbeat()
        # navegador.close()
        # scheduler = BlockingScheduler(executors={'default': ProcessPoolExecutor(6)})
        # scheduler.reschedule_job('gerarBases', trigger='interval', minutes=60,job_id="meu_aprov")

    except Exception as e:
        logging.error(f"Erro na função: {e}")
        # registrar_log(f"Erro na função: {e}", nivel="ERROR")
        urlerro="https://api.prefect.cloud/hooks/UNI5vWJApZut7ysPIDvezQ"
        response3 = post_webhook(urlerro)
        if response3:
            print(f"Resposta do webhook: {response3.status_code} - {response3.text}")

if __name__ == "__main__":
    # creates a deployment and stays running to monitor for work instructions 
    # generated on the server
    
    gerarBases.serve(name="Atualiza_Metas",
        tags=["atualiza_metas"],
        schedules=[
            IntervalSchedule(
            interval=timedelta(minutes=60))]
    )