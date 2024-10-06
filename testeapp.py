import logging
from datetime import datetime
import os
import dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from logging.handlers import RotatingFileHandler
from apscheduler.triggers.cron import CronTrigger
import time
import psycopg2

loadenv = dotenv.find_dotenv()
dotenv.load_dotenv(loadenv)


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
            cursor.execute(sql, (datetime.now(), nivel, mensagem))
            conn.commit()
            logging.info("Log registrado no banco de dados com sucesso.")
        except (Exception, psycopg2.Error) as error:
            logging.error(f"Erro ao registrar log no banco de dados: {error}")
        finally:
            if conn:
                cursor.close()
                conn.close()

def tick():
    try:
        # Seu código aqui
        print('Tick! The time is: %s' % datetime.now())
        registrar_log("Tick executado com sucesso!")
        heartbeat()
    except Exception as e:
        logging.error(f"Erro na função tick: {e}")
        registrar_log(f"Erro na função tick: {e}", nivel="ERROR")

if __name__ == '__main__':
    scheduler = BlockingScheduler(executors={'default': ProcessPoolExecutor(5)})
    scheduler.add_job(tick, 'interval', minutes=1)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass