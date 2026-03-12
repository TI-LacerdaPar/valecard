import requests
import pyodbc
import logging
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)

file_handler = logging.FileHandler("console.log")
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

# Variáveis de ambiente

server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
user = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
api_key = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")

# Datas da consulta

today = datetime.today()

data_inicial = (today - timedelta(days=7)).strftime("%Y-%m-%d")
data_final = today.strftime("%Y-%m-%d")

logger.info(f"Data inicial: {data_inicial}")
logger.info(f"Data final: {data_final}")

API_BODY = {
    "contratoCliente": "136768",
    "dataInicial": data_inicial,
    "dataFinal": data_final,
    "tipotransacao": "TODAS"
}

API_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": api_key
}

# API_BODY = {
#     "contratoCliente": "136768",
#     "dataInicial": "2026-02-01",
#     "dataFinal": "2026-02-28",
#     "tipotransacao": "TODAS"
# }

# API_HEADERS = {
#     "Content-Type": "application/json",
#     "Authorization": api_key
# }

# conexão banco

def conectar_banco():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={user};PWD={password}"
        )
        logger.info(f"Conectado ao banco {database}")
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar: {e}")
        raise


# buscar dados da API

def buscar_dados():
    try:
        response = requests.post(API_URL, headers=API_HEADERS, json=API_BODY, timeout=30)
        response.raise_for_status()
        logger.info(response.text)

        logger.info(f"Consulta API realizada com status {response.status_code}")

        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao consultar API: {e}")
        return []


# converter data

def converter_data(data_str):
    if not data_str:
        return None

    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%b %d %Y %I:%M%p"):
        try:
            return datetime.strptime(data_str.strip(), fmt)
        except ValueError:
            continue

    return None

def salvar_dados_gastos(dados):

    conn = conectar_banco()
    cursor = conn.cursor()

    cursor.execute("""
    IF OBJECT_ID(N'dbo.Gastos', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.Gastos (
            data DATETIME,
            motorista NVARCHAR(255),
            numeroFrota FLOAT,
            placa NVARCHAR(20),
            modelo NVARCHAR(100),
            hodometro FLOAT,
            nomeFantasia NVARCHAR(255),
            estado NVARCHAR(50),
            cidade NVARCHAR(100),
            endereco NVARCHAR(100),
            bairro NVARCHAR(200),
            telefone NVARCHAR(20),
            produto NVARCHAR(100),
            quantidade FLOAT,
            valorUnitario DECIMAL(18,2),
            valorTotal DECIMAL(18,2),
            limiteCredito DECIMAL(18,2),
            saldoAtual DECIMAL(18,2),
            distancia FLOAT,
            consumo FLOAT,
            unidade NVARCHAR(50),
            reaisPorKm DECIMAL(18,4),
            filialVeiculo NVARCHAR(200),
            centroResultadoVeiculo NVARCHAR(100),
            numeroCartao NVARCHAR(50),
            responsavel NVARCHAR(100),
            tipoFrota NVARCHAR(100),
            cnpj NVARCHAR(50),
            tipoServico NVARCHAR(100),
            cpfMotorista NVARCHAR(50),
            produtoId NVARCHAR(50),
            id FLOAT,
            centroCusto NVARCHAR(100),
            codigo NVARCHAR(100),
            classificacaoContabil NVARCHAR(30),
            codigoTerminal NVARCHAR(30),

        )
    END
    """)

    conn.commit()

    registros = 0

    for item in dados:

        try:

            data_convertida = converter_data(item.get("data"))

            sql = """
            INSERT INTO dbo.Gastos (
            data, motorista, numeroFrota, placa, modelo, hodometro,
            nomeFantasia, estado, cidade, endereco, bairro, telefone, produto, quantidade,
            valorUnitario, valorTotal, limiteCredito,
            saldoAtual, distancia, consumo, unidade,
            reaisPorKm, filialVeiculo, centroResultadoVeiculo,
            numeroCartao, responsavel, tipoFrota,
            cnpj, tipoServico, cpfMotorista, produtoId, id,
            centroCusto, codigo, classificacaoContabil, codigoTerminal
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            params = (
                data_convertida,
                item.get("motorista"),
                item.get("numeroFrota"),
                item.get("placa"),
                item.get("modelo"),
                item.get("hodometro"),
                item.get("nomeFantasia"),
                item.get("estado"),
                item.get("cidade"),
                item.get("endereco"),
                item.get("bairro"),
                item.get("telefone"),
                item.get("produto"),
                item.get("quantidade"),
                item.get("valorUnitario"),
                item.get("valorTotal"),
                item.get("limiteCredito"),
                item.get("saldoAtual"),
                item.get("distancia"),
                item.get("consumo"),
                item.get("unidade"),
                item.get("reaisPorKm"),
                item.get("filialVeiculo"),
                item.get("centroResultadoVeiculo"),
                item.get("numeroCartao"),
                item.get("responsavel"), 
                item.get("tipoFrota"),
                item.get("cnpj"),
                item.get("tipoServico"),
                item.get("cpfMotorista"),
                item.get("produtoId"),
                item.get("id"),
                item.get("centroCusto"),
                item.get("codigo"),
                item.get("classificacaoContabil"), 
                item.get("codigoTerminal")
            )

            cursor.execute(sql, params)

            registros += 1

        except Exception as e:
            logger.error(f"Erro ao inserir gasto: {e}")

    conn.commit()
    conn.close()

    logger.info(f"{registros} registros inseridos na tabela Gastos com sucesso")


# execução principal

def main():

    logger.info("Iniciando extração da API")

    dados = buscar_dados()

    if not dados:
        logger.error("Nenhum dado retornado da API")
        return

    logger.info(f"{len(dados)} registros recebidos")

    salvar_dados_gastos(dados)

    logger.info("Execução finalizada com sucesso")


if __name__ == "__main__":
    main()