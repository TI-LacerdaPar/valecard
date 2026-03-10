import requests
import pyodbc
import logging
import json
import os
from dotenv import load_dotenv
from datetime import datetime,timedelta

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)

# arquivo
file_handler = logging.FileHandler("console.log")
file_handler.setFormatter(formatter)

# terminal
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
user = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
api_key = os.getenv("API_KEY")

# pegar as data atuais de formas inseridas

# puxar os dado
API_URL = os.getenv("API_URL")
API_BODY = {
    "contratoCliente": "136768",
    "dataInicial": "2026-03-01",
    "dataFinal": "2026-03-09",
    "tipotransacao": "TODAS"
}

# pegar as datas atuais

today = datetime.today()


# data_inicial = (today - timedelta(days=7)).strftime("%Y-%m-%d")
# data_final = today.strftime("%Y-%m-%d")

# API_URL = os.getenv("API_URL")
# API_BODY = {
#     "contratoCliente": "136768",
#     "dataInicial": data_inicial,
#     "dataFinal": data_final,
#     "tipotransacao": "TODAS"
# }



API_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": api_key
}

# realiza a conexão com o banco de dados
def conectar_banco():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL SERVER}};SERVER={server};DATABASE={database};UID={user};PWD={password}"
        )
        logger.info(f"Conectado ao banco de dados {database} com sucesso...")
        return conn
    except Exception as e:
        logging.error(f"Erro ao conectar com o banco: {e}")
        raise

# captura os dados da API
def buscar_dados():
    headers = API_HEADERS.copy()
    try:
        response = requests.post(API_URL, headers=headers, json=API_BODY, timeout=30)
        response.raise_for_status()
        logger.info(f"dados enviados: {json.dumps(API_BODY, indent=2, ensure_ascii=False)}")
        logger.info(f"realizada consulta API com status {response.status_code}")
        logger.info(f"resposta da consulta a API: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao realizar consulta: {e}")
        return []

# realiza a conversão das datas
def converter_data(data_str):

    if not data_str:
        return None

    try:
        return datetime.strptime(data_str.strip(), "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return None

# salvar os dados da tabela de gastos
def salvar_dados_gastos(dados):

    conn = conectar_banco()
    cursor = conn.cursor()

    registros_atualizados = 0

    for item in dados:
        try:

            data_convertida = converter_data(item.get("data"))

            sql = """
                INSERT INTO Gastos (
                    data, motorista, numeroFrota, placa, modelo, hodometro,
                    nomeFantasia, estado, cidade, endereco, bairro, telefone,
                    produto, quantidade, valorUnitario, valorTotal,
                    limiteCredito, saldoAtual, distancia, consumo, unidade,
                    reaisPorKm, filialVeiculo, centroResultadoVeiculo,
                    numeroCartao, responsavel, tipoFrota, cnpj, tipoServico,
                    cpfMotorista, ID_produto, ID, centroCusto, codigo, codigoTerminal
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                item.get("codigoTerminal")
            )

            cursor.execute(sql, params)

            registros_atualizados += 1

        except Exception as e:
            logging.error(f"Erro ao inserir registro: {e}")
            continue

    conn.commit()
    conn.close()

    logging.info(f"{registros_atualizados} registros inseridos na tabela Gastos")

# realizar o salvamento dos dados de endereço

def salvar_dados_endereco(dados):

    conn = conectar_banco()
    cursor = conn.cursor()

    # cria tabela caso não exista
    cursor.execute("""
        IF OBJECT_ID('dbo.Endereco', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.Endereco (
                id INT IDENTITY(1,1) PRIMARY KEY,
                estado NVARCHAR(50),
                bairro NVARCHAR(150),
                cidade NVARCHAR(150),
                data DATETIME,
                endereco NVARCHAR(255)
            )
        END
    """)

    conn.commit()

    enderecos_inseridos = 0

    for item in dados:
        try:

            data_str = item.get("data")

            data_convertida = None
            if data_str:
                try:
                    data_convertida = datetime.strptime(data_str, "%d/%m/%Y %H:%M:%S")
                except:
                    logging.warning(f"Data inválida: {data_str}")

            sql = """
                INSERT INTO dbo.Endereco (estado, bairro, cidade, data, endereco)
                VALUES (?, ?, ?, ?, ?)
            """

            params = (
                item.get("estado"),
                item.get("bairro"),
                item.get("cidade"),
                data_convertida,
                item.get("endereco")
            )

            cursor.execute(sql, params)
            enderecos_inseridos += 1

        except Exception as e:
            logging.error(f"Erro ao inserir na tabela Endereco: {e}")
            continue

    conn.commit()
    conn.close()

    logging.info(f"{enderecos_inseridos} registros foram inseridos com sucesso na tabela Endereço")

# realiza a conversão das datas

def converter_data(data_str):
    if not data_str:
        return None
    
    try:
        return datetime.strptime(data_str.strip(), "%d/%m/%Y %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(data_str.strip(), "%d/%m/%Y")
        except ValueError:
            return None

# função para salvar os dados da tabela de motorista

def  salvar_dados_motorista(dados):
    conn = conectar_banco()
    cursor = conn.cursor()

    # criar tabela caso não exista
    cursor.execute("""
        IF OBJECT_ID('dbo.Motoristas', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.Motoristas (
                id INT IDENTITY(1,1) PRIMARY KEY,
                motorista NVARCHAR(200),
                cpfMotorista NVARCHAR(20),
                data DATETIME
            )
        END
    """)

    conn.commit()

    motorista_inseridos = 0

    for item in dados:
        try:

            data_str = item.get("data")
            data_convertida = None

            if data_str:
                try:
                    data_convertida = datetime.strptime(data_str, "%d/%m/%Y %H:%M:%S")
                except:
                    logging.warning(f"Data inválida: {data_str}")

            sql = """
                INSERT INTO dbo.Motoristas (motorista, cpfMotorista, data)
                VALUES (?, ?, ?)
            """

            params = (
                item.get("motorista"),
                item.get("cpfMotorista"),
                data_convertida
            )

            cursor.execute(sql, params)
            motorista_inseridos += 1

        except Exception as e:
            logging.error(f"Erro ao inserir registros na tabela Motoristas: {e}")
            continue

    conn.commit()
    conn.close()

    logging.info(f"{motorista_inseridos} registros inseridos na tabela Motoristas")

    # salvar os dados da tabela para salvar todos os valores

def salvar_dados_todos(dados):
    conn = conectar_banco()
    cursor = conn.cursor()

    registros_atualizados = 0

    # Cria a tabela se não existir
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Todos' AND xtype='U')
    BEGIN
        CREATE TABLE Todos (
            data DATETIME,
            motorista NVARCHAR(200),
            numeroFrota NVARCHAR(50),
            placa NVARCHAR(20),
            modelo NVARCHAR(200),
            hodometro FLOAT,
            nomeFantasia NVARCHAR(200),
            estado NVARCHAR(50),
            cidade NVARCHAR(100),
            endereco NVARCHAR(200),
            bairro NVARCHAR(100),
            telefone NVARCHAR(50),
            produto NVARCHAR(200),
            quantidade FLOAT,
            valorUnitario FLOAT,
            valorTotal FLOAT,
            limiteCredito FLOAT,
            saldoAtual FLOAT,
            distancia FLOAT,
            consumo FLOAT,
            unidade NVARCHAR(50),
            reaisPorKm FLOAT,
            filialVeiculo NVARCHAR(100),
            centroResultadoVeiculo NVARCHAR(100),
            numeroCartao NVARCHAR(100),
            responsavel NVARCHAR(200),
            tipoFrota NVARCHAR(100),
            cnpj NVARCHAR(30),
            tipoServico NVARCHAR(100),
            cpfMotorista NVARCHAR(30),
            ID_produto NVARCHAR(50),
            ID NVARCHAR(50),
            centroCusto NVARCHAR(100),
            codigo NVARCHAR(50),
            codigoTerminal NVARCHAR(50)
        )
    END
    """)

    for item in dados:
        try:
            sql = """
                INSERT INTO Todos (
                    data, motorista, numeroFrota, placa, modelo, hodometro,
                    nomeFantasia, estado, cidade, endereco, bairro, telefone,
                    produto, quantidade, valorUnitario, valorTotal,
                    limiteCredito, saldoAtual, distancia, consumo, unidade,
                    reaisPorKm, filialVeiculo, centroResultadoVeiculo,
                    numeroCartao, responsavel, tipoFrota, cnpj, tipoServico,
                    cpfMotorista, ID_produto, ID, centroCusto, codigo, codigoTerminal
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            params = (
                item.get("data"), item.get("motorista"), item.get("numeroFrota"), item.get("placa"),
                item.get("modelo"), item.get("hodometro"), item.get("nomeFantasia"), item.get("estado"),
                item.get("cidade"), item.get("endereco"), item.get("bairro"), item.get("telefone"),
                item.get("produto"), item.get("quantidade"), item.get("valorUnitario"), item.get("valorTotal"),
                item.get("limiteCredito"), item.get("saldoAtual"), item.get("distancia"), item.get("consumo"),
                item.get("unidade"), item.get("reaisPorKm"), item.get("filialVeiculo"),
                item.get("centroResultadoVeiculo"),
                item.get("numeroCartao"), item.get("responsavel"), item.get("tipoFrota"), item.get("cnpj"),
                item.get("tipoServico"), item.get("cpfMotorista"), item.get("ID_produto"), item.get("ID"),
                item.get("centroCusto"), item.get("codigo"), item.get("codigoTerminal")
            )

            cursor.execute(sql, params)
            registros_atualizados += 1

        except Exception as e:
            logging.error(f"Erro ao inserir {item} no banco: {e}")
            continue

    conn.commit()
    conn.close()

    logging.info(f"{registros_atualizados} registros foram inseridos com sucesso na Tabela Todos")


# criação da classe principal que executa as outras classes
def main():
    logging.info("Iniciando a Execução de Conexão da API")
    dados = buscar_dados()

    if not dados:
        logging.error("Erro ao receber dados da API")
        return
    logging.info(f"total de registros recebidos: {len(dados)}")
    salvar_dados_gastos(dados)
    salvar_dados_endereco(dados)
    salvar_dados_motorista(dados)
    salvar_dados_todos(dados)
    logging.info("Script finalizado com sucesso")
    
# realiza a instânciação da classe main
if __name__ == "__main__":
    main()