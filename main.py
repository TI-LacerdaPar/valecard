import requests
import pyodbc
import logging
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import calendar

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

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
user = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
api_key = os.getenv("API_KEY")

# captura os dados por mês

# API_BODY = {
#     "contratoCliente": "136768",
#     "dataInicial": "2026-03-01",
#     "dataFinal": "2026-03-09",
#     "tipotransacao": "TODAS"
# }

# capturar os dados da api de forma mensal

today = datetime.today()

data_inicial = (today - timedelta(days=7)).strftime("%Y-%m-%d")
data_final = today.strftime("%Y-%m-%d")

logger.info(f"Data inicial: {data_inicial}")
logger.info(f"Data final: {data_final}")

API_URL = os.getenv("API_URL")

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

def conectar_banco():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL SERVER}};SERVER={server};DATABASE={database};UID={user};PWD={password}"
        )
        logger.info(f"Conectado ao banco {database}")
        return conn
    except Exception as e:
        logging.error(f"Erro ao conectar: {e}")
        raise


def buscar_dados():
    try:
        response = requests.post(API_URL, headers=API_HEADERS, json=API_BODY, timeout=30)
        response.raise_for_status()

        logger.info(f"Consulta API realizada com status {response.status_code}")
        print(response)

        return response.json()

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao consultar API: {e}")
        return []

# def converter_data(data_str):

#     if not data_str:
#         return None

#     try:
#         return datetime.strptime(data_str.strip(), "%d/%m/%Y %H:%M:%S")
#     except ValueError:
#         try:
#             return datetime.strptime(data_str.strip(), "%d/%m/%Y")
#         except ValueError:
#             return None
        
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

    registros = 0

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

            registros += 1

        except Exception as e:
            logging.error(f"Erro ao inserir registro: {e}")
            continue

    conn.commit()
    conn.close()

    logger.info(f"{registros} registros inseridos na tabela Gastos")


def salvar_dados_endereco(dados):

    conn = conectar_banco()
    cursor = conn.cursor()

    registros = 0

    for item in dados:

        try:

            data_convertida = converter_data(item.get("data"))

            sql = """
            INSERT INTO Endereco (estado, bairro, cidade, data, endereco)
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

            registros += 1

        except Exception as e:
            logging.error(f"Erro ao inserir endereço: {e}")
            continue

    conn.commit()
    conn.close()

    logger.info(f"{registros} registros inseridos na tabela Endereco")


def salvar_dados_motorista(dados):

    conn = conectar_banco()
    cursor = conn.cursor()

    registros = 0

    for item in dados:

        try:

            data_convertida = converter_data(item.get("data"))

            sql = """
            INSERT INTO Motoristas (motorista, cpfMotorista, data)
            VALUES (?, ?, ?)
            """

            params = (
                item.get("motorista"),
                item.get("cpfMotorista"),
                data_convertida
            )

            cursor.execute(sql, params)

            registros += 1

        except Exception as e:
            logging.error(f"Erro ao inserir motorista: {e}")
            continue

    conn.commit()
    conn.close()

    logger.info(f"{registros} registros inseridos na tabela Motoristas")


def salvar_dados_todos(dados):

    conn = conectar_banco()
    cursor = conn.cursor()

    registros = 0

    for item in dados:

        try:

            data_convertida = converter_data(item.get("data"))

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

            registros += 1

        except Exception as e:
            logging.error(f"Erro ao inserir registro: {e}")
            continue

    conn.commit()
    conn.close()

    logger.info(f"{registros} registros inseridos na tabela Todos")

def ajustar_datas_endereco():

    conn = conectar_banco()
    cursor = conn.cursor()

    try:

        sql = """
        UPDATE Endereco
        SET data = CONVERT(VARCHAR(19), TRY_CONVERT(DATETIME, data, 100), 103)
                   + ' ' 
                   + CONVERT(VARCHAR(8), TRY_CONVERT(DATETIME, data, 100), 108)
        WHERE TRY_CONVERT(DATETIME, data, 100) IS NOT NULL
        """

        cursor.execute(sql)
        conn.commit()

        logger.info("Datas da tabela Todos ajustadas com sucesso")

    except Exception as e:
        logger.error(f"Erro ao ajustar datas: {e}")

    finally:
        conn.close()

def ajustar_datas_gastos():

    conn = conectar_banco()
    cursor = conn.cursor()

    try:

        sql = """
        UPDATE Gastos
        SET data = CONVERT(VARCHAR(19), TRY_CONVERT(DATETIME, data, 100), 103)
                   + ' ' 
                   + CONVERT(VARCHAR(8), TRY_CONVERT(DATETIME, data, 100), 108)
        WHERE TRY_CONVERT(DATETIME, data, 100) IS NOT NULL
        """

        cursor.execute(sql)
        conn.commit()

        logger.info("Datas da tabela Todos ajustadas com sucesso")

    except Exception as e:
        logger.error(f"Erro ao ajustar datas: {e}")

    finally:
        conn.close()


def ajustar_datas_motoristas():

    conn = conectar_banco()
    cursor = conn.cursor()

    try:

        sql = """
        UPDATE Motoristas
        SET data = CONVERT(VARCHAR(19), TRY_CONVERT(DATETIME, data, 100), 103)
                   + ' ' 
                   + CONVERT(VARCHAR(8), TRY_CONVERT(DATETIME, data, 100), 108)
        WHERE TRY_CONVERT(DATETIME, data, 100) IS NOT NULL
        """

        cursor.execute(sql)
        conn.commit()

        logger.info("Datas da tabela Todos ajustadas com sucesso")

    except Exception as e:
        logger.error(f"Erro ao ajustar datas: {e}")

    finally:
        conn.close()

def ajustar_datas_todos():

    conn = conectar_banco()
    cursor = conn.cursor()

    try:

        sql = """
        UPDATE Todos
        SET data = CONVERT(VARCHAR(19), TRY_CONVERT(DATETIME, data, 100), 103)
                   + ' ' 
                   + CONVERT(VARCHAR(8), TRY_CONVERT(DATETIME, data, 100), 108)
        WHERE TRY_CONVERT(DATETIME, data, 100) IS NOT NULL
        """

        cursor.execute(sql)
        conn.commit()

        logger.info("Datas da tabela Todos ajustadas com sucesso")

    except Exception as e:
        logger.error(f"Erro ao ajustar datas: {e}")

    finally:
        conn.close()


def main():

    logger.info("Iniciando execução da API")

    dados = buscar_dados()

    if not dados:
        logger.error("Nenhum dado retornado da API")
        return
    
    # salvar no banco de dados

    logger.info(f"{len(dados)} registros recebidos")
    salvar_dados_gastos(dados)
    salvar_dados_endereco(dados)
    salvar_dados_motorista(dados)
    salvar_dados_todos(dados)
    
    # faz o ajustes para salvar os dados no formato correto

    logger.info("Fazendo o salvamento dos dados")

    ajustar_datas_endereco()
    ajustar_datas_gastos()
    ajustar_datas_motoristas()
    ajustar_datas_motoristas()
    ajustar_datas_todos() 

    logger.info("Fazendo ajustes nas datas")

    logger.info("Script finalizado com sucesso")


if __name__ == "__main__":
    main()