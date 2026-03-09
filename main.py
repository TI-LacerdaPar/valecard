import requests
import pyodbc
import logging
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

logging.basicConfig(filename="app.log", level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
user = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
api_key = os.getenv("API_KEY")

today = datetime.today()
data_inicial = (today-timedelta(days=7)).strftime("%Y/%m/%d")
data_final = today.strftime("%Y/%m/%d")

API_URL = os.getenv("APIURL")
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
        logging.info("Conectado ao banco com sucesso")
        return conn
    except Exception as e:
        logging.error(f"Erro ao conectar com o banco: {e}")
        raise

def buscar_dados():
    headers = API_HEADERS.copy()
    try:
        response = requests.post(API_URL, headers=headers, json=API_BODY, timeout=30)
        response.raise_for_status()
        logging.info(f"dados enviados: {json.dumps(API_BODY, indent=2, ensure_ascii=False)}")
        logging.info(f"realizada consulta API com status {response.status_code}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao realizar consulta: {e}")
        return []

def salvar_dados_banco(dados):
    conn = conectar_banco()
    cursor = conn.cursor()

    registros_atualizados = 0

    for item in dados:
        try:
            sql = """
                INSERT INTO Gastos (
                    data, numeroFrota, placa, modelo, hodometro,
                    nomeFantasia, telefone, produto, quantidade, valorUnitario,
                    valorTotal, limiteCredito, saldoAtual, distancia, consumo,
                    unidade,reaisPorKm, filialVeiculo, centroResultadoVeiculo,
                    numeroCartao, responsavel, tipoFrota, cnpj, tipoServico,
                    ID_produto, ID, centroCusto, codigo, codigoTerminal
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            params = (
                item.get("data"), item.get("numeroFrota"), item.get("placa"),
                item.get("modelo"), item.get("hodometro"), item.get("nomeFantasia"), item.get("telefone"),
                item.get("produto"), item.get("quantidade"), item.get("valorUnitario"), item.get("valorTotal"),
                item.get("limiteCredito"), item.get("saldoAtual"), item.get("distancia"), item.get("consumo"),
                item.get("unidade"), item.get("reaisPorKm"), item.get("filialVeiculo"),
                item.get("centroResultadoVeiculo"),
                item.get("numeroCartao"), item.get("responsavel"), item.get("tipoFrota"), item.get("cnpj"),
                item.get("tipoServico"), item.get("ID_produto"), item.get("ID"),
                item.get("centroCusto"), item.get("codigo"), item.get("codigoTerminal")
            )
            cursor.execute(sql, params)
            registros_atualizados += 1
        except Exception as e:
            logging.error(f"Erro ao inserir {item} na tabela gasto: {e}")
            continue
    conn.commit()
    conn.close()
    logging.info(f"{registros_atualizados} registros foram atualizados com sucesso")

def salvar_endereco(dados):
    conn = conectar_banco()
    cursor = conn.cursor()

    enderecos_inseridos = 0

    for item in dados:
        try:
            sql = """
                INSERT INTO Endereco (estado, bairro, cidade, data, endereco)
                VALUES (?, ?, ?, ?, ?)
            """
            params = (
                item.get("estado"),
                item.get("bairro"),
                item.get("cidade"),
                item.get("data"),
                item.get("endereco")
            )
            cursor.execute(sql, params)
            enderecos_inseridos += 1
        except Exception as e:
            logging.error(f"Erro ao inserir na tabela endereço: {e}")
            continue
    conn.commit()
    conn.close()
    logging.info(f"{enderecos_inseridos} registros foram inseridos com sucesso")


def salvar_motorista(dados):
    conn = conectar_banco()
    cursor = conn.cursor()
    motorista_inseridos = 0
    for item in dados:
        try:
            sql = """
                INSERT INTO Motoristas (motorista, cpfMotorista, data)
                VALUES (?, ?, ?)
            """
            params = (item.get("motorista"), item.get("cpfMotorista"), item.get("data"))
            cursor.execute(sql, params)
            motorista_inseridos += 1

        except Exception as e:
            logging.error(f"Erro ao inserir registros na tabela motorista: {e}")
            continue
    conn.commit()
    conn.close()
def salvar_dados_todos(dados):
    conn = conectar_banco()
    cursor = conn.cursor()

    registros_atualizados = 0

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
    logging.info(f"{registros_atualizados} registros foram atualizados com sucesso")


def main():
    logging.info("Iniciando execução de script")
    dados = buscar_dados()

    if not dados:
        logging.error("Erro ao receber dados da API")
        return
    logging.info(f"total de registros recebidos: {len(dados)}")
    salvar_dados_banco(dados)
    salvar_endereco(dados)
    salvar_motorista(dados)
    salvar_dados_todos(dados)
    logging.info("Script finalizado com sucesso")

if __name__ == "__main__":
    main()