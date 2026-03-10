# Automação do ValeCard

## Objetivo:

Automação que faz a integração entre a API do valeshop, obtendo os dados para fazer a persitência dos dados no banco de dados SQL Server.

## Sobre:

Integração que faz a obtenção dos dados através da API, em seguida realiza a persistência dos dados no banco de dados Valecard. E finaliza com a conexão e extração dos dados do PowerBI.

## Descrição do Projeto:

Este script é responsável por integrar dados de uma API externa de transações financeiras com um banco de dados SQL Server, realizando a coleta, o processamento e a persistência dessas informações de forma automatizada.

### O fluxo principal consiste em:

Consultar uma API REST autenticada.
Receber dados de transações (ex: abastecimentos, gastos ou serviços).
Armazenar esses dados em uma tabela específica do banco de dados.
O script foi projetado para execução manual ou automatizada (ex: via Agendador de Tarefas no Windows ou CRON no Linux).

### Objetivo Principal

Automatizar a ingestão de dados financeiros provenientes de uma API externa, garantindo que todas as transações retornadas sejam armazenadas corretamente em banco de dados para posterior análise, relatórios ou auditoria.

Dependências

requests
Utilizada para realizar chamadas HTTP POST à API externa.

pyodbc
Driver responsável pela conexão e execução de comandos em banco de dados Microsoft SQL Server.

dotenv
Carrega variáveis de ambiente a partir de um arquivo .env, evitando exposição de credenciais no código.

logging
Responsável pelo controle de logs de execução, erros e auditoria do processo.

json
Utilizada para formatação e registro das respostas da API em log.

Configuração (.env)

O script depende das seguintes variáveis de ambiente para funcionamento correto:

DB_SERVER
Endereço do servidor SQL Server.

DB_DATABASE
Nome do banco de dados onde os dados serão persistidos.

DB_USERNAME
Usuário de acesso ao banco de dados.

DB_PASSWORD
Senha do usuário de banco de dados.

API_KEY
Token de autenticação utilizado no header Authorization da API.

APIURL
Endpoint da API responsável por retornar os dados de transações.

### Constantes do Script

API_BODY
Objeto que define os parâmetros enviados à API, incluindo:

contratoCliente
dataInicial
dataFinal
tipotransacao
Esses parâmetros determinam o escopo dos dados retornados pela API.

API_HEADERS
Define os headers da requisição HTTP, incluindo:

Content-Type: application/json
Authorization: API_KEY
Detalhamento das Funções

conectar_banco()
Objetivo
Responsável por estabelecer conexão com o banco de dados SQL Server.

### Lógica

Utiliza pyodbc para abrir a conexão.
Usa as variáveis de ambiente para montar a string de conexão.
Registra sucesso ou erro no log.
Retorna o objeto de conexão ativa.
Em caso de falha, gera exceção e interrompe a execução.
buscar_dados()
Objetivo
Realizar a consulta à API externa e obter os dados de transações.

### Lógica

Executa uma requisição HTTP POST para o endpoint configurado em APIURL.
Envia o corpo da requisição definido em API_BODY.
Inclui autenticação via header Authorization.
Registra em log:
Payload enviado
Status HTTP da resposta
Conteúdo completo retornado pela API
Retorna o JSON da resposta em caso de sucesso.
Em caso de erro HTTP ou de conexão, registra o erro e retorna uma lista vazia.
salvar_dados_banco(dados)
Objetivo
Persistir os dados retornados pela API na tabela Gastos do banco de dados.

### Parâmetros
dados: Lista de registros retornados pela API.

### Lógica

Abre conexão com o banco utilizando conectar_banco().
Itera sobre cada item retornado pela API.
Executa um INSERT na tabela Gastos com todos os campos mapeados.
Cada campo do banco é preenchido utilizando item.get(), evitando falhas por ausência de chaves.
Conta quantos registros foram inseridos com sucesso.
Em caso de erro em um registro específico:
Registra o erro no log.
Continua a execução para os próximos registros.
Realiza commit ao final do processo.
Fecha a conexão com o banco.
Registra no log o total de registros inseridos com sucesso.
main()
Objetivo
Função principal que controla o fluxo completo do script.

### Lógica

Registra o início da execução.
Chama buscar_dados() para consultar a API.
Valida se houve retorno de dados.
Registra a quantidade total de registros recebidos.
Chama salvar_dados_banco() para persistir os dados.
Registra o encerramento bem-sucedido da execução.
Execução

O script pode ser executado manualmente via linha de comando:

python main.py

Também pode ser integrado a:

Agendador de Tarefas do Windows
CRON em ambientes Linux
Pipelines de automação
Logs

Os logs de execução são gravados no arquivo:

console.log

Incluem:

Conexão com banco de dados
Requisições à API
Payloads enviados
Respostas recebidas
Erros de inserção
Status final da execução
Considerações Finais

Este script foi desenvolvido com foco em:

Robustez
Rastreamento via logs
Isolamento de falhas por registro
Facilidade de automação futura
Pode ser facilmente adaptado para:

UPSERT (MERGE)
Controle de duplicidade
Paginação de API
Execução incremental por data

## Instalações e Atualizações

git clone https://github.com/TI-LacerdaPar/valecard


- Realiza a criação do ambiente virtual
python -m venv venv

- Realiza a criação do arquivo requirements
python -r requirements.txt

### Orientaçõs

As informações em relação a configuração da API e do banco estão no arquivo com as variáveis que serão utilizados para cinfiguração.

## Tecnologias

- Python
- SQL Server
- Power BI

## Contato

Vanessa Andrade