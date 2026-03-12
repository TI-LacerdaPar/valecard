-- Script para a criação de Gastos

-- Criação do Novo Script para a Execução do Valecard


IF OBJECT_ID (N'dbo.Endereco') IS NOT NULL
    DROP TABLE dbo.Endereco
GO

CREATE TABLE dbo.Endereco(
    endereco_id INT IDENTITY(1,1) PRIMARY KEY,
    estado VARCHAR(200) NULL,
    bairro VARCHAR(200) NULL,
    cidade VARCHAR(200) NULL,
    endereco_data DATETIME NULL
)
GO


IF OBJECT_ID (N'dbo.Motoristas') IS NOT NULL
	DROP TABLE dbo.Motoristas
GO

CREATE TABLE dbo.Motoristas
	(
	  motoristas_id INT IDENTITY(1,1) PRIMARY KEY,
	  motorista_nome    VARCHAR (255) NULL,
	  motorista_cpf VARCHAR (20) NULL,
	  motorista_data DATETIME NULL
	)
GO

-- criação da tabela de Gasto, com relacionamenteo de muitos para muitos com endereço e motoristas

IF OBJECT_ID (N'dbo.Gastos') IS NOT NULL
    DROP TABLE dbo.Gastos
GO

CREATE TABLE dbo.Gastos (
    gastos_id INT IDENTITY(1,1) PRIMARY KEY,

    endereco_id INT NULL,
    motoristas_id INT NULL,

    data DATETIME NULL,
    numero_frota NVARCHAR(255) NULL,
    placa NVARCHAR(20) NULL,
    modelo NVARCHAR(200) NULL,
    hodometro FLOAT NULL,
    nome_fantasia NVARCHAR(255) NULL,
    telefone NVARCHAR(30) NULL,
    produto NVARCHAR(200) NULL,
    quantidade FLOAT NULL,
    valor_unitario DECIMAL(18,2) NULL,
    valor_total DECIMAL(18,2) NULL,
    limite_credito DECIMAL(18,2) NULL,
    saldo_atual DECIMAL(18,2) NULL,
    distancia FLOAT NULL,
    consumo FLOAT NULL,
    unidade NVARCHAR(50) NULL,
    reais_por_km DECIMAL(18,4) NULL,
    filial_veiculo NVARCHAR(200) NULL,
    centro_veiculo NVARCHAR(200) NULL,
    numero_cartao NVARCHAR(50) NULL,
    responsavel_empresa NVARCHAR(255) NULL,
    tipo_frota NVARCHAR(100) NULL,
    cnpj NVARCHAR(100) NULL,
    tipo_servico NVARCHAR(200) NULL,
    id_produto INT NULL,
    id_api BIGINT NULL,
    centro_custo NVARCHAR(255) NULL,
    codigo NVARCHAR(100) NULL,
    codigo_terminal NVARCHAR(255) NULL
    
    -- relacionamentos de muitos para muitos

    CONSTRAINT FK_Gastos_Endereco
        FOREIGN KEY (endereco_id)
        REFERENCES Endereco(endereco_id),

    CONSTRAINT FK_Gastos_Motorista
        FOREIGN KEY (motoristas_id)
        REFERENCES Motoristas(motoristas_id)
)
GO
