-- Script de criação das tabelas operacionais do marketplace.
-- As tabelas recebem os dados capturados da FakeStore API pela DAG do Airflow.
-- A estrutura preserva campos principais para consulta e mantém o payload
-- original em JSONB para auditoria, rastreabilidade e futuras evoluções.

CREATE SCHEMA IF NOT EXISTS marketplace;

-- Tabela responsável por armazenar os usuários capturados do endpoint /users.
CREATE TABLE IF NOT EXISTS marketplace.usuarios (
    id_api INTEGER PRIMARY KEY,
    email TEXT,
    usuario TEXT,
    nome TEXT,
    telefone TEXT,
    cidade TEXT,
    rua TEXT,
    numero INTEGER,
    cep TEXT,
    latitude TEXT,
    longitude TEXT,
    dados_originais JSONB NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tabela responsável por armazenar os produtos capturados do endpoint /products.
CREATE TABLE IF NOT EXISTS marketplace.produtos (
    id_api INTEGER PRIMARY KEY,
    titulo TEXT,
    preco NUMERIC(12, 2),
    descricao TEXT,
    categoria TEXT,
    imagem TEXT,
    avaliacao_nota NUMERIC(4, 2),
    avaliacao_quantidade INTEGER,
    dados_originais JSONB NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tabela responsável por armazenar os carrinhos/vendas capturados do endpoint /carts.
CREATE TABLE IF NOT EXISTS marketplace.carrinhos (
    id_api INTEGER PRIMARY KEY,
    usuario_id INTEGER,
    data_compra TIMESTAMPTZ,
    quantidade_itens INTEGER,
    dados_originais JSONB NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices auxiliares para consultas comuns e validações operacionais.
CREATE INDEX IF NOT EXISTS idx_usuarios_email
    ON marketplace.usuarios (email);

CREATE INDEX IF NOT EXISTS idx_produtos_categoria
    ON marketplace.produtos (categoria);

CREATE INDEX IF NOT EXISTS idx_carrinhos_usuario_id
    ON marketplace.carrinhos (usuario_id);
