"""Camada de persistência do pipeline no banco PostgreSQL.

Este módulo concentra a conexão com o banco operacional do marketplace e as
operações de inserção dos dados normalizados. A separação permite que a DAG
permaneça focada na orquestração do fluxo.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
from psycopg2.extensions import connection as ConexaoPostgres
from psycopg2.extras import Json, execute_values

Registro = dict[str, Any]


def criar_conexao() -> ConexaoPostgres:
    """Cria uma conexão com o banco operacional do marketplace."""

    # As credenciais são lidas do ambiente para evitar valores fixos no código
    # e manter compatibilidade com a execução em containers Docker.
    return psycopg2.connect(
        host=os.getenv("MARKETPLACE_DB_HOST", "postgres_marketplace"),
        port=int(os.getenv("MARKETPLACE_DB_PORT", "5432")),
        user=os.getenv("MARKETPLACE_DB_USER", "marketplace"),
        password=os.getenv("MARKETPLACE_DB_PASSWORD", "marketplace"),
        dbname=os.getenv("MARKETPLACE_DB_NAME", "marketplace"),
    )


@contextmanager
def obter_conexao() -> Iterator[ConexaoPostgres]:
    """Gerencia abertura, confirmação e fechamento da conexão."""

    conexao = criar_conexao()

    try:
        yield conexao
        conexao.commit()
    except Exception:
        conexao.rollback()
        raise
    finally:
        conexao.close()


def executar_script_criacao_tabelas(caminho_script: str) -> None:
    """Executa o script SQL responsável pela criação das tabelas."""

    with open(caminho_script, encoding="utf-8") as arquivo_sql:
        conteudo_sql = arquivo_sql.read()

    # A execução do script prepara o schema operacional antes das cargas,
    # permitindo que a DAG seja reexecutada de forma controlada.
    with obter_conexao() as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(conteudo_sql)


def inserir_usuarios(registros: list[Registro]) -> int:
    """Insere ou atualiza usuários no banco operacional."""

    if not registros:
        return 0

    comando_sql = """
        INSERT INTO marketplace.usuarios (
            id_api,
            email,
            usuario,
            nome,
            telefone,
            cidade,
            rua,
            numero,
            cep,
            latitude,
            longitude,
            dados_originais
        )
        VALUES %s
        ON CONFLICT (id_api) DO UPDATE SET
            email = EXCLUDED.email,
            usuario = EXCLUDED.usuario,
            nome = EXCLUDED.nome,
            telefone = EXCLUDED.telefone,
            cidade = EXCLUDED.cidade,
            rua = EXCLUDED.rua,
            numero = EXCLUDED.numero,
            cep = EXCLUDED.cep,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            dados_originais = EXCLUDED.dados_originais,
            atualizado_em = NOW();
    """

    valores = [
        (
            registro["id_api"],
            registro["email"],
            registro["usuario"],
            registro["nome"],
            registro["telefone"],
            registro["cidade"],
            registro["rua"],
            registro["numero"],
            registro["cep"],
            registro["latitude"],
            registro["longitude"],
            Json(registro["dados_originais"]),
        )
        for registro in registros
    ]

    with obter_conexao() as conexao:
        with conexao.cursor() as cursor:
            execute_values(cursor, comando_sql, valores)

    return len(registros)


def inserir_produtos(registros: list[Registro]) -> int:
    """Insere ou atualiza produtos no banco operacional."""

    if not registros:
        return 0

    comando_sql = """
        INSERT INTO marketplace.produtos (
            id_api,
            titulo,
            preco,
            descricao,
            categoria,
            imagem,
            avaliacao_nota,
            avaliacao_quantidade,
            dados_originais
        )
        VALUES %s
        ON CONFLICT (id_api) DO UPDATE SET
            titulo = EXCLUDED.titulo,
            preco = EXCLUDED.preco,
            descricao = EXCLUDED.descricao,
            categoria = EXCLUDED.categoria,
            imagem = EXCLUDED.imagem,
            avaliacao_nota = EXCLUDED.avaliacao_nota,
            avaliacao_quantidade = EXCLUDED.avaliacao_quantidade,
            dados_originais = EXCLUDED.dados_originais,
            atualizado_em = NOW();
    """

    valores = [
        (
            registro["id_api"],
            registro["titulo"],
            registro["preco"],
            registro["descricao"],
            registro["categoria"],
            registro["imagem"],
            registro["avaliacao_nota"],
            registro["avaliacao_quantidade"],
            Json(registro["dados_originais"]),
        )
        for registro in registros
    ]

    with obter_conexao() as conexao:
        with conexao.cursor() as cursor:
            execute_values(cursor, comando_sql, valores)

    return len(registros)


def inserir_carrinhos(registros: list[Registro]) -> int:
    """Insere ou atualiza carrinhos no banco operacional."""

    if not registros:
        return 0

    comando_sql = """
        INSERT INTO marketplace.carrinhos (
            id_api,
            usuario_id,
            data_compra,
            quantidade_itens,
            dados_originais
        )
        VALUES %s
        ON CONFLICT (id_api) DO UPDATE SET
            usuario_id = EXCLUDED.usuario_id,
            data_compra = EXCLUDED.data_compra,
            quantidade_itens = EXCLUDED.quantidade_itens,
            dados_originais = EXCLUDED.dados_originais,
            atualizado_em = NOW();
    """

    valores = [
        (
            registro["id_api"],
            registro["usuario_id"],
            registro["data_compra"],
            registro["quantidade_itens"],
            Json(registro["dados_originais"]),
        )
        for registro in registros
    ]

    with obter_conexao() as conexao:
        with conexao.cursor() as cursor:
            execute_values(cursor, comando_sql, valores)

    return len(registros)


def inserir_registros(tipo_dado: str, registros: list[Registro]) -> int:
    """Direciona a persistência conforme o tipo de dado processado."""

    persistencias = {
        "usuarios": inserir_usuarios,
        "produtos": inserir_produtos,
        "carrinhos": inserir_carrinhos,
    }

    if tipo_dado not in persistencias:
        raise ValueError(f"Tipo de dado não suportado: {tipo_dado}")

    return persistencias[tipo_dado](registros)
