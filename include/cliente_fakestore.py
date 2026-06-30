"""Cliente HTTP para consumo dos dados da FakeStore API.

O módulo centraliza as chamadas aos endpoints externos utilizados pela DAG.
Essa separação evita acoplamento entre a lógica de orquestração do Airflow e
os detalhes de comunicação HTTP com a fonte de dados.
"""

from __future__ import annotations

import os
from typing import Any

import requests
from requests import Response


def obter_url_base() -> str:
    """Retorna a URL base da FakeStore API configurada no ambiente."""

    # A URL pode ser alterada por variável de ambiente para facilitar testes,
    # manutenção e execução em diferentes ambientes.
    return os.getenv(
        "FAKESTORE_API_BASE_URL",
        "https://fakestoreapi.com",
    ).rstrip("/")


def validar_resposta_http(resposta: Response, endpoint: str) -> None:
    """Valida a resposta HTTP recebida da API externa."""

    # A validação centralizada padroniza falhas de integração e evita que a DAG
    # continue a execução com dados incompletos ou resposta inválida.
    try:
        resposta.raise_for_status()
    except requests.HTTPError as erro:
        mensagem = (
            f"Falha ao consultar o endpoint '{endpoint}'. "
            f"Status HTTP: {resposta.status_code}."
        )
        raise RuntimeError(mensagem) from erro


def buscar_dados_endpoint(endpoint: str) -> list[dict[str, Any]]:
    """Busca uma lista de registros a partir do endpoint informado."""

    if not endpoint.strip():
        raise ValueError("O endpoint informado não pode ser vazio.")

    endpoint_normalizado = endpoint.strip().lstrip("/")
    url = f"{obter_url_base()}/{endpoint_normalizado}"

    # O timeout evita que a task fique bloqueada indefinidamente em caso de
    # instabilidade de rede ou indisponibilidade temporária da API.
    resposta = requests.get(url, timeout=30)
    validar_resposta_http(resposta=resposta, endpoint=endpoint_normalizado)

    dados = resposta.json()

    if not isinstance(dados, list):
        raise TypeError(
            f"O endpoint '{endpoint_normalizado}' não retornou uma lista."
        )

    # A DAG trabalha com listas de dicionários, pois cada item representa um
    # registro capturado da API e posteriormente normalizado para persistência.
    registros: list[dict[str, Any]] = []
    for item in dados:
        if not isinstance(item, dict):
            raise TypeError(
                f"O endpoint '{endpoint_normalizado}' retornou item inválido."
            )
        registros.append(item)

    return registros


def buscar_usuarios() -> list[dict[str, Any]]:
    """Busca os usuários disponíveis na FakeStore API."""

    return buscar_dados_endpoint("users")


def buscar_produtos() -> list[dict[str, Any]]:
    """Busca os produtos disponíveis na FakeStore API."""

    return buscar_dados_endpoint("products")


def buscar_carrinhos() -> list[dict[str, Any]]:
    """Busca os carrinhos disponíveis na FakeStore API."""

    return buscar_dados_endpoint("carts")
