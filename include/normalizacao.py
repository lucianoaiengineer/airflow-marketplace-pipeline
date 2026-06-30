"""Funções de normalização dos registros capturados da FakeStore API.

A API retorna objetos com estruturas aninhadas. Este módulo transforma esses
objetos em dicionários compatíveis com as tabelas operacionais do PostgreSQL,
preservando também o payload original para rastreabilidade.
"""

from __future__ import annotations

from typing import Any

Registro = dict[str, Any]


def obter_dicionario(valor: Any) -> Registro:
    """Retorna o valor recebido quando ele for dicionário."""

    # Campos aninhados da API podem não existir. Essa proteção evita erros de
    # acesso a chaves quando o payload vier incompleto.
    if isinstance(valor, dict):
        return valor
    return {}


def obter_lista(valor: Any) -> list[Any]:
    """Retorna o valor recebido quando ele for lista."""

    if isinstance(valor, list):
        return valor
    return []


def converter_inteiro(valor: Any) -> int | None:
    """Converte valores compatíveis para inteiro."""

    if valor is None or valor == "":
        return None

    try:
        return int(valor)
    except (TypeError, ValueError):
        return None


def converter_float(valor: Any) -> float | None:
    """Converte valores compatíveis para número decimal."""

    if valor is None or valor == "":
        return None

    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def normalizar_usuario(registro: Registro) -> Registro:
    """Normaliza um usuário da FakeStore API para o modelo relacional."""

    endereco = obter_dicionario(registro.get("address"))
    geolocalizacao = obter_dicionario(endereco.get("geolocation"))
    nome_api = obter_dicionario(registro.get("name"))

    primeiro_nome = str(nome_api.get("firstname") or "").strip()
    sobrenome = str(nome_api.get("lastname") or "").strip()
    nome_completo = " ".join(
        parte for parte in [primeiro_nome, sobrenome] if parte
    )

    # O payload original é preservado em JSONB para auditoria e para permitir
    # futuras mudanças de modelagem sem perda dos dados brutos capturados.
    return {
        "id_api": converter_inteiro(registro.get("id")),
        "email": registro.get("email"),
        "usuario": registro.get("username"),
        "nome": nome_completo or None,
        "telefone": registro.get("phone"),
        "cidade": endereco.get("city"),
        "rua": endereco.get("street"),
        "numero": converter_inteiro(endereco.get("number")),
        "cep": endereco.get("zipcode"),
        "latitude": geolocalizacao.get("lat"),
        "longitude": geolocalizacao.get("long"),
        "dados_originais": registro,
    }


def normalizar_produto(registro: Registro) -> Registro:
    """Normaliza um produto da FakeStore API para o modelo relacional."""

    avaliacao = obter_dicionario(registro.get("rating"))

    return {
        "id_api": converter_inteiro(registro.get("id")),
        "titulo": registro.get("title"),
        "preco": converter_float(registro.get("price")),
        "descricao": registro.get("description"),
        "categoria": registro.get("category"),
        "imagem": registro.get("image"),
        "avaliacao_nota": converter_float(avaliacao.get("rate")),
        "avaliacao_quantidade": converter_inteiro(avaliacao.get("count")),
        "dados_originais": registro,
    }


def normalizar_carrinho(registro: Registro) -> Registro:
    """Normaliza um carrinho da FakeStore API para o modelo relacional."""

    produtos = obter_lista(registro.get("products"))

    # A quantidade de itens considera a soma das quantidades de produtos no
    # carrinho, representando melhor o volume operacional da venda.
    quantidade_itens = 0
    for produto in produtos:
        if isinstance(produto, dict):
            quantidade_itens += converter_inteiro(produto.get("quantity")) or 0

    return {
        "id_api": converter_inteiro(registro.get("id")),
        "usuario_id": converter_inteiro(registro.get("userId")),
        "data_compra": registro.get("date"),
        "quantidade_itens": quantidade_itens,
        "dados_originais": registro,
    }


def normalizar_registros(tipo_dado: str, registros: list[Registro]) -> list[Registro]:
    """Normaliza uma lista de registros conforme o tipo de dado informado."""

    normalizadores = {
        "usuarios": normalizar_usuario,
        "produtos": normalizar_produto,
        "carrinhos": normalizar_carrinho,
    }

    if tipo_dado not in normalizadores:
        raise ValueError(f"Tipo de dado não suportado: {tipo_dado}")

    return [normalizadores[tipo_dado](registro) for registro in registros]
