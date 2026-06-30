"""DAG distribuída para captura de dados do marketplace via FakeStore API.

O fluxo utiliza TaskFlow API para orquestrar a captura paralela de usuários,
produtos e carrinhos. Os registros capturados são normalizados e persistidos em
PostgreSQL, mantendo a execução compatível com Airflow distribuído usando
Celery Executor.
"""
from __future__ import annotations

from typing import Any

import pendulum
from airflow.decorators import dag, task


from include.banco_dados import (
    executar_script_criacao_tabelas,
    inserir_registros,
)
from include.cliente_fakestore import (
    buscar_carrinhos,
    buscar_produtos,
    buscar_usuarios,
)
from include.normalizacao import normalizar_registros

Registro = dict[str, Any]
ResumoCarga = dict[str, int | str]

CAMINHO_SCRIPT_SQL = "/opt/airflow/sql/criar_tabelas.sql"
TIMEZONE_SAO_PAULO = pendulum.timezone("America/Sao_Paulo")


@dag(
    dag_id="marketplace_fakestore_distribuido",
    description=(
        "Pipeline distribuído para captura paralela de usuários, produtos "
        "e carrinhos da FakeStore API."
    ),
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE_SAO_PAULO),
    catchup=False,
    tags=["airflow", "celery", "marketplace", "fakestore"],
)
def pipeline_marketplace_fakestore() -> None:
    """Define o fluxo principal de captura e persistência do marketplace."""

    @task(task_id="preparar_banco_operacional")
    def preparar_banco_operacional() -> str:
        """Cria o schema e as tabelas operacionais do marketplace."""

        # A preparação do banco ocorre no início da DAG para garantir que as
        # cargas possam ser reexecutadas sem depender de intervenção manual.
        executar_script_criacao_tabelas(CAMINHO_SCRIPT_SQL)
        return "estrutura_banco_validada"

    @task(task_id="capturar_usuarios")
    def capturar_usuarios() -> list[Registro]:
        """Captura os usuários disponíveis na FakeStore API."""

        # A função retorna uma lista pequena para uso controlado de XCom
        # automático, conforme orientação da TaskFlow API.
        return buscar_usuarios()

    @task(task_id="capturar_produtos")
    def capturar_produtos() -> list[Registro]:
        """Captura os produtos disponíveis na FakeStore API."""

        return buscar_produtos()

    @task(task_id="capturar_carrinhos")
    def capturar_carrinhos() -> list[Registro]:
        """Captura os carrinhos disponíveis na FakeStore API."""

        return buscar_carrinhos()

    @task(task_id="persistir_usuarios")
    def persistir_usuarios(registros_brutos: list[Registro]) -> ResumoCarga:
        """Normaliza e persiste os usuários no banco operacional."""

        # A normalização fica separada da captura para manter o fluxo legível e
        # permitir evolução independente entre integração externa e persistência.
        registros_normalizados = normalizar_registros(
            tipo_dado="usuarios",
            registros=registros_brutos,
        )
        total_persistido = inserir_registros(
            tipo_dado="usuarios",
            registros=registros_normalizados,
        )

        return {
            "tipo_dado": "usuarios",
            "total_capturado": len(registros_brutos),
            "total_persistido": total_persistido,
        }

    @task(task_id="persistir_produtos")
    def persistir_produtos(registros_brutos: list[Registro]) -> ResumoCarga:
        """Normaliza e persiste os produtos no banco operacional."""

        registros_normalizados = normalizar_registros(
            tipo_dado="produtos",
            registros=registros_brutos,
        )
        total_persistido = inserir_registros(
            tipo_dado="produtos",
            registros=registros_normalizados,
        )

        return {
            "tipo_dado": "produtos",
            "total_capturado": len(registros_brutos),
            "total_persistido": total_persistido,
        }

    @task(task_id="persistir_carrinhos")
    def persistir_carrinhos(registros_brutos: list[Registro]) -> ResumoCarga:
        """Normaliza e persiste os carrinhos no banco operacional."""

        registros_normalizados = normalizar_registros(
            tipo_dado="carrinhos",
            registros=registros_brutos,
        )
        total_persistido = inserir_registros(
            tipo_dado="carrinhos",
            registros=registros_normalizados,
        )

        return {
            "tipo_dado": "carrinhos",
            "total_capturado": len(registros_brutos),
            "total_persistido": total_persistido,
        }

    @task(task_id="consolidar_resultado")
    def consolidar_resultado(
        resumo_usuarios: ResumoCarga,
        resumo_produtos: ResumoCarga,
        resumo_carrinhos: ResumoCarga,
    ) -> dict[str, ResumoCarga]:
        """Consolida os resultados finais das cargas executadas."""

        # O retorno consolidado mantém apenas dados pequenos no XCom, facilitando
        # auditoria da execução sem armazenar grandes volumes no metadado Airflow.
        return {
            "usuarios": resumo_usuarios,
            "produtos": resumo_produtos,
            "carrinhos": resumo_carrinhos,
        }

    banco_preparado = preparar_banco_operacional()

    usuarios_capturados = capturar_usuarios()
    produtos_capturados = capturar_produtos()
    carrinhos_capturados = capturar_carrinhos()

    # As três capturas ficam no mesmo nível de dependência após a preparação do
    # banco. Com Celery Executor, essas tasks podem ser distribuídas entre
    # workers diferentes durante a execução.
    banco_preparado >> [
        usuarios_capturados,
        produtos_capturados,
        carrinhos_capturados,
    ]

    resultado_usuarios = persistir_usuarios(usuarios_capturados)
    resultado_produtos = persistir_produtos(produtos_capturados)
    resultado_carrinhos = persistir_carrinhos(carrinhos_capturados)

    consolidar_resultado(
        resumo_usuarios=resultado_usuarios,
        resumo_produtos=resultado_produtos,
        resumo_carrinhos=resultado_carrinhos,
    )


pipeline_marketplace_fakestore()
