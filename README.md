# Airflow Marketplace Pipeline

Pipeline distribuído com Apache Airflow para ingestão, normalização e persistência de dados de marketplace a partir da FakeStore API.

O projeto utiliza Docker Compose, PostgreSQL, Redis, Celery Executor, Flower e múltiplos workers para demonstrar uma arquitetura de orquestração distribuída.

## Objetivo

Implementar um pipeline de dados orquestrado pelo Apache Airflow para coletar dados das entidades:

- Users
- Products
- Carts

Os dados são consumidos da FakeStore API, normalizados em tarefas separadas e persistidos em tabelas relacionais no PostgreSQL.

## Arquitetura da solução

A solução é composta pelos seguintes serviços:

- `airflow-webserver`: interface web do Apache Airflow.
- `airflow-scheduler`: componente responsável pelo agendamento e disparo das DAGs.
- `airflow-worker`: workers Celery responsáveis pela execução distribuída das tarefas.
- `airflow-triggerer`: componente auxiliar para execução assíncrona.
- `airflow-init`: serviço de inicialização do ambiente.
- `postgres`: banco de dados utilizado pelo Airflow e pelas tabelas do pipeline.
- `redis`: broker utilizado pelo Celery Executor.
- `flower`: interface de monitoramento dos workers Celery.

## Estrutura do projeto

```text
airflow-marketplace-pipeline/
├── dags/
│   └── dag_marketplace_fakestore.py
├── include/
│   ├── __init__.py
│   ├── banco_dados.py
│   ├── cliente_fakestore.py
│   └── normalizacao.py
├── sql/
│   └── criar_tabelas.sql
├── relatorio/
│   └── evidencias/
│       └── evidencias_airflow_marketplace.pdf
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Fluxo da DAG

A DAG `dag_marketplace_fakestore` executa o seguinte fluxo:

1. criação das tabelas no PostgreSQL;
2. coleta dos dados de usuários;
3. coleta dos dados de produtos;
4. coleta dos dados de carrinhos;
5. normalização dos dados coletados;
6. persistência dos dados normalizados no PostgreSQL.

As tarefas de coleta são executadas em paralelo e depois consolidadas para a etapa de normalização e carga.

## Execução do ambiente

Para subir o ambiente:

```bash
docker compose up -d --build
```

Para verificar os containers:

```bash
docker compose ps
```

Acessos principais:

```text
Airflow UI: http://localhost:8080
Flower:     http://localhost:5555
```

## Documentação e evidências

O PDF com evidências da execução é o documento final de comprovação da entrega e está disponível em:

```text
relatorio/evidencias/evidencias_airflow_marketplace.pdf
```

As evidências contemplam a execução da DAG, a distribuição das tarefas, o acompanhamento pelo Flower e a persistência dos dados no PostgreSQL.

## Tecnologias utilizadas

- Apache Airflow
- Celery Executor
- Docker Compose
- PostgreSQL
- Redis
- Flower
- Python
- FakeStore API

## Resultado esperado

Ao executar a DAG com sucesso, o pipeline deve:

- consumir dados da FakeStore API;
- executar tarefas paralelas no Airflow;
- distribuir processamento entre workers Celery;
- registrar os dados normalizados no PostgreSQL;
- permitir acompanhamento da execução pelo Airflow UI e pelo Flower.


## Validação da entrega

A validação da entrega deve ser feita a partir do PDF de evidências, que registra:

- execução bem-sucedida da DAG `dag_marketplace_fakestore`;
- coleta das entidades `Users`, `Products` e `Carts`;
- execução paralela das tarefas no Airflow;
- uso do Celery Executor com workers distribuídos;
- acompanhamento dos workers pelo Flower;
- persistência dos dados normalizados no PostgreSQL.

O arquivo de evidências está disponível em:

```text
relatorio/evidencias/evidencias_airflow_marketplace.pdf
