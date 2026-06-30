# Imagem base oficial do Apache Airflow.
# A versão é fixada para manter previsibilidade no ambiente acadêmico e evitar
# mudanças inesperadas de comportamento entre execuções futuras.
FROM apache/airflow:2.10.5

# Instala as dependências específicas do pipeline.
# As dependências ficam separadas no requirements.txt para facilitar manutenção.
COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
