# Case de Engenharia de Dados – Pipeline Analítico COVID-19 (Airflow + Snowflake + Streamlit)

## Visão Geral
Este projeto apresenta um pipeline **ELT (Extract, Load, Transform)** moderno e completo, que integra dados da COVID-19 de múltiplas fontes, transforma e armazena no **Snowflake**, e disponibiliza visualizações interativas por meio de um **dashboard Streamlit**.  
O pipeline foi inteiramente orquestrado no **Apache Airflow**, com o objetivo de demonstrar domínio sobre ingestão, modelagem, governança de dados e storytelling analítico.

---

## Arquitetura Geral

| Camada | Função | Exemplos de Tabelas/Views |
|--------|--------|----------------------------|
| **Bronze** | Dados brutos carregados do ambiente local e da API REST Countries. | `CASES_DATASET`, `TESTS_DATASET`, `HOSPITAL_DATASET`, `VACCINATION_DATASET`, `RESTCOUNTRIES_RAW` |
| **Silver** | Dados tratados, tipados e unificados, prontos para modelagem. | `DIM_COUNTRY`, `DIM_RESTCOUNTRIES`, `CASES_DATASET`, `HOSPITAL_DATASET`, `TESTS_DATASET`, `VACCINATION_DATASET` |
| **Gold** | Camada analítica e de consumo, com métricas e visões consolidadas. | `FACT_COVID_DAILY`, `V_COUNTRY_LATEST`, `V_TREND_COUNTRY`, `V_DIM_COUNTRY_ENRICHED` |

O fluxo completo pode ser descrito como:
**Arquivos Locais → Bronze → Silver → Gold → Streamlit Dashboard**

---

## Tecnologias Utilizadas
- **Apache Airflow** – orquestração de DAGs e agendamento das etapas ELT.  
- **Snowflake** – data warehouse para armazenamento e transformação SQL.  
- **Python (Pandas, Requests)** – leitura de dados, integração com APIs e geração de Parquet.  
- **Parquet** – formato de dados intermediário otimizado para ingestão.  
- **Streamlit** – camada de apresentação e exploração analítica hospedada dentro do Snowflake.  

---

## Etapas do Pipeline

### 1. Ingestão de Dados Locais
- Leitura de 5 arquivos originais (4 `.xlsx` e 1 `.csv`).
- Conversão para **Parquet** com schema consistente.
- Upload automático para a camada **Bronze** do Snowflake.
- **DAGs**:  
  - `local_to_parquet`  
  - `parquet_to_snowflake`

### 2. Integração com API Pública (REST Countries)
- Endpoint: `https://restcountries.com/v3.1/all`
- Enriquecimento com metadados geográficos e demográficos (região, capital, área, população etc.).
- Armazenamento em `BRONZE.RESTCOUNTRIES_RAW` → transformação em `SILVER.DIM_RESTCOUNTRIES`.
- **DAG:** `api_ingest_to_bronze`

### 3. Transformação (Bronze → Silver)
- **Limpeza e tipagem dos datasets COVID-19.**
- **Remoção de agregações** (`OWID_%`) e normalização de colunas.
- **Criação de dimensões:**  
  - `DIM_COUNTRY`  
  - `DIM_RESTCOUNTRIES`
- **DAG:** `bronze_to_silver`

### 4. Modelagem Analítica (Silver → Gold)
- Agregação e enriquecimento cruzando COVID + REST Countries.
- Construção de:
  - `FACT_COVID_DAILY` – fatos diários de casos, óbitos, testes e vacinação.
  - `V_COUNTRY_LATEST` – snapshot mais recente de cada país.
  - `V_TREND_COUNTRY` – séries temporais.
  - `V_DIM_COUNTRY_ENRICHED` – metadados combinados.
- **DAG:** `silver_to_gold`

---

## Insights Analíticos (Dashboard Streamlit)

O dashboard foi desenvolvido e hospedado dentro do Snowflake, utilizando o **Streamlit Native App**.  
As análises exploram a camada **Gold** e destacam padrões regionais e epidemiológicos.

### Principais Seções:

#### **5.1 Panorama das Américas**
Média de casos, óbitos e vacinação — visão geral do continente, consolidada por país e sub-região.

#### **5.2 Tendência Temporal**
Comparação entre **Brasil, Chile e EUA**.  
Mostra a evolução de novos casos ao longo do tempo, com suavização móvel.

#### **5.3 Vacinação × Infecção nas Américas**
Dispersão interativa relacionando **% da população vacinada** e **% infectada**.  

#### **5.4 Evolução da Vacinação em Cuba**
Análise de destaque positivo: Cuba como exemplo de cobertura vacinal.  
Gráfico mostra a **proporção da população totalmente vacinada ao longo do tempo.**

#### **5.5 Comparativo por Sub-região**
Comparação entre América do Norte, Central, do Sul e Caribe, com estatísticas médias de vacinação e infecção.

---

## Resultados Principais
- Pipeline automatizado, reprodutível e escalável.  
- Camadas organizadas seguindo o padrão **medallion (Bronze, Silver, Gold)**.  
- Enriquecimento com fonte externa (REST Countries).  
- Visualização moderna e interativa diretamente no Snowflake.  
- Insights contextualizados e consistentes com a realidade dos dados das Américas.

---

## Boas Práticas e Design
- Código modular e reutilizável em Python e SQL.  
- Orquestração e logging via Airflow.  
- Uso de `.env` para variáveis sensíveis e seguras.  
- Transformações realizadas **dentro do Snowflake (ELT)**, explorando performance do warehouse.  
- Criação de views analíticas com nomenclatura padronizada (`V_` prefix).  

---

## 👨‍💻 Autor
**Reinaldo Neto**  
Projeto: Case de Engenharia de Dados – COVID Analytics  
Salvador, Bahia – 2025  