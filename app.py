# app.py — Streamlit in Snowflake (GOLD analytics)
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# ---------------------------
# Configuração de página
# ---------------------------
st.set_page_config(
    page_title="COVID Analytics – GOLD",
    layout="wide",
)

# ---------------------------
# Sessão Snowpark (injetada)
# ---------------------------
session = get_active_session()

DB = "COVID_DB"
SCH = "GOLD"
FACT = f"{DB}.{SCH}.FACT_COVID_DAILY"
V_LATEST = f"{DB}.{SCH}.V_COUNTRY_LATEST"
V_TREND = f"{DB}.{SCH}.V_TREND_COUNTRY"
V_ENRICH = f"{DB}.{SCH}.V_DIM_COUNTRY_ENRICHED"

# ---------------------------
# Helpers de consulta (com cache)
# ---------------------------
@st.cache_data(ttl=600)
def q(sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas()

@st.cache_data(ttl=600)
def get_max_date() -> pd.Timestamp:
    df = q(f"SELECT MAX(DATE) AS MAX_DT FROM {FACT}")
    return pd.to_datetime(df["MAX_DT"].iloc[0])

@st.cache_data(ttl=600)
def get_countries() -> list[str]:
    df = q(f"SELECT DISTINCT ISO_CODE FROM {V_TREND} ORDER BY ISO_CODE")
    return df["ISO_CODE"].tolist()

@st.cache_data(ttl=600)
def get_regions() -> list[str]:
    # Região/sub-região via view enriquecida (API REST Countries)
    df = q(f"SELECT DISTINCT REGION FROM {V_ENRICH} WHERE REGION IS NOT NULL ORDER BY REGION")
    return df["REGION"].tolist()

# ---------------------------
# Header
# ---------------------------
st.title("COVID Analytics – Camada GOLD (Snowflake)")
st.caption("Dados: OWID (processados) + REST Countries (enriquecimento).")

# ---------------------------
# Filtros globais
# ---------------------------
max_dt = get_max_date()
col_f1, col_f2, col_f3 = st.columns(3)
with col_f1:
    st.write("**Janela:** série completa")
with col_f2:
    region_filter = st.selectbox("Filtrar por Região (opcional)", ["(Todas)"] + get_regions(), index=0)
with col_f3:
    st.write(f"**Última data disponível:** {max_dt.date()}")

# WHERE clause condicional por região (para queries que usam enriched)
where_region = ""
if region_filter != "(Todas)":
    safe_region = region_filter.replace("'", "''")
    where_region = f"AND ISO_CODE IN (SELECT ISO_CODE FROM {V_ENRICH} WHERE REGION = '{safe_region}')"
else:
    where_region = ""
# ---------------------------
# Abas
# ---------------------------
tab_overview, tab_rank, tab_trend, tab_profile, tab_insights = st.tabs(
    ["Visão Geral", "Rankings 7d", "Tendências", "Perfil do País", "Insights"]
)

# ===========================
# 1) Visão Geral
# ===========================
with tab_overview:
    st.subheader("KPIs (último dia disponível)")
    kpi_sql = f"""
    WITH days AS (
      SELECT
        DATE,
        SUM(COALESCE(NEW_CASES,0))        AS daily_cases,
        SUM(COALESCE(NEW_DEATHS,0))       AS daily_deaths,
        SUM(COALESCE(NEW_VACCINATIONS,0)) AS daily_vax
      FROM {FACT}
      WHERE 1=1 {where_region}
      GROUP BY DATE
    ), agg AS (
      SELECT
        AVG(daily_cases)  AS NEW_CASES,
        AVG(daily_deaths) AS NEW_DEATHS,
        AVG(daily_vax)    AS NEW_VACCINATIONS,
        SUM(daily_cases)  AS sum_cases,
        SUM(daily_deaths) AS sum_deaths
      FROM days
    )
    SELECT
      NEW_CASES,
      NEW_DEATHS,
      NEW_VACCINATIONS,
      ROUND(sum_deaths*100.0/NULLIF(sum_cases,0), 2) AS DEATH_RATE_PCT
    FROM agg
    """
    kpis = q(kpi_sql)
    c1, c2, c3, c4 = st.columns(4)
    # Helpers de formatação robustos para KPIs
    def fmt_int(x):
        """Formata inteiro com milhares, tratando None/NaN/float/Decimal."""
        try:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return "0"
            v = int(float(x))
        except Exception:
            v = 0
        return f"{v:,}".replace(",", ".")

    def fmt_pct(x):
        """Formata percentual com 2 casas, tratando None/NaN."""
        try:
            v = float(0 if x is None or (isinstance(x, float) and pd.isna(x)) else x)
        except Exception:
            v = 0.0
        return f"{v:.2f}%".replace(".", ",")

    # Extrai a primeira linha (ou valores padrão se vazio)
    if len(kpis) > 0:
        row = kpis.iloc[0]
    else:
        row = pd.Series({
            "NEW_CASES": 0,
            "NEW_DEATHS": 0,
            "NEW_VACCINATIONS": 0,
            "DEATH_RATE_PCT": 0.0,
        })

    c1.metric("Novos casos (média diária)",       fmt_int(row.get("NEW_CASES", 0)))
    c2.metric("Novos óbitos (média diária)",      fmt_int(row.get("NEW_DEATHS", 0)))
    c3.metric("Novas doses (média diária)",       fmt_int(row.get("NEW_VACCINATIONS", 0)))
    c4.metric("Letalidade (%)",          fmt_pct(row.get("DEATH_RATE_PCT", 0)))

    st.caption("KPIs calculados como média diária ao longo de **toda a série histórica** (após filtros aplicados).")

    st.markdown("---")
    st.subheader("Snapshot mais recente por país")
    latest_sql = f"""
    SELECT COUNTRY, ISO_CODE,
           TOTAL_CASES, TOTAL_DEATHS,
           ROUND(TOTAL_DEATHS*100.0/NULLIF(TOTAL_CASES,0),2) AS MORTALITY_RATE_PCT,
           VACCINATED_COVERAGE_PCT, FULLY_VACCINATED_COVERAGE_PCT, BOOSTER_COVERAGE_PCT
    FROM {V_LATEST}
    WHERE 1=1 {where_region}
    ORDER BY TOTAL_CASES DESC
    LIMIT 50
    """
    st.dataframe(q(latest_sql), use_container_width=True)

# ===========================
# 2) Rankings (7 dias)
# ===========================
with tab_rank:
    st.subheader("Top 10 países – novos casos (série completa)")
    rank_cases_sql = f"""
    SELECT COUNTRY, ISO_CODE, SUM(COALESCE(NEW_CASES,0)) AS CASES_TOTAL
    FROM {FACT}
    WHERE 1=1 {where_region}
    GROUP BY 1,2
    ORDER BY CASES_TOTAL DESC
    LIMIT 10
    """
    st.dataframe(q(rank_cases_sql), use_container_width=True)
    st.caption("Ranking considerando a série histórica completa (com filtros aplicados).")

    col_r1, col_r2 = st.columns(2)
    with col_r1:
        st.markdown("**Top 10 – Mortalidade por 100k (série completa)**")
        rank_mort_sql = f"""
        SELECT
          COUNTRY, ISO_CODE,
          ROUND(SUM(COALESCE(NEW_DEATHS,0)) * 100000.0 / NULLIF(AVG(POPULATION),0), 2) AS DEATHS_PER_100K_TOTAL
        FROM {FACT}
        WHERE 1=1 {where_region}
        GROUP BY 1,2
        ORDER BY DEATHS_PER_100K_TOTAL DESC
        LIMIT 10
        """
        st.dataframe(q(rank_mort_sql), use_container_width=True)
        st.caption("Ranking considerando a série histórica completa (com filtros aplicados).")

    with col_r2:
        st.markdown("**Top 10 – Incidência por 100k (série completa)**")
        rank_incid_sql = f"""
        SELECT
          COUNTRY, ISO_CODE,
          ROUND(SUM(COALESCE(NEW_CASES,0)) * 100000.0 / NULLIF(AVG(POPULATION),0), 2) AS CASES_PER_100K_TOTAL
        FROM {FACT}
        WHERE 1=1 {where_region}
        GROUP BY 1,2
        ORDER BY CASES_PER_100K_TOTAL DESC
        LIMIT 10
        """
        st.dataframe(q(rank_incid_sql), use_container_width=True)
        st.caption("Ranking considerando a série histórica completa (com filtros aplicados).")

# ===========================
# 3) Tendências (série temporal)
# ===========================
with tab_trend:
    st.subheader("Tendências (média móvel 7d)")
    df_trend_countries = q(f"SELECT DISTINCT COUNTRY, ISO_CODE FROM {V_TREND} ORDER BY COUNTRY")
    country_names = df_trend_countries["COUNTRY"].tolist()
    sel_countries = st.multiselect("País(es)", country_names, default=country_names[:1])

    if sel_countries:
        safe_values = [df_trend_countries.loc[df_trend_countries["COUNTRY"] == c, "ISO_CODE"].iloc[0].replace("'", "''") for c in sel_countries]
        in_list = ",".join([f"'{v}'" for v in safe_values])
        trend_sql = f"""
        SELECT COUNTRY, DATE, NEW_CASES_7D, NEW_DEATHS_7D, NEW_VACCINATIONS_7D
        FROM {V_TREND}
        WHERE ISO_CODE IN ({in_list})
        ORDER BY DATE
        """
        trend = q(trend_sql)
        st.line_chart(trend.pivot_table(index="DATE", columns="COUNTRY", values="NEW_CASES_7D"))
        st.caption("Gráfico: NEW_CASES_7D por país (linhas). Use o filtro acima para comparar países.")

# ===========================
# 4) Perfil do País
# ===========================
with tab_profile:
    st.subheader("Perfil do País (snapshot mais recente)")
    df_countries = q(f"SELECT DISTINCT COUNTRY, ISO_CODE FROM {V_LATEST} ORDER BY COUNTRY")
    country_names = df_countries["COUNTRY"].tolist()
    country = st.selectbox("Selecione um país", country_names, index=0)
    iso_escaped = df_countries.loc[df_countries["COUNTRY"] == country, "ISO_CODE"].iloc[0].replace("'", "''")

    profile_sql = f"""
    SELECT
      l.COUNTRY, l.ISO_CODE,
      l.TOTAL_CASES, l.TOTAL_DEATHS,
      ROUND(l.TOTAL_DEATHS*100.0/NULLIF(l.TOTAL_CASES,0),2) AS MORTALITY_RATE_PCT,
      l.VACCINATED_COVERAGE_PCT, l.FULLY_VACCINATED_COVERAGE_PCT, l.BOOSTER_COVERAGE_PCT,
      e.COUNTRY_API_NAME, e.OFFICIAL_NAME, e.REGION, e.SUBREGION, e.CAPITAL, e.AREA_KM2, e.POPULATION_API
    FROM {V_LATEST} l
    LEFT JOIN {V_ENRICH} e USING (ISO_CODE)
    WHERE l.ISO_CODE = '{iso_escaped}'
    """
    st.dataframe(q(profile_sql), use_container_width=True)
    st.caption("Combina o snapshot de métricas (OWID/Silver→Gold) com metadados da REST Countries (API).")

# ===========================
# 5) Insights (curadoria analítica)
# ===========================
with tab_insights:
    st.subheader("Insights focados nas **AMERICAS**")

    # ---------------------------
    # Países das AMERICAS (sem filtro de sub-região)
    # ---------------------------
    # Primeiro, tenta pela view enriquecida (case-insensitive)
    amer_iso_df = q(f"""
        SELECT ISO_CODE
        FROM {V_ENRICH}
        WHERE COALESCE(REGION,'') ILIKE 'amer%'
    """)

    # Fallback: se não encontrar via enriched, tenta detectar pelas dimensões do FACT (continente)
    if amer_iso_df.empty:
        amer_iso_df = q(f"""
            SELECT DISTINCT ISO_CODE
            FROM {FACT}
            WHERE UPPER(COALESCE(CONTINENT,'')) IN (
                'NORTH AMERICA','SOUTH AMERICA','CENTRAL AMERICA','CARIBBEAN','LATIN AMERICA','AMERICAS'
            )
        """)

    amer_iso_list = amer_iso_df["ISO_CODE"].tolist()
    if not amer_iso_list:
        st.info("Nenhum país encontrado para as Américas nas fontes GOLD.")
        st.stop()
    safe_amer_iso = [x.replace("'", "''") for x in amer_iso_list]
    in_list = ",".join([f"'{x}'" for x in safe_amer_iso])

    # ---------------------------
    # 5.1 Panorama das AMERICAS (médias e letalidade)
    # ---------------------------
    st.markdown("### 5.1 Panorama das AMERICAS – Médias e Letalidade")
    kpi_amer_sql = f"""
    WITH days AS (
      SELECT DATE,
             SUM(COALESCE(NEW_CASES,0))        AS daily_cases,
             SUM(COALESCE(NEW_DEATHS,0))       AS daily_deaths,
             SUM(COALESCE(NEW_VACCINATIONS,0)) AS daily_vax
      FROM {FACT}
      WHERE ISO_CODE IN ({in_list})
      GROUP BY DATE
    ), agg AS (
      SELECT AVG(daily_cases)  AS NEW_CASES,
             AVG(daily_deaths) AS NEW_DEATHS,
             AVG(daily_vax)    AS NEW_VACCINATIONS,
             SUM(daily_cases)  AS sum_cases,
             SUM(daily_deaths) AS sum_deaths
      FROM days
    )
    SELECT NEW_CASES, NEW_DEATHS, NEW_VACCINATIONS,
           ROUND(sum_deaths*100.0/NULLIF(sum_cases,0), 2) AS DEATH_RATE_PCT
    FROM agg
    """
    kpi_amer = q(kpi_amer_sql)
    def fmt_int(x):
        try:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return "0"
            v = int(float(x))
        except Exception:
            v = 0
        return f"{v:,}".replace(",", ".")
    def fmt_pct(x):
        try:
            v = float(0 if x is None or (isinstance(x, float) and pd.isna(x)) else x)
        except Exception:
            v = 0.0
        return f"{v:.2f}%".replace(".", ",")
    row = kpi_amer.iloc[0] if len(kpi_amer) > 0 else pd.Series({
        "NEW_CASES":0, "NEW_DEATHS":0, "NEW_VACCINATIONS":0, "DEATH_RATE_PCT":0.0
    })
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Novos casos (média diária)", fmt_int(row.get("NEW_CASES", 0)))
    c2.metric("Novos óbitos (média diária)", fmt_int(row.get("NEW_DEATHS", 0)))
    c3.metric("Novas doses (média diária)", fmt_int(row.get("NEW_VACCINATIONS", 0)))
    c4.metric("Letalidade média (%)", fmt_pct(row.get("DEATH_RATE_PCT", 0)))
    st.caption("KPIs médios ao longo da série histórica para todos os países das **Américas**.")
    st.markdown("---")

    # ---------------------------
    # 5.2 Tendência de Novos Casos – Brasil, Chile e EUA (média móvel 7d)
    # ---------------------------
    st.markdown("### 5.2 Tendência de Novos Casos – Brasil, Chile e EUA (média móvel 7d)")
    fixed_countries = ["Brazil", "Chile", "United States"]
    df_trend_list = q(f"""
        SELECT DISTINCT COUNTRY, ISO_CODE
        FROM {V_TREND}
        WHERE ISO_CODE IN ({in_list})
    """)
    # Garantir correspondência case-insensitive
    df_trend_list["COUNTRY_UP"] = df_trend_list["COUNTRY"].str.upper()
    iso_codes = []
    for country in fixed_countries:
        match = df_trend_list[df_trend_list["COUNTRY_UP"] == country.upper()]
        if not match.empty:
            iso_codes.append(match["ISO_CODE"].iloc[0].replace("'", "''"))
    if iso_codes:
        in_countries = ",".join([f"'{v}'" for v in iso_codes])
        trend_sql = f"""
        SELECT COUNTRY, DATE, NEW_CASES_7D
        FROM {V_TREND}
        WHERE ISO_CODE IN ({in_countries})
        ORDER BY DATE
        """
        trend_df = q(trend_sql)
        st.line_chart(trend_df.pivot_table(index="DATE", columns="COUNTRY", values="NEW_CASES_7D"))
        st.caption("Média móvel de 7 dias de **novos casos** para Brasil, Chile e EUA (Américas).")
    else:
        st.info("Não foi possível localizar todos os países: Brasil, Chile, EUA.")
    st.markdown("---")

    # ---------------------------
    # 5.3 Vacinação × Infecção nas Américas
    # ---------------------------
    st.markdown("### 5.3 Vacinação × Infecção nas Américas")
    vax_inf_sql = f"""
    SELECT
      l.COUNTRY,
      ROUND(l.TOTAL_CASES * 100.0 / NULLIF(l.POPULATION,0), 2) AS INFECTION_RATE_PCT,
      l.FULLY_VACCINATED_COVERAGE_PCT,
      COALESCE(e.SUBREGION, 'Sem sub-região') AS SUBREGION,
      l.POPULATION
    FROM {V_LATEST} l
    LEFT JOIN {V_ENRICH} e USING (ISO_CODE)
    WHERE COALESCE(e.REGION,'') ILIKE 'amer%'
      AND l.FULLY_VACCINATED_COVERAGE_PCT IS NOT NULL
      AND l.TOTAL_CASES IS NOT NULL
      AND l.POPULATION IS NOT NULL
    """
    vax_inf_df = q(vax_inf_sql)

    chart_df = vax_inf_df.rename(columns={
        "FULLY_VACCINATED_COVERAGE_PCT": "Vacinação Completa (%)",
        "INFECTION_RATE_PCT": "Infecção (%)",
        "POPULATION": "População"
    })

    scatter_inf = (
        alt.Chart(chart_df)
        .mark_circle(opacity=0.8)
        .encode(
            x=alt.X("Vacinação Completa (%):Q", title="Vacinação completa (%)"),
            y=alt.Y("Infecção (%):Q", title="População infectada (%)"),
            color=alt.Color("SUBREGION:N", title="Sub-região"),
            size=alt.Size("População:Q", title="População total", scale=alt.Scale(range=[30,400])),
            tooltip=[
                alt.Tooltip("COUNTRY:N", title="País"),
                alt.Tooltip("SUBREGION:N", title="Sub-região"),
                alt.Tooltip("Vacinação Completa (%):Q"),
                alt.Tooltip("Infecção (%):Q"),
                alt.Tooltip("População:Q")
            ]
        )
        .interactive()
    )

    st.altair_chart(scatter_inf, use_container_width=True)
    st.caption("Correlação entre vacinação completa e taxa de infecção por país nas Américas. Passe o mouse para detalhes.")
    st.markdown("---")

    # ---------------------------
    # 5.4 Evolução da Vacinação – Destaque Cuba
    # ---------------------------
    st.markdown("### 5.4 Evolução da Vacinação – Destaque Cuba")
    cuba_iso_df = q(f"SELECT DISTINCT ISO_CODE FROM {V_TREND} WHERE UPPER(COUNTRY) = 'CUBA' AND ISO_CODE IN ({in_list})")
    if not cuba_iso_df.empty:
        cuba_iso = cuba_iso_df["ISO_CODE"].iloc[0].replace("'", "''")
        cuba_vax = q(f"""
            SELECT DATE,
                   LEAST(100, ROUND(PEOPLE_FULLY_VACCINATED * 100.0 / NULLIF(POPULATION, 0), 2)) AS FULLY_VAX_PCT
            FROM {FACT}
            WHERE ISO_CODE = '{cuba_iso}'
            ORDER BY DATE
        """)
        st.line_chart(cuba_vax.set_index("DATE")["FULLY_VAX_PCT"])
        st.caption("Cuba – evolução do **% da população totalmente vacinada** (limitado a 100%).")
    else:
        st.info("Cuba não encontrada na base atual.")
    st.markdown("---")

    # ---------------------------
    # 5.5 Comparativo por Sub-região nas Américas
    # ---------------------------
    st.markdown("### 5.5 Comparativo por Sub-região nas Américas")
    amer_sub_sql = f"""
    SELECT 
      COALESCE(e.SUBREGION, 'Sem sub-região') AS SUBREGION,
      ROUND(AVG(l.FULLY_VACCINATED_COVERAGE_PCT), 2) AS VAX_FULL_PCT_AVG,
      ROUND(SUM(COALESCE(f.NEW_CASES,0))*100000.0/NULLIF(AVG(f.POPULATION),0),2) AS CASES_PER_100K_TOTAL,
      ROUND(SUM(COALESCE(f.NEW_DEATHS,0))*100000.0/NULLIF(AVG(f.POPULATION),0),2) AS DEATHS_PER_100K_TOTAL,
      COUNT(DISTINCT f.ISO_CODE) AS N_COUNTRIES
    FROM {FACT} f
    JOIN {V_LATEST} l USING (ISO_CODE)
    LEFT JOIN {V_ENRICH} e USING (ISO_CODE)
    WHERE COALESCE(e.REGION,'') ILIKE 'amer%'
    GROUP BY COALESCE(e.SUBREGION, 'Sem sub-região')
    ORDER BY VAX_FULL_PCT_AVG DESC
    """
    st.dataframe(q(amer_sub_sql), use_container_width=True)
    st.caption("Comparativo entre sub-regiões das Américas: vacinação completa média (%), casos e mortes por 100 mil habitantes. Regiões ausentes aparecem como 'Sem sub-região'.")