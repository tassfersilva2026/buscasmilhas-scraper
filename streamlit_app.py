from __future__ import annotations
from pathlib import Path
from io import BytesIO
from typing import List, Optional
import os
import re
import numpy as np
import pandas as pd
import streamlit as st
import altair as alt

# ==========================
# Configuração base do app
# ==========================
st.set_page_config(page_title="Painel de Concorrência — data/", layout="wide")

# Tema simples (tons de cinza + amarelo)
AMARELO = "#F2C94C"  # amarelo
CINZA_TXT = "#333333"
CINZA_BG = "#F5F5F5"

st.markdown(
    f"""
    <style>
      .block-container {{ padding-top: 0.8rem; }}
      h1, h2, h3, h4, h5, h6 {{ color: {CINZA_TXT}; }}
      [data-testid="stMetricDelta"] {{ color: {CINZA_TXT} !important; }}
      .kpi .stMetric {{ background:{CINZA_BG}; border-radius:1rem; padding:0.8rem; }}
      .viewbox {{ border:1px solid #e5e5e5; border-radius:1rem; padding:1rem; margin-bottom:1rem; }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ==========================
# Localização da pasta data/
# ==========================

def find_data_dir(start: Path) -> str:
    cur: Optional[Path] = start
    for _ in range(8):  # sobe alguns níveis até achar data/
        data_here = cur / "data"
        if data_here.exists() and data_here.is_dir():
            return data_here.as_posix()
        if cur.parent == cur:
            break
        cur = cur.parent
    return (start.parent / "data").as_posix()

DATA_DIR_DEFAULT = find_data_dir(Path(__file__).resolve())

# ==========================
# Funções utilitárias
# ==========================

@st.cache_data(show_spinner=False)
def _list_files(data_dir: str, patterns: List[str] | None = None) -> List[Path]:
    p = Path(data_dir)
    if not p.exists():
        return []
    pats = patterns or ["*.xlsx", "*.xls", "*.csv", "*.parquet"]
    files: List[Path] = []
    for pat in pats:
        files.extend(sorted(p.glob(pat)))
    return files


def _to_float_series(s: pd.Series) -> pd.Series:
    if s.dtype.kind in ("i", "u", "f"):
        return s.astype(float)
    txt = (
        s.astype(str)
        .str.replace(r"[^0-9,.-]", "", regex=True)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(txt, errors="coerce")


def detect_empresa_from_filename(name: str) -> str:
    u = name.upper()
    if "FLIP" in u or "FLIPMILHAS" in u: return "FLIPMILHAS"
    if "CAPO" in u: return "CAPO VIAGENS"
    if "MAX" in u and "MILHAS" in u: return "MAXMILHAS"
    if "123" in u and "MILHAS" in u: return "123MILHAS"
    return "N/A"


@st.cache_data(show_spinner=False)
def _read_one(path: str, mtime: float) -> pd.DataFrame:
    p = Path(path)
    ext = p.suffix.lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(p)
    elif ext == ".csv":
        for sep in [";", ","]:
            try:
                df = pd.read_csv(p, sep=sep)
                break
            except Exception:
                continue
        else:
            df = pd.read_csv(p)
    elif ext == ".parquet":
        df = pd.read_parquet(p)
    else:
        return pd.DataFrame()

    # Padroniza nomes
    colmap = {c: re.sub(r"\s+", " ", str(c)).strip().upper() for c in df.columns}
    df = df.rename(columns=colmap)

    # Sinônimos
    ren = {
        "CIA": "CIA DO VOO",
        "CIA DO VÔO": "CIA DO VOO",
        "TX EMBARQUE": "TX DE EMBARQUE",
        "TAXA DE EMBARQUE": "TX DE EMBARQUE",
        "VALOR TOTAL": "TOTAL",
        "VALOR": "TOTAL",
    }
    for k, v in ren.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    # Garante colunas
    required = [
        "DATA DA BUSCA", "HORA DA BUSCA", "TRECHO",
        "DATA PARTIDA", "HORA DA PARTIDA", "DATA CHEGADA", "HORA DA CHEGADA",
        "TARIFA", "TX DE EMBARQUE", "TOTAL", "CIA DO VOO",
    ]
    for c in required:
        if c not in df.columns:
            df[c] = np.nan

    # Tipos numéricos
    for c in ["TARIFA", "TX DE EMBARQUE", "TOTAL"]:
        df[c] = _to_float_series(df[c])

    # Datetimes
    def combo_dt(dcol: str, tcol: str) -> pd.Series:
        d = pd.to_datetime(df[dcol].astype(str).str.strip(), dayfirst=True, errors="coerce")
        t = pd.to_datetime(df[tcol].astype(str).str.strip(), errors="coerce")
        raw = df[dcol].astype(str).str.strip() + " " + df[tcol].astype(str).str.strip()
        dt = pd.to_datetime(raw, dayfirst=True, errors="coerce").fillna(d).fillna(t)
        return dt

    df["BUSCA_DATETIME"]   = combo_dt("DATA DA BUSCA", "HORA DA BUSCA")
    df["PARTIDA_DATETIME"] = combo_dt("DATA PARTIDA", "HORA DA PARTIDA")
    df["CHEGADA_DATETIME"] = combo_dt("DATA CHEGADA", "HORA DA CHEGADA")
    df["ADVP"] = (df["PARTIDA_DATETIME"].dt.normalize() - df["BUSCA_DATETIME"].dt.normalize()).dt.days
    df["HORA_HH"] = df["BUSCA_DATETIME"].dt.hour

    # Metadados
    df["ARQUIVO"] = p.name
    df["CAMINHO"] = str(p)
    df["EMPRESA"] = detect_empresa_from_filename(p.name)

    # Ordenação de colunas
    base = [
        "BUSCA_DATETIME", "DATA DA BUSCA", "HORA DA BUSCA", "HORA_HH",
        "TRECHO", "CIA DO VOO", "ADVP",
        "PARTIDA_DATETIME", "DATA PARTIDA", "HORA DA PARTIDA",
        "CHEGADA_DATETIME", "DATA CHEGADA", "HORA DA CHEGADA",
        "TARIFA", "TX DE EMBARQUE", "TOTAL",
        "EMPRESA", "ARQUIVO", "CAMINHO",
    ]
    other = [c for c in df.columns if c not in base]
    return df[base + other]


@st.cache_data(show_spinner=True)
def load_all(data_dir: str) -> pd.DataFrame:
    files = _list_files(data_dir)
    if not files:
        return pd.DataFrame()
    parts = []
    for f in files:
        try:
            parts.append(_read_one(str(f), f.stat().st_mtime))
        except Exception as e:
            st.warning(f"Falha ao ler {f.name}: {e}")
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    df = df.sort_values("BUSCA_DATETIME", ascending=False, kind="stable")
    return df


def fmt_moeda(v) -> str:
    try:
        if pd.isna(v):
            return "-"
        return f"R$ {float(v):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    except Exception:
        return "-"

# ==========================
# Carregamento de dados
# ==========================

st.title("📊 Painel de Concorrência — Flip/Capo/Max/123")
st.caption("Sem menu lateral. Filtros horizontais aplicados a todas as abas.")

with st.spinner("Lendo planilhas da pasta data/…"):
    df_all = load_all(DATA_DIR_DEFAULT)

if df_all.empty:
    st.warning("Nenhum arquivo lido. Verifique a pasta `data/`." )
    st.stop()

# ==========================
# Filtros (horizontais, globais)
# ==========================

min_d = df_all["BUSCA_DATETIME"].dropna().min()
max_d = df_all["BUSCA_DATETIME"].dropna().max()

col1, col2, col3, col4, col5 = st.columns([1.2,1.2,1,2,1.2])
if pd.isna(min_d) or pd.isna(max_d):
    d_ini = d_fim = None
else:
    d_ini = col1.date_input("Data inicial", value=min_d.date(), min_value=min_d.date(), max_value=max_d.date(), format="DD/MM/YYYY")
    d_fim = col2.date_input("Data final",   value=max_d.date(), min_value=min_d.date(), max_value=max_d.date(), format="DD/MM/YYYY")

advp_opts = sorted([int(x) for x in df_all["ADVP"].dropna().unique()])
hora_opts = sorted([int(x) for x in df_all["HORA_HH"].dropna().unique()])
trecho_opts = sorted([str(x) for x in df_all["TRECHO"].dropna().unique() if str(x).strip()])

advp_sel  = col3.multiselect("ADVP", options=advp_opts, default=advp_opts)
trecho_sel= col4.multiselect("Trechos", options=trecho_opts, default=trecho_opts)
hora_sel  = col5.multiselect("Hora da busca", options=hora_opts, default=hora_opts)

# Aplica filtros globais
mask = pd.Series(True, index=df_all.index)
if d_ini and d_fim:
    d0, d1 = pd.to_datetime(d_ini), pd.to_datetime(d_fim)
    mask &= df_all["BUSCA_DATETIME"].dt.date.between(d0.date(), d1.date())
if advp_sel:
    mask &= df_all["ADVP"].isin(advp_sel)
if trecho_sel:
    mask &= df_all["TRECHO"].isin(trecho_sel)
if hora_sel:
    mask &= df_all["HORA_HH"].isin(hora_sel)

view_all = df_all.loc[mask].copy()

st.caption(
    f"Linhas após filtros: **{len(view_all):,}** | Última atualização: **{df_all['BUSCA_DATETIME'].max():%d/%m/%Y - %H:%M:%S}**".replace(",", ".")
)

# ==========================
# Funções de agregação/plots
# ==========================

def chart_line(df: pd.DataFrame, x: str, y: str, title: str):
    ch = (
        alt.Chart(df)
        .mark_line(point=True, color=AMARELO)
        .encode(x=x, y=y, tooltip=list(df.columns))
        .properties(height=280, title=title)
    )
    st.altair_chart(ch, use_container_width=True)


def chart_bar(df: pd.DataFrame, x: str, y: str, title: str):
    ch = (
        alt.Chart(df)
        .mark_bar(color=AMARELO)
        .encode(x=x, y=y, tooltip=list(df.columns))
        .properties(height=320, title=title)
    )
    st.altair_chart(ch, use_container_width=True)


def chart_heatmap(df: pd.DataFrame, x: str, y: str, z: str, title: str):
    ch = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            x=x,
            y=y,
            color=alt.Color(z, scale=alt.Scale(range=["#EEEEEE", AMARELO])),
            tooltip=list(df.columns),
        )
        .properties(height=320, title=title)
    )
    st.altair_chart(ch, use_container_width=True)


def chart_box(df: pd.DataFrame, x: str, y: str, title: str):
    ch = (
        alt.Chart(df)
        .mark_boxplot(color=AMARELO)
        .encode(x=x, y=y)
        .properties(height=320, title=title)
    )
    st.altair_chart(ch, use_container_width=True)


# ==========================
# Abas
# ==========================

abas = st.tabs(["FLIPMILHAS", "CAPO VIAGENS", "MAXMILHAS", "123MILHAS"])

# --------------------------
# ABA: FLIPMILHAS (10 visões)
# --------------------------
with abas[0]:
    df = view_all[view_all["EMPRESA"] == "FLIPMILHAS"].copy()
    st.header("FLIPMILHAS")
    if df.empty:
        st.info("Sem dados para os filtros atuais.")
    else:
        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        with k1: st.metric("Registros", f"{len(df):,}".replace(",", "."))
        with k2: st.metric("Preço mediano (TOTAL)", fmt_moeda(df["TOTAL"].median()))
        with k3: st.metric("Tarifa mediana", fmt_moeda(df["TARIFA"].median()))
        with k4: st.metric("Tx embarque mediana", fmt_moeda(df["TX DE EMBARQUE"].median()))

        # 1) Evolução diária (mediana TOTAL)
        daily = (
            df.assign(DIA=df["BUSCA_DATETIME"].dt.date)
              .groupby("DIA", as_index=False)["TOTAL"].median()
              .rename(columns={"TOTAL":"TOTAL_MED"})
        )
        chart_line(daily, "DIA:T", "TOTAL_MED:Q", "Evolução diária do preço mediano (TOTAL)")

        # 2) Mediana por ADVP
        by_advp = df.groupby("ADVP", as_index=False)["TOTAL"].median().rename(columns={"TOTAL":"TOTAL_MED"})
        chart_bar(by_advp, "ADVP:O", "TOTAL_MED:Q", "Preço mediano por ADVP")

        # 3) Heatmap ADVP x Hora (mediana TOTAL)
        heat = (
            df.groupby(["ADVP","HORA_HH"], as_index=False)["TOTAL"].median()
              .rename(columns={"TOTAL":"TOTAL_MED"})
        )
        chart_heatmap(heat, "HORA_HH:O", "ADVP:O", "TOTAL_MED:Q", "Mapa de calor: Hora x ADVP (mediana TOTAL)")

        # 4) Top 15 trechos pelo preço mediano
        by_trecho = (
            df.groupby("TRECHO", as_index=False)["TOTAL"].median()
              .rename(columns={"TOTAL":"TOTAL_MED"})
              .sort_values("TOTAL_MED", ascending=False)
              .head(15)
        )
        chart_bar(by_trecho, "TRECHO:N", "TOTAL_MED:Q", "Top 15 trechos por preço mediano (TOTAL)")

        # 5) Histograma de preços (TOTAL)
        bins = pd.cut(df["TOTAL"], bins=20)
        hist = df.groupby(bins, as_index=False)["TOTAL"].count().rename(columns={"TOTAL":"QTDE"})
        hist["FAIXA"] = hist["TOTAL"].astype(str)
        chart_bar(hist, "FAIXA:N", "QTDE:Q", "Distribuição de preços (TOTAL)")

        # 6) Scatter Tarifa x Taxa (amostra)
        samp = df.sample(min(5000, len(df)), random_state=42) if len(df) > 5000 else df
        ch = (
            alt.Chart(samp)
            .mark_circle(color=AMARELO, opacity=0.5)
            .encode(x="TARIFA:Q", y="TX DE EMBARQUE:Q", tooltip=["TOTAL","TARIFA","TX DE EMBARQUE","TRECHO","ADVP","HORA_HH","BUSCA_DATETIME"])
            .properties(height=320, title="Tarifa vs Taxa de embarque (amostra)")
        )
        st.altair_chart(ch, use_container_width=True)

        # 7) Tabela: últimas 50 linhas
        st.subheader("Últimas 50 buscas")
        st.dataframe(df.sort_values("BUSCA_DATETIME", ascending=False).head(50), use_container_width=True, hide_index=True)

        # 8) Tabela: menor preço por Trecho x ADVP
        min_tbl = (
            df.groupby(["TRECHO","ADVP"], as_index=False)
              .agg(
                  TOTAL_MIN=("TOTAL","min"),
                  TARIFA_MIN=("TARIFA","min"),
                  EXEMPLO_DATA=("BUSCA_DATETIME","max")
              )
              .sort_values(["TOTAL_MIN"], ascending=True)
        )
        st.subheader("Menor preço por Trecho x ADVP")
        st.dataframe(min_tbl, use_container_width=True, hide_index=True)

        # 9) Maiores variações d/d por Trecho (mediana TOTAL)
        dd = (
            df.assign(DIA=df["BUSCA_DATETIME"].dt.date)
              .groupby(["TRECHO","DIA"], as_index=False)["TOTAL"].median()
              .rename(columns={"TOTAL":"TOTAL_MED"})
        )
        dd["VAR_%"] = dd.groupby("TRECHO")["TOTAL_MED"].pct_change()*100
        var_tbl = dd.dropna(subset=["VAR_%"]).sort_values("VAR_%", ascending=False).head(20)
        st.subheader("Maiores altas d/d por trecho (mediana TOTAL)")
        st.dataframe(var_tbl, use_container_width=True, hide_index=True)

        # 10) Boxplot TOTAL por Trecho (top 10 por contagem)
        top10 = (
            df["TRECHO"].value_counts().head(10).index.tolist()
        )
        box_df = df[df["TRECHO"].isin(top10)]
        chart_box(box_df, "TRECHO:N", "TOTAL:Q", "Boxplot de TOTAL por Trecho (top 10)")

# --------------------------
# ABA: CAPO VIAGENS (placeholder)
# --------------------------
with abas[1]:
    st.header("CAPO VIAGENS")
    df = view_all[view_all["EMPRESA"] == "CAPO VIAGENS"].copy()
    if df.empty:
        st.info("Sem dados para os filtros atuais.")
    else:
        st.write("(Reservei a estrutura; depois plugamos suas visões específicas.)")
        st.dataframe(df.head(200), use_container_width=True, hide_index=True)

# --------------------------
# ABA: MAXMILHAS (placeholder)
# --------------------------
with abas[2]:
    st.header("MAXMILHAS")
    df = view_all[view_all["EMPRESA"] == "MAXMILHAS"].copy()
    if df.empty:
        st.info("Sem dados para os filtros atuais.")
    else:
        st.write("(Reservei a estrutura; depois plugamos suas visões específicas.)")
        st.dataframe(df.head(200), use_container_width=True, hide_index=True)

# --------------------------
# ABA: 123MILHAS (placeholder)
# --------------------------
with abas[3]:
    st.header("123MILHAS")
    df = view_all[view_all["EMPRESA"] == "123MILHAS"].copy()
    if df.empty:
        st.info("Sem dados para os filtros atuais.")
    else:
        st.write("(Reservei a estrutura; depois plugamos suas visões específicas.)")
        st.dataframe(df.head(200), use_container_width=True, hide_index=True)
