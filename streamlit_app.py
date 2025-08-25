# streamlit_app.py
from __future__ import annotations
from pathlib import Path
from typing import List, Optional
import re
import numpy as np
import pandas as pd
import streamlit as st
import altair as alt

# ==========================
# Configuração base do app
# ==========================
st.set_page_config(page_title="Painel de Concorrência — Flip/Capo/Max/123", layout="wide")

# Paleta cinza + amarelo
AMARELO   = "#F2C94C"
CINZA_TXT = "#333333"
CINZA_BG  = "#F7F7F7"

st.markdown(
    f"""
    <style>
      .block-container {{ padding-top: 0.6rem; }}
      h1, h2, h3, h4, h5, h6 {{ color: {CINZA_TXT}; }}
      .kpi .stMetric {{ background:{CINZA_BG}; border-radius:12px; padding:10px; }}
      .smallcap {{ color:#666; font-size:0.9rem; margin-top:-8px; }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown("<h4>Painel de Concorrência — Flip/Capo/Max/123</h4>", unsafe_allow_html=True)

# ==========================
# Localização da pasta data/
# ==========================
def find_data_dir(start: Path) -> str:
    cur: Optional[Path] = start
    for _ in range(8):
        data_here = cur / "data"
        if data_here.exists() and data_here.is_dir():
            return data_here.as_posix()
        if cur.parent == cur:
            break
        cur = cur.parent
    return (start.parent / "data").as_posix()

DATA_DIR_DEFAULT = find_data_dir(Path(__file__).resolve())

# ==========================
# Leitura e normalização
# ==========================
@st.cache_data(show_spinner=False)
def _list_files(data_dir: str, patterns: List[str] | None = None) -> List[Path]:
    p = Path(data_dir)
    if not p.exists():
        return []
    pats = patterns or ["*.xlsx", "*.xls", "*.csv", "*.parquet"]
    out: List[Path] = []
    for pat in pats:
        out.extend(sorted(p.glob(pat)))
    return out

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

    # Normaliza colunas para UPPER
    colmap = {c: re.sub(r"\s+", " ", str(c)).strip().upper() for c in df.columns}
    df = df.rename(columns=colmap)

    # Renomeações usuais
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

    required = [
        "DATA DA BUSCA", "HORA DA BUSCA", "TRECHO",
        "DATA PARTIDA", "HORA DA PARTIDA", "DATA CHEGADA", "HORA DA CHEGADA",
        "TARIFA", "TX DE EMBARQUE", "TOTAL", "CIA DO VOO",
    ]
    for c in required:
        if c not in df.columns:
            df[c] = np.nan

    for c in ["TARIFA", "TX DE EMBARQUE", "TOTAL"]:
        df[c] = _to_float_series(df[c])

    def combo_dt(dcol: str, tcol: str) -> pd.Series:
        d = pd.to_datetime(df[dcol].astype(str).str.strip(), dayfirst=True, errors="coerce")
        t = pd.to_datetime(df[tcol].astype(str).str.strip(), errors="coerce")
        raw = df[dcol].astype(str).str.strip() + " " + df[tcol].astype(str).str.strip()
        dt = pd.to_datetime(raw, dayfirst=True, errors="coerce").fillna(d).fillna(t)
        return dt

    df["BUSCA_DATETIME"]   = combo_dt("DATA DA BUSCA", "HORA DA BUSCA")
    df["PARTIDA_DATETIME"] = combo_dt("DATA PARTIDA", "HORA DA PARTIDA")
    df["CHEGADA_DATETIME"] = combo_dt("DATA CHEGADA", "HORA DA CHEGADA")

    # Horas 0..23 (tratadas como locais América/São Paulo)
    df["HORA_HH"] = df["BUSCA_DATETIME"].dt.hour

    df["ADVP"] = (df["PARTIDA_DATETIME"].dt.normalize() - df["BUSCA_DATETIME"].dt.normalize()).dt.days

    df["ARQUIVO"] = p.name
    df["CAMINHO"] = str(p)
    df["EMPRESA"] = detect_empresa_from_filename(p.name)

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

def fmt_moeda0(v) -> str:
    try:
        if pd.isna(v):
            return "-"
        return "R$ " + f"{int(round(float(v))):,}".replace(",", ".")
    except Exception:
        return "-"

# ======================================================
# FILTROS NO TOPO
# ======================================================
with st.spinner("Lendo planilhas da pasta data/…"):
    df_all = load_all(DATA_DIR_DEFAULT)
if df_all.empty:
    st.info("Nenhum arquivo lido. Verifique a pasta `data/`.")
    st.stop()

min_d = df_all["BUSCA_DATETIME"].dropna().min()
max_d = df_all["BUSCA_DATETIME"].dropna().max()

c1, c2, c3, c4, c5 = st.columns([1.2, 1.2, 1.6, 3.4, 1.6])
d_ini = c1.date_input(
    "Data inicial",
    value=min_d.date() if pd.notna(min_d) else None,
    min_value=min_d.date() if pd.notna(min_d) else None,
    max_value=max_d.date() if pd.notna(max_d) else None,
    format="DD/MM/YYYY",
)
d_fim = c2.date_input(
    "Data final",
    value=max_d.date() if pd.notna(max_d) else None,
    min_value=min_d.date() if pd.notna(min_d) else None,
    max_value=max_d.date() if pd.notna(max_d) else None,
    format="DD/MM/YYYY",
)

advp_opts   = sorted([int(x) for x in df_all["ADVP"].dropna().unique()])
trecho_opts = sorted([str(x) for x in df_all["TRECHO"].dropna().unique() if str(x).strip()])
hora_opts   = sorted([int(x) for x in df_all["HORA_HH"].dropna().unique()])

advp_sel   = c3.multiselect("ADVP", options=advp_opts, default=[], placeholder="Todos")
trecho_sel = c4.multiselect("Trechos", options=trecho_opts, default=[], placeholder="Todos")
hora_sel   = c5.multiselect("Hora da busca", options=hora_opts, default=[], placeholder="Todas")

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
    f"Linhas após filtros: **{len(view_all):,}** • Última atualização: **{df_all['BUSCA_DATETIME'].max():%d/%m/%Y - %H:%M:%S}**".replace(",", ".")
)
st.markdown("---")

# ======================================================
# Helpers de gráfico (barras + linha de tendência)
# ======================================================
def x_axis(enc: str, title: Optional[str] = None):
    return alt.X(enc, axis=alt.Axis(labelAngle=0, labelOverlap=True, title=title, labelFontWeight="bold", labelColor=CINZA_TXT))

def y_axis(enc: str, title: Optional[str] = None):
    return alt.Y(enc, axis=alt.Axis(format=".0f", title=title, labelFontWeight="bold", labelColor=CINZA_TXT))

def barras_com_tendencia(df: pd.DataFrame, x_col: str, y_col: str, x_type: str, titulo: str, nota: str, sort=None):
    base = alt.Chart(df).encode(
        x=x_axis(f"{x_col}:{x_type}", title=None) if sort is None else x_axis(f"{x_col}:{x_type}", title=None).sort(sort),
        y=y_axis(f"{y_col}:Q", title=None),
        tooltip=[x_col, alt.Tooltip(y_col, format=".0f")],
    )
    bars = base.mark_bar(color=AMARELO)
    labels = (
        base.mark_text(
            baseline="middle",
            align="center",
            fontWeight="bold",
            dy=0,
            color=CINZA_TXT,
            size=12,
        ).encode(text=alt.Text(f"{y_col}:Q", format=".0f"))
    )
    # Linha de tendência (LOESS quando possível; se x não for numérico, apenas conecta pontos)
    if np.issubdtype(df[x_col].dtype, np.number):
        line = (
            alt.Chart(df)
            .transform_loess(x_col, y_col, bandwidth=0.5)
            .mark_line(color=CINZA_TXT, opacity=0.9)
            .encode(x=x_axis(f"{x_col}:{x_type}"), y=y_axis("loess:Q"))
        )
    else:
        line = base.mark_line(color=CINZA_TXT, opacity=0.9)

    ch = (bars + labels + line).properties(title=titulo, height=340)
    st.altair_chart(ch, use_container_width=True)
    st.markdown(f'<div class="smallcap">{nota}</div>', unsafe_allow_html=True)

def preco_medio(series: pd.Series) -> float:
    # média = soma TOTAL / quantidade de buscas
    total = pd.to_numeric(series, errors="coerce").sum()
    return float(total)

# ======================================================
# Renderização por empresa (mesma lógica p/ todas as abas)
# ======================================================
def render_empresa(df_emp: pd.DataFrame, nome: str):
    st.subheader(nome)

    if df_emp.empty:
        st.info("Sem dados para os filtros atuais.")
        return

    # KPIs
    k1, k2 = st.columns(2)
    with k1:
        st.metric("Buscas", f"{len(df_emp):,}".replace(",", "."))
    with k2:
        preco_medio_val = df_emp["TOTAL"].sum() / max(len(df_emp), 1)
        st.metric("Preço médio (TOTAL)", fmt_moeda0(preco_medio_val))

    # ===== Gráfico 1 — Evolução por hora (preço médio por hora) =====
    # Garante 0..23
    horas = pd.DataFrame({"HORA_HH": list(range(24))})
    by_hora = (
        df_emp.groupby("HORA_HH", as_index=False)["TOTAL"].mean()
              .rename(columns={"TOTAL": "PRECO_MEDIO"})
    )
    by_hora = horas.merge(by_hora, on="HORA_HH", how="left").fillna({"PRECO_MEDIO": 0})

    barras_com_tendencia(
        by_hora,
        x_col="HORA_HH",
        y_col="PRECO_MEDIO",
        x_type="O",
        titulo="Evolução diária — preço médio por hora (0–23)",
        nota="Barras = preço médio por hora (América/São Paulo, sem casas decimais). Linha = tendência.",
        sort=list(range(24)),
    )

    # ===== Gráfico 2 — Média de preços por ADVP =====
    by_advp = (
        df_emp.groupby("ADVP", as_index=False)["TOTAL"].mean()
              .rename(columns={"TOTAL": "PRECO_MEDIO"})
              .sort_values("ADVP")
    )
    barras_com_tendencia(
        by_advp,
        x_col="ADVP",
        y_col="PRECO_MEDIO",
        x_type="O",
        titulo="Média de preços por ADVP",
        nota="Barras com rótulo interno; linha de tendência conectando os pontos.",
    )

    # ===== Gráfico 3 — Média de preços por Trecho (TOP 20 p/ legibilidade) =====
    by_trecho = (
        df_emp.groupby("TRECHO", as_index=False)["TOTAL"].mean()
              .rename(columns={"TOTAL": "PRECO_MEDIO"})
              .sort_values("PRECO_MEDIO", ascending=False)
              .head(20)
    )
    barras_com_tendencia(
        by_trecho,
        x_col="TRECHO",
        y_col="PRECO_MEDIO",
        x_type="N",
        titulo="Média de preços por Trecho (Top 20)",
        nota="Ordenado pelo maior preço médio; rótulos dentro das barras.",
    )

    # ===== Tabela — Top 3 preços por Top 3 ADVPs (por Trecho) =====
    base_min = (
        df_emp.groupby(["TRECHO", "ADVP"], as_index=False)["TOTAL"].min()
              .rename(columns={"TOTAL": "PRECO_MIN"})
    )
    rows = []
    for trecho, sub in base_min.groupby("TRECHO", sort=True):
        top = sub.nsmallest(3, "PRECO_MIN").reset_index(drop=True)
        vals = top["PRECO_MIN"].tolist()
        advs = top["ADVP"].tolist()
        row = {
            "TRECHO": trecho,
            "PREÇO TOP 1": vals[0] if len(vals) > 0 else np.nan,
            "ADVP TOP 1":  advs[0] if len(advs) > 0 else np.nan,
            "PREÇO TOP 2": vals[1] if len(vals) > 1 else np.nan,
            "ADVP TOP 2":  advs[1] if len(advs) > 1 else np.nan,
            "PREÇO TOP 3": vals[2] if len(vals) > 2 else np.nan,
            "ADVP TOP 3":  advs[2] if len(advs) > 2 else np.nan,
        }
        rows.append(row)

    top3_tbl = pd.DataFrame(rows).sort_values("TRECHO")
    # Mapa de calor horizontal nos preços
    price_cols = ["PREÇO TOP 1", "PREÇO TOP 2", "PREÇO TOP 3"]
    styled = (
        top3_tbl.style
        .format({c: lambda v: fmt_moeda0(v) for c in price_cols})
        .format({"ADVP TOP 1":"{:.0f}", "ADVP TOP 2":"{:.0f}", "ADVP TOP 3":"{:.0f}"}, na_rep="-")
        .background_gradient(axis=1, subset=price_cols, cmap="YlOrBr")
    )

    st.subheader("Menor preço por Trecho × ADVP — Top 3 por trecho")
    st.caption("Para cada trecho, mostramos os 3 menores preços (e seus ADVPs). Cores indicam valores na linha (mapa de calor horizontal).")
    # Para renderizar o estilo, use st.write/st.table (st.dataframe não aplica Styler)
    st.write(styled)

# ==========================
# Abas por empresa
# ==========================
abas = st.tabs(["FLIPMILHAS", "CAPO VIAGENS", "MAXMILHAS", "123MILHAS"])

with abas[0]:
    render_empresa(view_all[view_all["EMPRESA"] == "FLIPMILHAS"].copy(), "FLIPMILHAS")

with abas[1]:
    render_empresa(view_all[view_all["EMPRESA"] == "CAPO VIAGENS"].copy(), "CAPO VIAGENS")

with abas[2]:
    render_empresa(view_all[view_all["EMPRESA"] == "MAXMILHAS"].copy(), "MAXMILHAS")

with abas[3]:
    render_empresa(view_all[view_all["EMPRESA"] == "123MILHAS"].copy(), "123MILHAS")
