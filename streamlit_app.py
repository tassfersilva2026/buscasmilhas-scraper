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

AMARELO   = "#F2C94C"
CINZA_TXT = "#333333"
CINZA_BG  = "#F7F7F7"
GOL_COLOR   = "#F2994A"   # laranja
AZUL_COLOR  = "#1F4E79"   # azul escuro
LATAM_COLOR = "#8B0000"   # vermelho escuro

st.markdown(
    f"""
    <style>
      .block-container {{ padding-top: 0.6rem; }}
      h1, h2, h3, h4, h5, h6 {{ color: {CINZA_TXT}; }}
      .kpi .stMetric {{ background:{CINZA_BG}; border-radius:12px; padding:10px; }}
      .smallcap {{ color:#666; font-size:0.9rem; margin-top:-8px; }}
      table td, table th {{ font-size: 0.95rem; }}
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

    colmap = {c: re.sub(r"\s+", " ", str(c)).strip().upper() for c in df.columns}
    df = df.rename(columns=colmap)

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
        if pd.isna(v): return "-"
        return "R$ " + f"{int(round(float(v))):,}".replace(",", ".")
    except Exception:
        return "-"

def fmt_pontos(v: float) -> str:
    try:
        return f"{int(round(float(v))):,}".replace(",", ".")
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
d_ini = c1.date_input("Data inicial",
    value=min_d.date() if pd.notna(min_d) else None,
    min_value=min_d.date() if pd.notna(min_d) else None,
    max_value=max_d.date() if pd.notna(max_d) else None,
    format="DD/MM/YYYY",
)
d_fim = c2.date_input("Data final",
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
# Helpers de gráfico
# ======================================================
def x_axis(enc: str):
    return alt.X(enc, axis=alt.Axis(labelAngle=0, labelOverlap=True, labelFontWeight="bold", labelColor=CINZA_TXT))

def y_axis(enc: str, domain=None):
    return alt.Y(enc, axis=alt.Axis(format=".0f", labelFontWeight="bold", labelColor=CINZA_TXT),
                 scale=alt.Scale(domain=domain) if domain is not None else alt.Undefined)

def barras_com_tendencia(df: pd.DataFrame, x_col: str, y_col: str, x_type: str, titulo: str, nota: str, sort=None, y_max: Optional[int]=None):
    # prepara rótulo com milhar pontuado
    df = df.copy()
    df["_LABEL"] = df[y_col].apply(fmt_pontos)

    base = alt.Chart(df).encode(
        x=(x_axis(f"{x_col}:{x_type}").sort(sort) if sort is not None else x_axis(f"{x_col}:{x_type}")),
        y=y_axis(f"{y_col}:Q", domain=[0, y_max] if y_max else None),
        tooltip=[x_col, alt.Tooltip(y_col, format=",.0f")],
    )
    bars = base.mark_bar(color=AMARELO)

    # rótulo DENTRO da barra (desce 14px para ficar dentro do topo)
    labels = alt.Chart(df).encode(
        x=x_axis(f"{x_col}:{x_type}") if sort is None else x_axis(f"{x_col}:{x_type}").sort(sort),
        y=y_axis(f"{y_col}:Q", domain=[0, y_max] if y_max else None),
        text=alt.Text("_LABEL:N"),
    ).mark_text(
        baseline="top",      # ancora no topo…
        align="center",
        dy=14,               # …e empurra para DENTRO da barra
        color=CINZA_TXT,
        fontWeight="bold",
        size=18,
    )

    # Linha de tendência (loess)
    line = (
        alt.Chart(df)
        .transform_loess(x_col, y_col, bandwidth=0.6)
        .mark_line(color=CINZA_TXT, opacity=0.9)
        .encode(x=x_axis(f"{x_col}:{x_type}"), y=y_axis("loess:Q", domain=[0, y_max] if y_max else None))
    )

    ch = (bars + labels + line).properties(title=titulo, height=340)
    st.altair_chart(ch, use_container_width=True)
    st.markdown(f'<div class="smallcap">{nota}</div>', unsafe_allow_html=True)

def chart_cia_stack_trecho(df_emp: pd.DataFrame):
    # Normaliza CIA (LATAM Airlines -> LATAM)
    cia = df_emp["CIA DO VOO"].astype(str).str.upper()
    df_emp = df_emp.copy()
    df_emp["CIA3"] = np.select(
        [cia.str.contains("GOL"),
         cia.str.contains("AZUL"),
         cia.str.contains("LATAM")],
        ["GOL", "AZUL", "LATAM"],
        default="OUTRAS",
    )
    df_emp = df_emp[df_emp["CIA3"].isin(["GOL", "AZUL", "LATAM"])]
    if df_emp.empty:
        st.info("Sem dados de GOL/AZUL/LATAM para os filtros atuais.")
        return

    grp = df_emp.groupby(["TRECHO", "CIA3"], as_index=False).size().rename(columns={"size":"COUNT"})
    tot = grp.groupby("TRECHO", as_index=False)["COUNT"].sum().rename(columns={"COUNT":"TOT"})
    dfp = grp.merge(tot, on="TRECHO", how="left")
    dfp["PERC"] = dfp["COUNT"] / dfp["TOT"]
    dfp["PERC_TXT"] = (dfp["PERC"]*100).round(0).astype(int).astype(str) + "%"

    base = alt.Chart(dfp).encode(
        x=x_axis("TRECHO:N"),
        y=alt.Y("COUNT:Q", stack="normalize"),
        color=alt.Color("CIA3:N",
                        scale=alt.Scale(domain=["GOL","AZUL","LATAM"],
                                        range=[GOL_COLOR, AZUL_COLOR, LATAM_COLOR]),
                        legend=alt.Legend(title="CIA")),
        detail="CIA3:N",  # importante p/ o rótulo ficar por segmento (não 100%)
        tooltip=[alt.Tooltip("TRECHO:N"), alt.Tooltip("CIA3:N"),
                 alt.Tooltip("PERC:Q", format=".0%"), alt.Tooltip("COUNT:Q")]
    )

    bars = base.mark_bar()

    # RÓTULO PRETO, CENTRALIZADO DENTRO DO SEGMENTO
    labels = base.mark_text(
        baseline="middle",
        align="center",
        color="#000",
        fontWeight="bold",
        size=18
    ).encode(
        text=alt.Text("PERC_TXT:N"),
        y=alt.Y("COUNT:Q", stack="normalize"),
    )

    ch = (bars + labels).properties(
        title="Participação da CIA do voo por Trecho (GOL/AZUL/LATAM) — barras empilhadas",
        height=380
    )
    st.altair_chart(ch, use_container_width=True)
    st.caption("Rótulos = participação dentro do trecho (normalizado).")

# -------- Tabela Top3 com “mapa de calor” CSS (sem matplotlib) --------
def _fmt_currency_int(v):
    try:
        if pd.isna(v): return "-"
        return "R$ " + f"{int(round(float(v))):,}".replace(",", ".")
    except Exception:
        return "-"

def _row_heat_css(row: pd.Series, price_cols: List[str]) -> pd.Series:
    vals = row[price_cols].astype(float).values
    styles = {c: "" for c in row.index}
    if np.all(np.isnan(vals)):
        return pd.Series(styles)

    vmin = np.nanmin(vals); vmax = np.nanmax(vals)
    rng = max(vmax - vmin, 1e-9)
    def interp(v):
        c0 = (255, 247, 224)  # #FFF7E0
        c1 = (242, 201,  76)  # #F2C94C
        t = (v - vmin) / rng
        r = int(c0[0] + t*(c1[0]-c0[0]))
        g = int(c0[1] + t*(c1[1]-c0[1]))
        b = int(c0[2] + t*(c1[2]-c0[2]))
        return f"background-color: rgb({r},{g},{b});"
    for c in price_cols:
        v = row[c]
        if not pd.isna(v):
            styles[c] = interp(float(v))
    return pd.Series(styles)

def top3_tabela(df_emp: pd.DataFrame):
    base_min = (
        df_emp.groupby(["TRECHO", "ADVP"], as_index=False)["TOTAL"].min()
              .rename(columns={"TOTAL": "PRECO_MIN"})
    )
    rows = []
    for trecho, sub in base_min.groupby("TRECHO", sort=True):
        top = sub.nsmallest(3, "PRECO_MIN").reset_index(drop=True)
        vals = top["PRECO_MIN"].tolist()
        advs = top["ADVP"].tolist()
        rows.append({
            "TRECHO": trecho,
            "PREÇO TOP 1": vals[0] if len(vals) > 0 else np.nan,
            "ADVP TOP 1":  advs[0] if len(advs) > 0 else np.nan,
            "PREÇO TOP 2": vals[1] if len(vals) > 1 else np.nan,
            "ADVP TOP 2":  advs[1] if len(advs) > 1 else np.nan,
            "PREÇO TOP 3": vals[2] if len(vals) > 2 else np.nan,
            "ADVP TOP 3":  advs[2] if len(advs) > 2 else np.nan,
        })
    if not rows:
        st.info("Sem dados para montar o Top 3 por trecho.")
        return

    df_tbl = pd.DataFrame(rows).sort_values("TRECHO").reset_index(drop=True)
    df_tbl.insert(0, "Nº", range(1, len(df_tbl)+1))  # contagem 1..N

    price_cols = ["PREÇO TOP 1", "PREÇO TOP 2", "PREÇO TOP 3"]
    fmt_map = {c: _fmt_currency_int for c in price_cols}
    fmt_map.update({"ADVP TOP 1":"{:.0f}", "ADVP TOP 2":"{:.0f}", "ADVP TOP 3":"{:.0f}"})

    sty = df_tbl.style.format(fmt_map, na_rep="-").apply(
        lambda r: _row_heat_css(r, price_cols), axis=1
    )
    st.subheader("Menor preço por Trecho × ADVP — Top 3 por trecho")
    st.caption("Para cada trecho, os 3 menores preços (e seus ADVPs). Cores por linha = mapa de calor horizontal.")
    st.write(sty)  # Styler (sem matplotlib)

# ======================================================
# Renderização por empresa
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

    # 1) Preço médio por hora (0..23) — barras + linha de tendência, rótulo interno
    horas = pd.DataFrame({"HORA_HH": list(range(24))})
    by_hora = (
        df_emp.groupby("HORA_HH", as_index=False)["TOTAL"].mean()
              .rename(columns={"TOTAL": "PRECO_MEDIO"})
    )
    by_hora = horas.merge(by_hora, on="HORA_HH", how="left").fillna({"PRECO_MEDIO": 0})
    barras_com_tendencia(
        by_hora, "HORA_HH", "PRECO_MEDIO", "O",
        "Evolução do preço médio por hora (0–23)",
        "Barras internas, linha de tendência; eixo Y até 3000.",
        sort=list(range(24)), y_max=3000,
    )

    # 2) Média de preços por ADVP — barras + linha, rótulo interno
    by_advp = (
        df_emp.groupby("ADVP", as_index=False)["TOTAL"].mean()
              .rename(columns={"TOTAL": "PRECO_MEDIO"})
              .sort_values("ADVP")
    )
    barras_com_tendencia(
        by_advp, "ADVP", "PRECO_MEDIO", "O",
        "Média de preços por ADVP",
        "Rótulos internos; linha de tendência; eixo Y até 3000.",
        y_max=3000,
    )

    # 3) Média de preços por Trecho (Top 20) — barras + linha, rótulo interno
    by_trecho = (
        df_emp.groupby("TRECHO", as_index=False)["TOTAL"].mean()
              .rename(columns={"TOTAL": "PRECO_MEDIO"})
              .sort_values("PRECO_MEDIO", ascending=False)
              .head(20)
    )
    barras_com_tendencia(
        by_trecho, "TRECHO", "PRECO_MEDIO", "N",
        "Média de preços por Trecho (Top 20)",
        "Ordenado pelo maior preço médio; rótulos dentro das barras.",
        y_max=3000,
    )

    # 4) Tabela Top 3 por trecho × ADVP (vem ANTES do gráfico de CIAs)
    top3_tabela(df_emp)

    # 5) (ÚLTIMO) Participação da CIA do voo por Trecho
    chart_cia_stack_trecho(df_emp)

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
