# streamlit_app.py
from __future__ import annotations
from pathlib import Path
from typing import List, Optional
import math, re, os, io, hashlib, time
import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
import requests  # << novo: para buscar arquivos direto do GitHub

# ==========================
# Configurações do app
# ==========================
st.set_page_config(page_title="Painel de Concorrência — Flip/Capo/Max/123", layout="wide")

AMARELO     = "#F2C94C"
CINZA_TXT   = "#333333"
CINZA_BG    = "#F7F7F7"
GOL_COLOR   = "#F2994A"
AZUL_COLOR  = "#1F4E79"
LATAM_COLOR = "#8B0000"

st.markdown(
    f"""
    <style>
      .block-container {{ padding-top: 0.6rem; }}
      h1, h2, h3, h4, h5, h6 {{ color: {CINZA_TXT}; }}
      .kpi .stMetric {{ background:{CINZA_BG}; border-radius:12px; padding:10px; }}
      table td, table th {{ font-size: 0.95rem; }}
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown("<h4>Painel de Concorrência — Flip/Capo/Max/123</h4>", unsafe_allow_html=True)

# ==========================
# Fonte dos dados
# ==========================
# Defaults do teu repo (pode sobrescrever via env se quiser)
GH_OWNER  = os.getenv("GH_OWNER",  "tassfersilva2026")
GH_REPO   = os.getenv("GH_REPO",   "flipmilhas-scraper")
GH_BRANCH = os.getenv("GH_BRANCH", "main")
GH_PATH   = os.getenv("GH_PATH",   "data")
GH_TOKEN  = os.getenv("GITHUB_TOKEN", "")  # opcional; sem token funciona (rate limit 60/h)

# Sidebar: escolha de fonte e nº máx de arquivos (para modo GitHub)
st.sidebar.markdown("### Fonte de dados")
USE_GITHUB = st.sidebar.toggle("Ler do GitHub (online)", value=True)
MAX_FILES  = st.sidebar.slider("Arquivos recentes (GitHub)", 5, 100, 30)  # limita downloads
st.sidebar.caption("Dica: deixe ligado para atualizar sem reboot. Desligue para usar /data local.")

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
DATA_DIR_PATH = Path(DATA_DIR_DEFAULT)

# ==========================
# Assinaturas (invalidar cache)
# ==========================
def dir_signature(path: Path, patterns: List[str] | None = None) -> str:
    pats = patterns or ["FLIPMILHAS_*.xlsx", "*.xlsx", "*.xls", "*.csv", "*.parquet"]
    parts: List[str] = []
    for pat in pats:
        for f in path.glob(pat):
            try:
                stt = f.stat()
                parts.append(f"{f.name}:{stt.st_mtime_ns}:{stt.st_size}")
            except FileNotFoundError:
                pass
    return hashlib.md5("|".join(sorted(parts)).encode()).hexdigest()

def gh_headers():
    h = {"User-Agent": "streamlit-app"}
    if GH_TOKEN:
        h["Authorization"] = f"Bearer {GH_TOKEN}"
    return h

@st.cache_data(ttl=0, show_spinner=False)
def gh_list_contents(owner: str, repo: str, path: str, branch: str) -> List[dict]:
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={branch}"
    r = requests.get(url, headers=gh_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def gh_listing_signature(items: List[dict]) -> str:
    # usa name+sha para detectar mudanças
    parts = [f"{it.get('name','')}:{it.get('sha','')}" for it in items if it.get("type") == "file"]
    return hashlib.md5("|".join(sorted(parts)).encode()).hexdigest()

@st.cache_data(ttl=0, show_spinner=False)
def gh_download_excel(url: str, sha: str) -> pd.DataFrame:
    # sha entra na chave do cache; se o arquivo mudar, baixa de novo
    r = requests.get(url, headers=gh_headers(), timeout=60)
    r.raise_for_status()
    bio = io.BytesIO(r.content)
    return pd.read_excel(bio)

# ==========================
# Normalização
# ==========================
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
    if s.dtype.kind in ("i", "u", "f"): return s.astype(float)
    txt = (s.astype(str)
           .str.replace(r"[^0-9,.-]", "", regex=True)
           .str.replace(".", "", regex=False)
           .str.replace(",", ".", regex=False))
    return pd.to_numeric(txt, errors="coerce")

def detect_empresa_from_filename(name: str) -> str:
    u = name.upper()
    if "FLIP" in u or "FLIPMILHAS" in u: return "FLIPMILHAS"
    if "CAPO" in u: return "CAPO VIAGENS"
    if "MAX" in u and "MILHAS" in u: return "MAXMILHAS"
    if "123" in u and "MILHAS" in u: return "123MILHAS"
    return "N/A"

def _normalize_df(df: pd.DataFrame, filename: str, fullpath: str) -> pd.DataFrame:
    colmap = {c: re.sub(r"\s+", " ", str(c)).strip().upper() for c in df.columns}
    df = df.rename(columns=colmap)

    ren = {"CIA":"CIA DO VOO","CIA DO VÔO":"CIA DO VOO","TX EMBARQUE":"TX DE EMBARQUE",
           "TAXA DE EMBARQUE":"TX DE EMBARQUE","VALOR TOTAL":"TOTAL","VALOR":"TOTAL"}
    for k, v in ren.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    required = ["DATA DA BUSCA","HORA DA BUSCA","TRECHO",
                "DATA PARTIDA","HORA DA PARTIDA","DATA CHEGADA","HORA DA CHEGADA",
                "TARIFA","TX DE EMBARQUE","TOTAL","CIA DO VOO"]
    for c in required:
        if c not in df.columns: df[c] = np.nan

    for c in ["TARIFA","TX DE EMBARQUE","TOTAL"]:
        df[c] = _to_float_series(df[c])

    def combo_dt(dcol: str, tcol: str) -> pd.Series:
        d = pd.to_datetime(df[dcol].astype(str).str.strip(), dayfirst=True, errors="coerce")
        t = pd.to_datetime(df[tcol].astype(str).str.strip(), errors="coerce")
        raw = df[dcol].astype(str).str.strip() + " " + df[tcol].astype(str).str.strip()
        dt = pd.to_datetime(raw, dayfirst=True, errors="coerce").fillna(d).fillna(t)
        return dt

    df["BUSCA_DATETIME"]   = combo_dt("DATA DA BUSCA","HORA DA BUSCA")
    df["PARTIDA_DATETIME"] = combo_dt("DATA PARTIDA","HORA DA PARTIDA")
    df["CHEGADA_DATETIME"] = combo_dt("DATA CHEGADA","HORA DA CHEGADA")
    df["HORA_HH"] = df["BUSCA_DATETIME"].dt.hour
    df["ADVP"] = (df["PARTIDA_DATETIME"].dt.normalize() - df["BUSCA_DATETIME"].dt.normalize()).dt.days
    df["ARQUIVO"] = filename; df["CAMINHO"] = fullpath; df["EMPRESA"] = detect_empresa_from_filename(filename)

    base = ["BUSCA_DATETIME","DATA DA BUSCA","HORA DA BUSCA","HORA_HH","TRECHO","CIA DO VOO","ADVP",
            "PARTIDA_DATETIME","DATA PARTIDA","HORA DA PARTIDA",
            "CHEGADA_DATETIME","DATA CHEGADA","HORA DA CHEGADA",
            "TARIFA","TX DE EMBARQUE","TOTAL","EMPRESA","ARQUIVO","CAMINHO"]
    other = [c for c in df.columns if c not in base]
    return df[base + other]

@st.cache_data(show_spinner=False, ttl=0)
def _read_one_local(path: str, mtime: float) -> pd.DataFrame:
    p = Path(path); ext = p.suffix.lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(p)
    elif ext == ".csv":
        for sep in [";", ","]:
            try: df = pd.read_csv(p, sep=sep); break
            except Exception: continue
        else: df = pd.read_csv(p)
    elif ext == ".parquet":
        df = pd.read_parquet(p)
    else:
        return pd.DataFrame()
    return _normalize_df(df, p.name, str(p))

@st.cache_data(show_spinner=True, ttl=0)
def load_all_local(data_dir: str, _sig: str) -> pd.DataFrame:
    files = _list_files(data_dir)
    if not files: return pd.DataFrame()
    parts = []
    for f in files:
        try: parts.append(_read_one_local(str(f), f.stat().st_mtime))
        except Exception as e: st.warning(f"Falha ao ler {f.name}: {e}")
    if not parts: return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    return df.sort_values("BUSCA_DATETIME", ascending=False, kind="stable")

@st.cache_data(show_spinner=True, ttl=0)
def load_all_github(owner: str, repo: str, path: str, branch: str, max_files: int) -> tuple[pd.DataFrame, str]:
    items = gh_list_contents(owner, repo, path, branch)
    # pega só FLIPMILHAS_*.xlsx (e similares se quiser)
    files = [it for it in items if it.get("type")=="file" and it.get("name","").upper().endswith(".XLSX")]
    # ordena por nome desc (formato YYYYMMDD_HHMMSS mantém ordem temporal)
    files = sorted(files, key=lambda x: x["name"], reverse=True)[:max_files]
    sig = gh_listing_signature(files)

    parts = []
    for it in files:
        name = it["name"]; url = it["download_url"]; sha = it.get("sha","")
        try:
            raw = gh_download_excel(url, sha)
            parts.append(_normalize_df(raw, name, url))
        except Exception as e:
            st.warning(f"Falha ao baixar {name}: {e}")
    if not parts:
        return pd.DataFrame(), sig
    df = pd.concat(parts, ignore_index=True)
    df = df.sort_values("BUSCA_DATETIME", ascending=False, kind="stable")
    return df, sig

# ==========================
# Formatação/Gráficos (igual ao seu)
# ==========================
def fmt_moeda0(v) -> str:
    try:
        if pd.isna(v): return "-"
        return "R$ " + f"{int(round(float(v))):,}".replace(",", ".")
    except Exception: return "-"

def fmt_pontos(v: float) -> str:
    try: return f"{int(round(float(v))):,}".replace(",", ".")
    except Exception: return "-"

def _nice_ceil(value: float, step: int = 50) -> int:
    if not np.isfinite(value) or value <= 0: return step
    return int(math.ceil(value / step) * step)

def dynamic_limit(series: pd.Series, hard_cap: Optional[int]) -> int:
    s = pd.to_numeric(series, errors="coerce")
    vmax = float(np.nanmax(s.values)) if len(s) else 0.0
    pad  = max(50.0, 0.10 * vmax)
    y    = _nice_ceil(vmax + pad, step=50)
    if hard_cap is not None: y = min(y, int(hard_cap))
    return max(y, 100)

# ==========================
# Controles de atualização
# ==========================
c_upd, _ = st.columns([1, 8])
with c_upd:
    if st.button("🔄 Atualizar agora", use_container_width=True):
        st.cache_data.clear()
        try: st.rerun()
        except Exception: st.experimental_rerun()

if st.sidebar.toggle("⏱️ Auto-atualizar a cada 60s", value=False):
    st.caption("Auto-atualizando em 60s…")
    time.sleep(60)
    st.rerun()

# ==========================
# Carregamento de dados
# ==========================
with st.spinner("Carregando dados…"):
    if USE_GITHUB:
        df_all, gh_sig = load_all_github(GH_OWNER, GH_REPO, GH_PATH, GH_BRANCH, MAX_FILES)
    else:
        sig_local = dir_signature(DATA_DIR_PATH)
        df_all = load_all_local(DATA_DIR_DEFAULT, sig_local)

if df_all.empty:
    st.info("Nenhum arquivo encontrado na fonte selecionada."); st.stop()

# Info da fonte
if USE_GITHUB:
    st.caption(f"Fonte: GitHub `{GH_OWNER}/{GH_REPO}` • branch `{GH_BRANCH}` • pasta `{GH_PATH}` • arquivos recentes: {MAX_FILES}")
else:
    try:
        latest_file = max(DATA_DIR_PATH.glob("FLIPMILHAS_*.xlsx"), key=lambda f: f.stat().st_mtime)
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(latest_file.stat().st_mtime))
        st.caption(f"Fonte local: `data/` • mais recente: `{latest_file.name}` • mtime: {ts}")
    except ValueError:
        st.caption("Fonte local: `data/`")

# ==========================
# Filtros
# ==========================
min_d = df_all["BUSCA_DATETIME"].dropna().min()
max_d = df_all["BUSCA_DATETIME"].dropna().max()

c1, c2, c3, c4, c5 = st.columns([1.2,1.2,1.6,3.4,1.6])
d_ini = c1.date_input("Data inicial",
    value=min_d.date() if pd.notna(min_d) else None,
    min_value=min_d.date() if pd.notna(min_d) else None,
    max_value=max_d.date() if pd.notna(max_d) else None, format="DD/MM/YYYY")
d_fim = c2.date_input("Data final",
    value=max_d.date() if pd.notna(max_d) else None,
    min_value=min_d.date() if pd.notna(min_d) else None,
    max_value=max_d.date() if pd.notna(max_d) else None, format="DD/MM/YYYY")

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
if advp_sel:   mask &= df_all["ADVP"].isin(advp_sel)
if trecho_sel: mask &= df_all["TRECHO"].isin(trecho_sel)
if hora_sel:   mask &= df_all["HORA_HH"].isin(hora_sel)

view_all = df_all.loc[mask].copy()
st.caption(
    f"Linhas após filtros: **{len(view_all):,}** • Última data no dado: **{df_all['BUSCA_DATETIME'].max():%d/%m/%Y - %H:%M:%S}**".replace(",", ".")
)
st.markdown("---")

# ==========================
# Helpers de gráfico (iguais)
# ==========================
def x_axis(enc: str, title: Optional[str]=None):
    return alt.X(enc, axis=alt.Axis(title=title, labelAngle=0, labelOverlap=True,
                                    labelFontWeight="bold", labelColor=CINZA_TXT))
def y_axis(enc: str, title: str="PREÇO", domain=None):
    return alt.Y(enc, axis=alt.Axis(title=title, format=".0f",
                                    labelFontWeight="bold", labelColor=CINZA_TXT),
                 scale=alt.Scale(domain=domain) if domain is not None else alt.Undefined)

def barras_com_tendencia(df: pd.DataFrame, x_col: str, y_col: str, x_type: str,
                         titulo: str, *, x_title: Optional[str]=None,
                         y_max: Optional[int]=None, sort=None):
    df = df.copy(); df["_LABEL"] = df[y_col].apply(fmt_pontos)

    base = alt.Chart(df).encode(
        x=(x_axis(f"{x_col}:{x_type}", title=x_title).sort(sort)
           if sort is not None else x_axis(f"{x_col}:{x_type}", title=x_title)),
        y=y_axis(f"{y_col}:Q", domain=[0, y_max] if y_max else None),
        tooltip=[x_col, alt.Tooltip(y_col, format=",.0f")],
    )
    bars = base.mark_bar(color=AMARELO)

    labels = alt.Chart(df).encode(
        x=(x_axis(f"{x_col}:{x_type}", title=x_title).sort(sort)
           if sort is not None else x_axis(f"{x_col}:{x_type}", title=x_title)),
        y=y_axis(f"{y_col}:Q", domain=[0, y_max] if y_max else None),
        text=alt.Text("_LABEL:N"),
    ).mark_text(baseline="top", align="center", dy=14, color=CINZA_TXT,
                fontWeight="bold", size=18)

    line = (
        alt.Chart(df)
        .mark_line(color=CINZA_TXT, opacity=0.95, strokeDash=[6,4])
        .encode(x=x_axis(f"{x_col}:{x_type}", title=x_title),
                y=y_axis(f"{y_col}:Q", domain=[0, y_max] if y_max else None))
    )

    ch = (bars + labels + line).properties(title=titulo, height=340)
    st.altair_chart(ch, use_container_width=True)

# ==========================
# SHARE CIAS
# ==========================
def chart_cia_stack_trecho(df_emp: pd.DataFrame):
    if df_emp.empty:
        st.info("Sem dados para os filtros atuais."); return

    cia_raw = df_emp["CIA DO VOO"].astype(str).str.upper()
    df = df_emp.copy()
    df["CIA3"] = np.select(
        [cia_raw.str.contains("AZUL"), cia_raw.str.contains("GOL"), cia_raw.str.contains("LATAM")],
        ["AZUL", "GOL", "LATAM"],
        default="OUTRAS",
    )
    df = df[df["CIA3"].isin(["AZUL", "GOL", "LATAM"])]
    if df.empty:
        st.info("Sem AZUL/GOL/LATAM para este filtro."); return

    base = alt.Chart(df)
    bars = base.mark_bar().encode(
        x=x_axis("TRECHO:N"),
        y=alt.Y("count():Q", stack="normalize",
                axis=alt.Axis(format=".0%", title=""),
                scale=alt.Scale(domain=[0, 1.2])),
        color=alt.Color("CIA3:N",
                        scale=alt.Scale(domain=["AZUL","GOL","LATAM"],
                                        range=[AZUL_COLOR, GOL_COLOR, LATAM_COLOR]),
                        legend=alt.Legend(title="CIA")),
        order=alt.Order("CIA3:N", sort="ascending"),
    )
    text = (base
        .transform_aggregate(count="count()", groupby=["TRECHO","CIA3"])
        .transform_stack(stack="count", groupby=["TRECHO"],
                         sort=[alt.SortField("CIA3", order="ascending")],
                         as_=["y0","y1"], offset="normalize")
        .transform_calculate(ycenter="(datum.y0 + datum.y1)/2",
                             label="format(datum.y1 - datum.y0, '.0%')")
        .mark_text(baseline="middle", align="center", size=18, fontWeight="bold", color="#FFFFFF")
        .encode(x=x_axis("TRECHO:N"),
                y=alt.Y("ycenter:Q", scale=alt.Scale(domain=[0, 1.2])),
                text="label:N", detail="CIA3:N")
    )
    st.altair_chart((bars + text).properties(title="SHARE CIAS", height=380), use_container_width=True)

# ==========================
# Tabela Top 3
# ==========================
def _fmt_currency_int(v):
    try:
        if pd.isna(v): return "-"
        return "R$ " + f"{int(round(float(v))):,}".replace(",", ".")
    except Exception: return "-"

def _row_heat_css(row: pd.Series, price_cols: List[str]) -> pd.Series:
    vals = row[price_cols].astype(float).values
    styles = {c: "" for c in row.index}
    if np.all(np.isnan(vals)): return pd.Series(styles)
    vmin = np.nanmin(vals); vmax = np.nanmax(vals); rng = max(vmax - vmin, 1e-9)
    def interp(v):
        c0=(255,247,224); c1=(242,201,76); t=(v - vmin)/rng
        r=int(c0[0]+t*(c1[0]-c0[0])); g=int(c0[1]+t*(c1[1]-c0[1])); b=int(c0[2]+t*(c1[2]-c0[2]))
        return f"background-color: rgb({r},{g},{b});"
    for c in price_cols:
        v=row[c]
        if not pd.isna(v): styles[c]=interp(float(v))
    return pd.Series(styles)

def top3_tabela(df_emp: pd.DataFrame, agg: str):
    if agg not in ("min", "mean"): agg = "min"
    if agg == "min":
        base = (df_emp.groupby(["TRECHO","ADVP"], as_index=False)["TOTAL"].min()
                .rename(columns={"TOTAL":"VAL"}))
    else:
        base = (df_emp.groupby(["TRECHO","ADVP"], as_index=False)["TOTAL"].mean()
                .rename(columns={"TOTAL":"VAL"}))
    rows=[]
    for trecho, sub in base.groupby("TRECHO", sort=True):
        top = sub.nsmallest(3, "VAL").reset_index(drop=True)
        vals = top["VAL"].tolist(); advs = top["ADVP"].tolist()
        rows.append({"TRECHO":trecho,
                     "PREÇO TOP 1": vals[0] if len(vals)>0 else np.nan, "ADVP TOP 1":advs[0] if len(advs)>0 else np.nan,
                     "PREÇO TOP 2": vals[1] if len(vals)>1 else np.nan, "ADVP TOP 2":advs[1] if len(advs)>1 else np.nan,
                     "PREÇO TOP 3": vals[2] if len(vals)>2 else np.nan, "ADVP TOP 3":advs[2] if len(advs)>2 else np.nan})
    if not rows:
        st.info("Sem dados para montar o Top 3 por trecho."); return
    df_tbl = pd.DataFrame(rows).sort_values("TRECHO").reset_index(drop=True)
    df_tbl.index = pd.RangeIndex(start=1, stop=len(df_tbl)+1, step=1)
    df_tbl.index.name = None
    price_cols = ["PREÇO TOP 1","PREÇO TOP 2","PREÇO TOP 3"]
    fmt_map = {c:_fmt_currency_int for c in price_cols}
    fmt_map.update({"ADVP TOP 1":"{:.0f}","ADVP TOP 2":"{:.0f}","ADVP TOP 3":"{:.0f}"})
    sty = df_tbl.style.format(fmt_map, na_rep="-").apply(lambda r: _row_heat_css(r, price_cols), axis=1)
    st.markdown("<h5>Preço Top 3 preços por ADVP</h5>", unsafe_allow_html=True)
    st.write(sty)

# ==========================
# Render por empresa
# ==========================
def render_empresa(df_emp: pd.DataFrame, key_suffix: str):
    menor_preco = st.toggle(
        "Menor preço", value=True, key=f"toggle_menor_preco_{key_suffix}",
        help="Ligado: usa menor preço; Desligado: usa média. Vale para gráficos e tabela."
    )
    if df_emp.empty:
        st.info("Sem dados para os filtros atuais."); return

    hard_cap = 1500 if menor_preco else 3000

    k1, k2 = st.columns(2)
    with k1:
        st.metric("Buscas", f"{len(df_emp):,}".replace(",", "."))
    with k2:
        preco_val = df_emp["TOTAL"].min() if menor_preco else df_emp["TOTAL"].mean()
        st.metric("Preço", fmt_moeda0(preco_val))

    horas = pd.DataFrame({"HORA_HH": list(range(24))})
    if menor_preco:
        by_hora = df_emp.groupby("HORA_HH", as_index=False)["TOTAL"].min().rename(columns={"TOTAL":"PRECO"})
    else:
        by_hora = df_emp.groupby("HORA_HH", as_index=False)["TOTAL"].mean().rename(columns={"TOTAL":"PRECO"})
    by_hora = horas.merge(by_hora, on="HORA_HH", how="left").fillna({"PRECO":0})
    y_max_hora = dynamic_limit(by_hora["PRECO"], hard_cap)
    barras_com_tendencia(by_hora, "HORA_HH", "PRECO", "O", "Preço por hora",
                         x_title="HORA", y_max=y_max_hora, sort=list(range(24)))

    if menor_preco:
        by_advp = df_emp.groupby("ADVP", as_index=False)["TOTAL"].min().rename(columns={"TOTAL":"PRECO"}).sort_values("ADVP")
    else:
        by_advp = df_emp.groupby("ADVP", as_index=False)["TOTAL"].mean().rename(columns={"TOTAL":"PRECO"}).sort_values("ADVP")
    y_max_advp = dynamic_limit(by_advp["PRECO"], hard_cap)
    barras_com_tendencia(by_advp, "ADVP", "PRECO", "O", "Preço por ADVP", y_max=y_max_advp)

    if menor_preco:
        by_trecho = (df_emp.groupby("TRECHO", as_index=False)["TOTAL"].min()
                          .rename(columns={"TOTAL":"PRECO"})
                          .sort_values("PRECO", ascending=False).head(20))
    else:
        by_trecho = (df_emp.groupby("TRECHO", as_index=False)["TOTAL"].mean()
                          .rename(columns={"TOTAL":"PRECO"})
                          .sort_values("PRECO", ascending=False).head(20))
    y_max_trecho = dynamic_limit(by_trecho["PRECO"], hard_cap)
    barras_com_tendencia(by_trecho, "TRECHO", "PRECO", "N", "Preço Top 20 trechos", y_max=y_max_trecho)

    top3_tabela(df_emp, agg="min" if menor_preco else "mean")
    chart_cia_stack_trecho(df_emp)

# ==========================
# Abas
# ==========================
abas = st.tabs(["FLIPMILHAS","CAPO VIAGENS","MAXMILHAS","123MILHAS"])
with abas[0]: render_empresa(view_all[view_all["EMPRESA"] == "FLIPMILHAS"].copy(), "FLIPMILHAS")  # view_all será definido abaixo

# (Definição de view_all logo após os filtros)
