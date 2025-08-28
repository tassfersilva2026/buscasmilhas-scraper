# streamlit_app.py
from __future__ import annotations
from pathlib import Path
from typing import List, Optional
import math
import re
import numpy as np
import pandas as pd
import streamlit as st
import altair as alt

# ==========================
# Configuração base do app
# ==========================
st.set_page_config(page_title="Painel de Concorrência — Flip/Capo/Max/123", layout="wide")

AMARELO     = "#F2C94C"
ROSA_CAPO   = "#E91E63"     # rosa para CAPO
CINZA_TXT   = "#333333"
CINZA_BG    = "#F7F7F7"
GOL_COLOR   = "#F2994A"     # laranja
AZUL_COLOR  = "#1F4E79"     # azul escuro
LATAM_COLOR = "#8B0000"     # vermelho escuro

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
    pats = patterns or [
        "*.xlsx", "*.xls", "*.csv", "*.parquet",
        "FLIPMILHAS_*.*", "CAPOVIAGENS_*.*", "MAXMILHAS_*.*", "123MILHAS_*.*"
    ]
    out: List[Path] = []
    for pat in pats:
        out.extend(sorted(p.glob(pat)))
    # remove duplicatas preservando ordem
    seen = set(); uniq: List[Path] = []
    for f in out:
        if f not in seen:
            uniq.append(f); seen.add(f)
    return uniq

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
    if re.match(r"^CAPOVIAGENS_", u, flags=re.I):
        return "CAPO VIAGENS"
    if re.match(r"^FLIPMILHAS_", u, flags=re.I):
        return "FLIPMILHAS"
    if re.match(r"^MAXMILHAS_", u, flags=re.I):
        return "MAXMILHAS"
    if re.match(r"^123MILHAS_", u, flags=re.I):
        return "123MILHAS"
    if "FLIP" in u or "FLIPMILHAS" in u:
        return "FLIPMILHAS"
    if "CAPO" in u or "CAPOVIAGENS" in u:
        return "CAPO VIAGENS"
    if "MAX" in u and "MILHAS" in u:
        return "MAXMILHAS"
    if "123" in u and "MILHAS" in u:
        return "123MILHAS"
    return "N/A"

def _norm_hhmmss(s: pd.Series) -> pd.Series:
    """Garante HH:MM:SS a partir de texto/horário."""
    txt = s.astype(str).str.strip()
    # tenta formatos comuns
    t = pd.to_datetime(txt, format="%H:%M:%S", errors="coerce")
    t = t.fillna(pd.to_datetime(txt, format="%H:%M", errors="coerce"))
    # fallback por regex (pega primeira ocorrência h:m[:s])
    miss = t.isna()
    if miss.any():
        mtxt = txt[miss].str.extract(r"(?P<h>\d{1,2}):(?P<m>\d{2})(?::(?P<s>\d{2}))?")
        hh = mtxt["h"].fillna("00"); mm = mtxt["m"].fillna("00"); ss = mtxt["s"].fillna("00")
        tloc = pd.to_datetime(hh + ":" + mm + ":" + ss, format="%H:%M:%S", errors="coerce")
        t = t.where(~miss, tloc)
    return t.dt.strftime("%H:%M:%S")

def _norm_ddmmaa_from_any_date(s: pd.Series) -> pd.Series:
    """Converte qualquer data (inclui AAAA/MM/DD) em DD/MM/AAAA (string)."""
    d = pd.to_datetime(s, dayfirst=False, errors="coerce")  # CAPO vem AAAA/MM/DD
    return d.dt.strftime("%d/%m/%Y")

@st.cache_data(show_spinner=False)
def _read_one(path: str, mtime: float) -> pd.DataFrame:
    p = Path(path); ext = p.suffix.lower()
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

    # Normaliza nomes de colunas para comparação
    original_cols = list(df.columns)
    colmap_upper = {c: re.sub(r"\s+", " ", str(c)).strip().upper() for c in df.columns}
    cols_upper_set = {v for v in colmap_upper.values()}

    # ======= Normalização ESPECÍFICA CAPO (captura_data, captura_hora, ...) =======
    lower_cols = {str(c).strip().lower() for c in original_cols}
    is_capo_schema = (
        "captura_data" in lower_cols and
        "captura_hora" in lower_cols and
        "trecho" in lower_cols and
        "antecedencia" in lower_cols and
        "data_voo" in lower_cols and
        "cia" in lower_cols and
        "valor_total" in lower_cols
    )

    if is_capo_schema or detect_empresa_from_filename(p.name) == "CAPO VIAGENS":
        # garante lower para acesso
        df.columns = [str(c).strip().lower() for c in df.columns]
        # Cria as colunas no padrão do app
        df_std = pd.DataFrame()
        df_std["DATA DA BUSCA"]  = _norm_ddmmaa_from_any_date(df.get("captura_data"))
        df_std["HORA DA BUSCA"]  = _norm_hhmmss(df.get("captura_hora"))
        df_std["TRECHO"]         = df.get("trecho")
        df_std["ADVP"]           = pd.to_numeric(df.get("antecedencia"), errors="coerce")
        df_std["DATA PARTIDA"]   = _norm_ddmmaa_from_any_date(df.get("data_voo"))
        df_std["HORA DA PARTIDA"]= _norm_hhmmss(df.get("hr_ida")) if "hr_ida" in df.columns else np.nan
        df_std["DATA CHEGADA"]   = df_std["DATA PARTIDA"]  # CAPO não traz data de volta explícita
        df_std["HORA DA CHEGADA"]= _norm_hhmmss(df.get("hr_volta")) if "hr_volta" in df.columns else np.nan
        df_std["CIA DO VOO"]     = df.get("cia").astype(str).str.strip().str.upper()
        df_std["TARIFA"]         = _to_float_series(df.get("por_adulto"))
        df_std["TX DE EMBARQUE"] = _to_float_series(df.get("taxa_embarque"))
        df_std["TX DE SERVIÇO"]  = _to_float_series(df.get("taxa_servico")) if "taxa_servico" in df.columns else np.nan
        df_std["TOTAL"]          = _to_float_series(df.get("valor_total"))
        df_std["NUMERO DO VOO"]  = df.get("numero_voo") if "numero_voo" in df.columns else np.nan

        # Metadados
        df_std["ARQUIVO"] = p.name
        df_std["CAMINHO"] = str(p)
        df_std["EMPRESA"] = "CAPO VIAGENS"

        # Datetimes e derivados
        def _combo_dt(dt_str: pd.Series, hhmmss_str: pd.Series) -> pd.Series:
            raw = dt_str.fillna("").astype(str).str.strip() + " " + hhmmss_str.fillna("").astype(str).str.strip()
            return pd.to_datetime(raw, dayfirst=True, errors="coerce")

        df_std["BUSCA_DATETIME"]   = _combo_dt(df_std["DATA DA BUSCA"],   df_std["HORA DA BUSCA"])
        df_std["PARTIDA_DATETIME"] = _combo_dt(df_std["DATA PARTIDA"],    df_std["HORA DA PARTIDA"])
        df_std["CHEGADA_DATETIME"] = _combo_dt(df_std["DATA CHEGADA"],    df_std["HORA DA CHEGADA"])
        df_std["HORA_HH"]          = df_std["BUSCA_DATETIME"].dt.hour

        # ADVP: usa fornecido; se vazio, calcula
        advp_calc = (df_std["PARTIDA_DATETIME"].dt.normalize() - df_std["BUSCA_DATETIME"].dt.normalize()).dt.days
        df_std["ADVP"] = df_std["ADVP"].fillna(advp_calc)

        # Ordena colunas base
        base = [
            "BUSCA_DATETIME","DATA DA BUSCA","HORA DA BUSCA","HORA_HH",
            "TRECHO","CIA DO VOO","ADVP",
            "PARTIDA_DATETIME","DATA PARTIDA","HORA DA PARTIDA",
            "CHEGADA_DATETIME","DATA CHEGADA","HORA DA CHEGADA",
            "TARIFA","TX DE EMBARQUE","TX DE SERVIÇO","TOTAL",
            "NUMERO DO VOO","EMPRESA","ARQUIVO","CAMINHO"
        ]
        other = [c for c in df_std.columns if c not in base]
        return df_std[base + other]

    # ======= Normalização geral (demais empresas) =======
    df = df.rename(columns=colmap_upper)

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
        "DATA DA BUSCA","HORA DA BUSCA","TRECHO",
        "DATA PARTIDA","HORA DA PARTIDA","DATA CHEGADA","HORA DA CHEGADA",
        "TARIFA","TX DE EMBARQUE","TOTAL","CIA DO VOO"
    ]
    for c in required:
        if c not in df.columns:
            df[c] = np.nan

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
    df["ARQUIVO"] = p.name; df["CAMINHO"] = str(p); df["EMPRESA"] = detect_empresa_from_filename(p.name)

    base = [
        "BUSCA_DATETIME","DATA DA BUSCA","HORA DA BUSCA","HORA_HH",
        "TRECHO","CIA DO VOO","ADVP",
        "PARTIDA_DATETIME","DATA PARTIDA","HORA DA PARTIDA",
        "CHEGADA_DATETIME","DATA CHEGADA","HORA DA CHEGADA",
        "TARIFA","TX DE EMBARQUE","TOTAL",
        "EMPRESA","ARQUIVO","CAMINHO"
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

# ============== utils p/ eixo dinâmico ==============
def _nice_ceil(value: float, step: int = 50) -> int:
    if not np.isfinite(value) or value <= 0:
        return step
    return int(math.ceil(value / step) * step)

def dynamic_limit(series: pd.Series, hard_cap: Optional[int]) -> int:
    s = pd.to_numeric(series, errors="coerce")
    vmax = float(np.nanmax(s.values)) if len(s) else 0.0
    pad  = max(50.0, 0.10 * vmax)
    y    = _nice_ceil(vmax + pad, step=50)
    if hard_cap is not None:
        y = min(y, int(hard_cap))
    return max(y, 100)

# ======================================================
# FILTROS NO TOPO
# ======================================================
with st.spinner("Lendo planilhas da pasta data/…"):
    df_all = load_all(DATA_DIR_DEFAULT)
if df_all.empty:
    st.info("Nenhum arquivo lido. Verifique a pasta data/.")
    st.stop()

min_d = df_all["BUSCA_DATETIME"].dropna().min()
max_d = df_all["BUSCA_DATETIME"].dropna().max()

c1, c2, c3, c4, c5 = st.columns([1.2,1.2,1.6,3.4,1.6])
d_ini = c1.date_input(
    "Data inicial",
    value=min_d.date() if pd.notna(min_d) else None,
    min_value=min_d.date() if pd.notna(min_d) else None,
    max_value=max_d.date() if pd.notna(max_d) else None,
    format="DD/MM/YYYY"
)
d_fim = c2.date_input(
    "Data final",
    value=max_d.date() if pd.notna(max_d) else None,
    min_value=min_d.date() if pd.notna(min_d) else None,
    max_value=max_d.date() if pd.notna(max_d) else None,
    format="DD/MM/YYYY"
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
def x_axis(enc: str, title: Optional[str]=None):
    return alt.X(enc, axis=alt.Axis(title=title, labelAngle=0, labelOverlap=True,
                                    labelFontWeight="bold", labelColor=CINZA_TXT))
def y_axis(enc: str, title: str="PREÇO", domain=None):
    return alt.Y(enc, axis=alt.Axis(title=title, format=".0f",
                                    labelFontWeight="bold", labelColor=CINZA_TXT),
                 scale=alt.Scale(domain=domain) if domain is not None else alt.Undefined)

def barras_com_tendencia(df: pd.DataFrame, x_col: str, y_col: str, x_type: str,
                         titulo: str, *, x_title: Optional[str]=None,
                         y_max: Optional[int]=None, sort=None, bar_color: str = AMARELO):
    """Barras + rótulo + linha pontilhada."""
    df = df.copy(); df["_LABEL"] = df[y_col].apply(fmt_pontos)

    base = alt.Chart(df).encode(
        x=(x_axis(f"{x_col}:{x_type}", title=x_title).sort(sort)
           if sort is not None else x_axis(f"{x_col}:{x_type}", title=x_title)),
        y=y_axis(f"{y_col}:Q", domain=[0, y_max] if y_max else None),
        tooltip=[x_col, alt.Tooltip(y_col, format=",.0f")],
    )
    bars = base.mark_bar(color=bar_color)

    labels = alt.Chart(df).encode(
        x=(x_axis(f"{x_col}:{x_type}", title=x_title).sort(sort)
           if sort is not None else x_axis(f"{x_col}:{x_type}", title=x_title)),
        y=y_axis(f"{y_col}:Q", domain=[0, y_max] if y_max else None),
        text=alt.Text("_LABEL:N"),
    ).mark_text(
        baseline="top", align="center", dy=14, color=CINZA_TXT,
        fontWeight="bold", size=18,
    )

    line = (
        alt.Chart(df)
        .mark_line(color=CINZA_TXT, opacity=0.95, strokeDash=[6,4])
        .encode(x=x_axis(f"{x_col}:{x_type}", title=x_title),
                y=y_axis(f"{y_col}:Q", domain=[0, y_max] if y_max else None))
    )

    ch = (bars + labels + line).properties(title=titulo, height=340)
    st.altair_chart(ch, use_container_width=True)

# ==========================
# SHARE CIAS (stack normalizado + rótulos)
# ==========================
def chart_cia_stack_trecho(df_emp: pd.DataFrame):
    if df_emp.empty:
        st.info("Sem dados para os filtros atuais.")
        return

    cia_raw = df_emp["CIA DO VOO"].astype(str).str.upper()
    df = df_emp.copy()
    df["CIA3"] = np.select(
        [cia_raw.str.contains("AZUL"), cia_raw.str.contains("GOL"), cia_raw.str.contains("LATAM")],
        ["AZUL", "GOL", "LATAM"],
        default="OUTRAS",
    )
    df = df[df["CIA3"].isin(["AZUL", "GOL", "LATAM"])]
    if df.empty:
        st.info("Sem AZUL/GOL/LATAM para este filtro.")
        return

    base = alt.Chart(df)

    bars = (
        base.mark_bar()
        .encode(
            x=x_axis("TRECHO:N"),
            y=alt.Y(
                "count():Q",
                stack="normalize",
                axis=alt.Axis(format=".0%", title=""),
                scale=alt.Scale(domain=[0, 1.2]),
            ),
            color=alt.Color(
                "CIA3:N",
                scale=alt.Scale(
                    domain=["AZUL", "GOL", "LATAM"],
                    range=[AZUL_COLOR, GOL_COLOR, LATAM_COLOR],
                ),
                legend=alt.Legend(title="CIA"),
            ),
            order=alt.Order("CIA3:N", sort="ascending"),
        )
    )

    text = (
        base
        .transform_aggregate(count="count()", groupby=["TRECHO", "CIA3"])
        .transform_stack(
            stack="count",
            groupby=["TRECHO"],
            sort=[alt.SortField("CIA3", order="ascending")],
            as_=["y0", "y1"],
            offset="normalize",
        )
        .transform_calculate(
            ycenter="(datum.y0 + datum.y1)/2",
            label="format(datum.y1 - datum.y0, '.0%')",
        )
        .mark_text(baseline="middle", align="center", size=18, fontWeight="bold", color="#FFFFFF")
        .encode(
            x=x_axis("TRECHO:N"),
            y=alt.Y("ycenter:Q", scale=alt.Scale(domain=[0, 1.2])),
            text="label:N",
            detail="CIA3:N",
        )
    )

    ch = (bars + text).properties(title="SHARE CIAS", height=380)
    st.altair_chart(ch, use_container_width=True)

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
def render_empresa(df_emp: pd.DataFrame, key_suffix: str, *, cor_barra: str = AMARELO):
    menor_preco = st.toggle(
        "Menor preço",
        value=True,
        key=f"toggle_menor_preco_{key_suffix}",
        help="Ligado: usa menor preço; Desligado: usa média. Vale para gráficos e tabela."
    )

    if df_emp.empty:
        st.info("Sem dados para os filtros atuais."); return

    hard_cap = 1500 if menor_preco else 3000

    # KPIs
    k1, k2 = st.columns(2)
    with k1:
        st.metric("Buscas", f"{len(df_emp):,}".replace(",", "."))
    with k2:
        preco_val = df_emp["TOTAL"].min() if menor_preco else df_emp["TOTAL"].mean()
        st.metric("Preço", fmt_moeda0(preco_val))

    # 1) Preço por hora
    horas = pd.DataFrame({"HORA_HH": list(range(24))})
    if menor_preco:
        by_hora = df_emp.groupby("HORA_HH", as_index=False)["TOTAL"].min().rename(columns={"TOTAL":"PRECO"})
    else:
        by_hora = df_emp.groupby("HORA_HH", as_index=False)["TOTAL"].mean().rename(columns={"TOTAL":"PRECO"})
    by_hora = horas.merge(by_hora, on="HORA_HH", how="left").fillna({"PRECO":0})
    y_max_hora = dynamic_limit(by_hora["PRECO"], hard_cap)
    barras_com_tendencia(by_hora, "HORA_HH", "PRECO", "O",
                         "Preço por hora", x_title="HORA",
                         y_max=y_max_hora, sort=list(range(24)), bar_color=cor_barra)

    # 2) Preço por ADVP
    if menor_preco:
        by_advp = df_emp.groupby("ADVP", as_index=False)["TOTAL"].min().rename(columns={"TOTAL":"PRECO"}).sort_values("ADVP")
    else:
        by_advp = df_emp.groupby("ADVP", as_index=False)["TOTAL"].mean().rename(columns={"TOTAL":"PRECO"}).sort_values("ADVP")
    y_max_advp = dynamic_limit(by_advp["PRECO"], hard_cap)
    barras_com_tendencia(by_advp, "ADVP", "PRECO", "O",
                         "Preço por ADVP", y_max=y_max_advp, bar_color=cor_barra)

    # 3) Preço Top 20 trechos
    if menor_preco:
        by_trecho = (df_emp.groupby("TRECHO", as_index=False)["TOTAL"].min()
                          .rename(columns={"TOTAL":"PRECO"})
                          .sort_values("PRECO", ascending=False).head(20))
    else:
        by_trecho = (df_emp.groupby("TRECHO", as_index=False)["TOTAL"].mean()
                          .rename(columns={"TOTAL":"PRECO"})
                          .sort_values("PRECO", ascending=False).head(20))
    y_max_trecho = dynamic_limit(by_trecho["PRECO"], hard_cap)
    barras_com_tendencia(by_trecho, "TRECHO", "PRECO", "N",
                         "Preço Top 20 trechos", y_max=y_max_trecho, bar_color=cor_barra)

    # 4) Tabela Top 3
    top3_tabela(df_emp, agg="min" if menor_preco else "mean")

    # 5) SHARE CIAS (mantém paleta por CIA)
    chart_cia_stack_trecho(df_emp)

# ==========================
# Abas
# ==========================
abas = st.tabs(["FLIPMILHAS","CAPO VIAGENS","MAXMILHAS","123MILHAS"])
with abas[0]:
    render_empresa(view_all[view_all["EMPRESA"] == "FLIPMILHAS"].copy(), "FLIPMILHAS", cor_barra=AMARELO)
with abas[1]:
    # CAPO: mesma estrutura do FLIPMILHAS, com barras rosa
    render_empresa(view_all[view_all["EMPRESA"] == "CAPO VIAGENS"].copy(), "CAPO", cor_barra=ROSA_CAPO)
with abas[2]:
    render_empresa(view_all[view_all["EMPRESA"] == "MAXMILHAS"].copy(), "MAX", cor_barra=AMARELO)
with abas[3]:
    render_empresa(view_all[view_all["EMPRESA"] == "123MILHAS"].copy(), "123", cor_barra=AMARELO)
