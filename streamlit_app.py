from __future__ import annotations
from pathlib import Path
from io import BytesIO
from typing import List
import os
import re
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Consulta de Planilhas — data/", layout="wide", initial_sidebar_state="expanded")

# ==========================
# Configuração básica
# ==========================
# Procura automaticamente uma pasta `data/` a partir do arquivo atual, subindo pastas se preciso.
# Assim, funciona se o app estiver na raiz do repositório OU dentro de uma subpasta (ex.: app/).

from typing import Optional


def find_data_dir(start: Path) -> str:
    cur: Optional[Path] = start
    for _ in range(8):  # evita loop infinito; sobe no máx. 8 níveis
        data_here = cur / "data"
        if data_here.exists() and data_here.is_dir():
            return data_here.as_posix()
        if cur.parent == cur:
            break
        cur = cur.parent
    # fallback: usa ./data ao lado do script
    return (start.parent / "data").as_posix()

DATA_DIR_DEFAULT = find_data_dir(Path(__file__).resolve())

st.title("📊 Consulta de Planilhas (pasta data)")
st.caption("Lê todos os .xlsx/.xls/.csv com o MESMO padrão de colunas e unifica com filtros rápidos.")

# ==========================
# Funções utilitárias
# ==========================

@st.cache_data(show_spinner=False)
def _list_files(data_dir: str, patterns: List[str] | None = None) -> List[Path]:
    """Lista arquivos por padrão (default: xlsx/xls/csv)."""
    p = Path(data_dir)
    if not p.exists():
        return []
    pats = patterns or ["*.xlsx", "*.xls", "*.csv"]
    files: List[Path] = []
    for pat in pats:
        files.extend(sorted(p.glob(pat)))
    return files


def _to_float_series(s: pd.Series) -> pd.Series:
    """Converte valores monetários/numéricos com pontuação BR para float.
    Mantém NaN onde não puder converter."""
    if s.dtype.kind in ("i", "u", "f"):
        return s.astype(float)
    txt = (s.astype(str)
             .str.replace(r"[^0-9,.-]", "", regex=True)
             .str.replace(".", "", regex=False)
             .str.replace(",", ".", regex=False))
    return pd.to_numeric(txt, errors="coerce")


@st.cache_data(show_spinner=False)
def _read_one(path: str, mtime: float) -> pd.DataFrame:
    """Lê um arquivo e normaliza colunas e tipos. mtime é parte da chave de cache."""
    p = Path(path)
    ext = p.suffix.lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(p)
    elif ext == ".csv":
        # tenta ; e ,
        for sep in [";", ","]:
            try:
                df = pd.read_csv(p, sep=sep)
                break
            except Exception:
                continue
        else:
            # fallback simples
            df = pd.read_csv(p)
    else:
        return pd.DataFrame()

    # Normalização leve baseada no exemplo enviado
    # Colunas esperadas (case-insensitive):
    #   DATA DA BUSCA, HORA DA BUSCA, TRECHO,
    #   DATA PARTIDA, HORA DA PARTIDA, DATA CHEGADA, HORA DA CHEGADA,
    #   TARIFA, TX DE EMBARQUE, TOTAL, CIA DO VOO

    # Padroniza nomes (remove espaços duplos, upper)
    colmap = {c: re.sub(r"\s+", " ", str(c)).strip().upper() for c in df.columns}
    df = df.rename(columns=colmap)

    # Renomeia sinônimos (se aparecerem)
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

    # Assegura existência das colunas chaves
    required = [
        "DATA DA BUSCA", "HORA DA BUSCA", "TRECHO",
        "DATA PARTIDA", "HORA DA PARTIDA", "DATA CHEGADA", "HORA DA CHEGADA",
        "TARIFA", "TX DE EMBARQUE", "TOTAL", "CIA DO VOO"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        # Se faltou algo, cria como NaN para não quebrar concat
        for c in missing:
            df[c] = np.nan

    # Converte números
    for c in ["TARIFA", "TX DE EMBARQUE", "TOTAL"]:
        df[c] = _to_float_series(df[c])

    # Datetimes de busca, partida e chegada
    def combo_dt(dcol: str, tcol: str) -> pd.Series:
        d = pd.to_datetime(df[dcol].astype(str).str.strip(), dayfirst=True, errors="coerce")
        t = pd.to_datetime(df[tcol].astype(str).str.strip(), errors="coerce")
        # concatena strings para ganhar robustez (aceita HH:MM ou HH:MM:SS)
        raw = (df[dcol].astype(str).str.strip() + " " + df[tcol].astype(str).str.strip())
        dt = pd.to_datetime(raw, dayfirst=True, errors="coerce")
        # fallback se um dos lados vier completo
        dt = dt.fillna(d)
        dt = dt.fillna(t)
        return dt

    df["BUSCA_DATETIME"]   = combo_dt("DATA DA BUSCA", "HORA DA BUSCA")
    df["PARTIDA_DATETIME"] = combo_dt("DATA PARTIDA", "HORA DA PARTIDA")
    df["CHEGADA_DATETIME"] = combo_dt("DATA CHEGADA", "HORA DA CHEGADA")

    # ADVP = (partida - busca) em dias
    df["ADVP"] = (df["PARTIDA_DATETIME"].dt.normalize() - df["BUSCA_DATETIME"].dt.normalize()).dt.days

    # HH da busca (0..23)
    df["HORA_HH"] = df["BUSCA_DATETIME"].dt.hour

    # Anexa metadados do arquivo
    df["ARQUIVO"] = p.name
    df["CAMINHO"] = str(p)

    # Ordena colunas
    base_cols = [
        "BUSCA_DATETIME", "DATA DA BUSCA", "HORA DA BUSCA", "HORA_HH",
        "TRECHO", "CIA DO VOO", "ADVP",
        "PARTIDA_DATETIME", "DATA PARTIDA", "HORA DA PARTIDA",
        "CHEGADA_DATETIME", "DATA CHEGADA", "HORA DA CHEGADA",
        "TARIFA", "TX DE EMBARQUE", "TOTAL",
        "ARQUIVO", "CAMINHO",
    ]
    other = [c for c in df.columns if c not in base_cols]
    df = df[base_cols + other]
    return df


@st.cache_data(show_spinner=True)
def load_all(data_dir: str, patterns: List[str] | None = None) -> pd.DataFrame:
    files = _list_files(data_dir, patterns)
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
    # Ordena por data/hora de busca desc
    df = df.sort_values("BUSCA_DATETIME", ascending=False, kind="stable")
    return df


def fmt_moeda(v: float | int | None) -> str:
    try:
        if pd.isna(v):
            return "-"
        return f"R$ {float(v):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    except Exception:
        return "-"

# ==========================
# Barra lateral (config)
# ==========================
with st.sidebar:
    st.subheader("⚙️ Configuração")
    data_dir = st.text_input("Pasta de dados", value=DATA_DIR_DEFAULT, help="Caminho da pasta com as planilhas.")
    pats = st.text_input("Padrões de arquivo", value="*.xlsx,*.xls,*.csv", help="Separados por vírgula.")
    patterns = [p.strip() for p in pats.split(",") if p.strip()]
    col1, col2 = st.columns(2)
    if col1.button("🔄 Recarregar dados", use_container_width=True):
        st.cache_data.clear()
    files = _list_files(data_dir, patterns)
    st.caption(f"Arquivos encontrados: {len(files)}")
    if files:
        with st.expander("Ver lista de arquivos", expanded=False):
            for f in files:
                st.write("•", f.name)

# ==========================
# Carregamento
# ==========================
with st.spinner("Lendo planilhas…"):
    df_all = load_all(data_dir, patterns)

if df_all.empty:
    st.info("Nenhum arquivo lido. Confirme a pasta e os padrões.")
    st.stop()

# ==========================
# Filtros
# ==========================
flt = st.container()
with flt:
    st.subheader("🔎 Filtros")

    # Datas da busca
    min_d = df_all["BUSCA_DATETIME"].dropna().min()
    max_d = df_all["BUSCA_DATETIME"].dropna().max()
    if pd.isna(min_d) or pd.isna(max_d):
        date_range = (None, None)
    else:
        c1, c2, c3 = st.columns([2,2,1])
        date_range = c1.date_input(
            "Período da BUSCA",
            value=(min_d.date(), max_d.date()),
            min_value=min_d.date(),
            max_value=max_d.date(),
        )
        hora_range = c2.slider("Hora da BUSCA (HH)", 0, 23, (0, 23))
        advp_min, advp_max = int(df_all["ADVP"].min(skipna=True) or 0), int(df_all["ADVP"].max(skipna=True) or 0)
        advp_sel = c3.slider("ADVP (dias)", advp_min, max(advp_max, advp_min), (advp_min, max(advp_max, advp_min)))

    c1, c2, c3, c4 = st.columns(4)
    trechos = sorted([t for t in df_all["TRECHO"].dropna().unique().tolist() if str(t).strip() != ""])
    cias    = sorted([t for t in df_all["CIA DO VOO"].dropna().unique().tolist() if str(t).strip() != ""])
    trecho_sel = c1.multiselect("Trecho", trechos)
    cia_sel    = c2.multiselect("CIA do voo", cias)

    # Filtro por valor total
    total_min = float(np.nanmin(df_all["TOTAL"])) if df_all["TOTAL"].notna().any() else 0.0
    total_max = float(np.nanmax(df_all["TOTAL"])) if df_all["TOTAL"].notna().any() else 0.0
    val_min, val_max = c3.slider("TOTAL (R$)", int(total_min), int(max(total_max, total_min)), (int(total_min), int(max(total_max, total_min))))

    q = c4.text_input("Busca rápida (Trecho/CIA)")

# Aplica filtros
mask = pd.Series(True, index=df_all.index)
if isinstance(date_range, (list, tuple)) and len(date_range) == 2 and all(date_range):
    d0, d1 = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    mask &= df_all["BUSCA_DATETIME"].dt.date.between(d0.date(), d1.date())

mask &= df_all["HORA_HH"].between(hora_range[0], hora_range[1])
mask &= df_all["ADVP"].between(advp_sel[0], advp_sel[1])

if trecho_sel:
    mask &= df_all["TRECHO"].isin(trecho_sel)
if cia_sel:
    mask &= df_all["CIA DO VOO"].isin(cia_sel)

mask &= df_all["TOTAL"].fillna(0).between(val_min, val_max)

if q:
    qn = q.strip().upper()
    mask &= (
        df_all["TRECHO"].astype(str).str.upper().str.contains(qn, na=False) |
        df_all["CIA DO VOO"].astype(str).str.upper().str.contains(qn, na=False)
    )

view = df_all.loc[mask].copy()

# ==========================
# KPIs
# ==========================
left, mid, right, r2 = st.columns(4)
left.metric("Registros", f"{len(view):,}".replace(",", "."))
mid.metric("Ticket médio (TOTAL)", fmt_moeda(view["TOTAL"].mean()))
right.metric("Tarifa média", fmt_moeda(view["TARIFA"].mean()))
r2.metric("Taxa de embarque média", fmt_moeda(view["TX DE EMBARQUE"].mean()))

# ==========================
# Tabela principal
# ==========================
st.subheader("📄 Resultados")
st.dataframe(view, use_container_width=True, hide_index=True)

# ==========================
# Downloads
# ==========================
colA, colB = st.columns(2)

csv_bytes = view.to_csv(index=False).encode("utf-8-sig")
colA.download_button(
    "⬇️ Baixar CSV (UTF-8)", data=csv_bytes, file_name="resultado.csv", mime="text/csv", use_container_width=True
)

bio = BytesIO()
with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
    view.to_excel(w, index=False, sheet_name="RESULTADO")
excel_bytes = bio.getvalue()
colB.download_button(
    "⬇️ Baixar Excel (.xlsx)", data=excel_bytes, file_name="resultado.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True
)

# Rodapé
st.caption("Dica: clique em 🔄 Recarregar dados após adicionar novos arquivos na pasta.")
