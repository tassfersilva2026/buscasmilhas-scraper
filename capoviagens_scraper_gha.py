#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Capo Viagens — Scraper para GitHub Actions (XLS por grupo + LOG detalhado)
- Salva SEMPRE em: data/CAPO_G{N}_YYYYMMDD_HHMMSS.xls
- Aba: BUSCAS
- COLUNAS (exatamente estas e nesta ordem):
  captura_data, captura_hora, trecho, antecedencia, data_voo,
  cia, hr_ida, hr_volta, por_adulto, taxa_embarque, taxa_servico,
  valor_total, numero_voo

ENV/CLI:
  TRECHOS_CSV="CGH-SDU,SDU-CGH,..."   | --trechos
  ADVPS_CSV="1,5,11,17,30"            | --advps
  GROUP_NAME="G1"                      | --group-name
  WAIT_SECONDS=12                      | --wait-seconds
  MAX_ATTEMPTS=2                       | --attempts
  SLEEP_RETRY=4                        | --sleep-retry
  PAGELOAD_TIMEOUT=30                  | --pageload-timeout
"""

import os
import re
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ====================== LISTAS PADRÃO ======================
ADVP_LIST = [1, 5, 11, 17, 30]
TRECHOS = [
    ("CGH", "SDU"), ("SDU", "CGH"),
    ("GRU", "POA"), ("POA", "GRU"),
    ("CGH", "GIG"), ("GIG", "CGH"),
    ("BSB", "CGH"), ("CGH", "BSB"),
    ("CGH", "REC"), ("REC", "CGH"),
    ("CGH", "SSA"), ("SSA", "CGH"),
    ("BSB", "GIG"), ("GIG", "BSB"),
    ("GIG", "REC"), ("REC", "GIG"),
    ("GIG", "SSA"), ("SSA", "GIG"),
    ("BSB", "SDU"), ("SDU", "BSB"),
]
# ===========================================================

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--trechos", type=str, default=os.getenv("TRECHOS_CSV", ""))     # "A-B,C-D"
    p.add_argument("--advps",   type=str, default=os.getenv("ADVPS_CSV", ""))       # "1,5,11,17,30"
    p.add_argument("--group-name", type=str, default=os.getenv("GROUP_NAME", ""))   # "G1"
    p.add_argument("--wait-seconds", type=int, default=int(os.getenv("WAIT_SECONDS", "12")))
    p.add_argument("--attempts", type=int, default=int(os.getenv("MAX_ATTEMPTS", "2")))
    p.add_argument("--sleep-retry", type=int, default=int(os.getenv("SLEEP_RETRY", "4")))
    p.add_argument("--pageload-timeout", type=int, default=int(os.getenv("PAGELOAD_TIMEOUT", "30")))
    return p.parse_args()

def _resolve_lists(args):
    # TRECHOS
    trechos = TRECHOS
    raw_t = (args.trechos or "").strip()
    if raw_t:
        trechos = []
        for tok in re.split(r"[;,]\s*", raw_t):
            if tok:
                a, b = tok.split("-")
                trechos.append((a.strip().upper(), b.strip().upper()))
    # ADVP
    advps = ADVP_LIST
    raw_a = (args.advps or "").strip()
    if raw_a:
        advps = [int(x) for x in re.split(r"[;,]\s*", raw_a) if x]
    return trechos, advps

def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )

def _make_driver(wait_seconds: int = 12, pageload_timeout: int = 30) -> tuple[webdriver.Chrome, WebDriverWait]:
    opts = ChromeOptions()
    try:
        opts.page_load_strategy = "eager"
    except Exception:
        pass
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--start-maximized")

    chrome_bin = os.environ.get("GOOGLE_CHROME_SHIM") or os.environ.get("CHROME_BIN")
    chrome_driver_dir = os.environ.get("CHROMEWEBDRIVER")

    if chrome_driver_dir:
        service = Service(str(Path(chrome_driver_dir) / "chromedriver"))
    else:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
        except Exception:
            service = Service()  # driver no PATH

    if chrome_bin:
        opts.binary_location = chrome_bin

    driver = webdriver.Chrome(service=service, options=opts)
    try:
        driver.set_page_load_timeout(pageload_timeout)
    except Exception:
        pass

    wait = WebDriverWait(driver, wait_seconds)
    return driver, wait

# XPaths (mantidos)
XPATH = {
    "cia":           "//*[@id='__next']/div[4]/div[3]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label[1]/div[1]/div/span",
    "hr_ida":        "//*[@id='__next']/div[4]/div[3]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label[1]/label/div/div/div[1]/span[1]",
    "hr_volta":      "//*[@id='__next']/div[4]/div[3]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label[1]/label/div/div/div[3]/span[1]",
    "por_adulto":    "//*[@id='__next']/div[4]/div[3]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[1]/div/span[1]",
    "taxa_embarque": "//*[@id='__next']/div[4]/div[3]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[4]/span[2]",
    "taxa_servico":  "//*[@id='__next']/div[4]/div[3]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[5]/span[2]",
    "valor_total":   "//*[@id='__next']/div[4]/div[3]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[6]/span",
    "buy_button":    "//*[@id='btn-buy-now']/button",
    "flight_num":    "//*[@id='__next']/div[4]/div/div/div[2]/div[2]/div/div[2]/div/div[1]/ul/li[3]/strong",
}

def _capturar(wait: WebDriverWait, xpath: str, cond=EC.visibility_of_element_located) -> str:
    try:
        el = wait.until(cond((By.XPATH, xpath)))
        return el.text.strip()
    except Exception:
        return ""

def _parse_money(s: str) -> float:
    if not s:
        return 0.0
    s2 = re.sub(r"[R$\s.]", "", s).replace(",", ".")
    try:
        return float(s2)
    except Exception:
        return 0.0

# ======= escrita .xls via xlwt (sem pandas writer) =======
def _save_xls(df: pd.DataFrame, xls_path: Path):
    try:
        import xlwt
    except Exception as e:
        raise RuntimeError("Pacote 'xlwt' não está instalado. Adicione 'pip install xlwt' no workflow.") from e

    wb = xlwt.Workbook()
    ws = wb.add_sheet("BUSCAS")

    header_style = xlwt.easyxf("font: bold on; align: horiz center; pattern: pattern solid, fore_colour ice_blue;")
    money_style  = xlwt.easyxf(num_format_str="0.00")
    int_style    = xlwt.easyxf(num_format_str="0")
    text_style   = xlwt.easyxf("")

    # Largura de colunas
    for c in range(len(df.columns)):
        ws.col(c).width = 5000

    # Cabeçalho
    for j, col in enumerate(df.columns):
        ws.write(0, j, col, header_style)

    money_cols = {"por_adulto", "taxa_embarque", "taxa_servico", "valor_total"}
    int_cols   = {"antecedencia", "numero_voo"}

    # Linhas
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        for j, col in enumerate(df.columns):
            val = row[col]
            if pd.isna(val) or val == "":
                ws.write(i, j, "", text_style)
            elif col in money_cols and isinstance(val, (int, float)):
                ws.write(i, j, float(val), money_style)
            elif col in int_cols and isinstance(val, (int, float)):
                ws.write(i, j, int(val), int_style)
            else:
                ws.write(i, j, str(val), text_style)

    wb.save(str(xls_path))

# ===============================================================

def run_once():
    args = _parse_args()
    _setup_logging()
    logging.info("==== Iniciando CapoScraper (XLS por grupo) ====")

    grupo = args.group_name or "G0"
    tz = ZoneInfo("America/Sao_Paulo")

    logging.info(f"Grupo: {grupo} | ADVPS={os.getenv('ADVPS_CSV', '') or ADVP_LIST} | "
                 f"WAIT={args.wait_seconds}s | ATTEMPTS={args.attempts} | RETRY_SLEEP={args.sleep_retry}s | "
                 f"PAGELOAD_TIMEOUT={args.pageload_timeout}s")

    trechos, advps = _resolve_lists(args)
    combos = [((o, d), a) for (o, d) in trechos for a in advps]

    logging.info(f"Total combos: {len(combos)}")
    logging.info(f"TRECHOS alvo: {trechos}")
    logging.info(f"ADVPs alvo:   {advps}")

    driver, wait = _make_driver(wait_seconds=args.wait_seconds, pageload_timeout=args.pageload_timeout)

    results: list[dict] = []
    now = datetime.now(tz)
    iter_ts = now.strftime("%Y%m%d_%H%M%S")
    captura_data = now.strftime("%Y-%m-%d")
    captura_hora = now.strftime("%H:%M:%S")

    ok_cnt = 0
    vazio_cnt = 0
    erros_cnt = 0
    t0_run = time.time()

    try:
        for i, ((orig, dest), dias) in enumerate(combos, start=1):
            t0 = time.time()
            trecho_str = f"{orig}-{dest}"
            target_date = datetime.now(tz) + timedelta(days=dias)
            search_date_str = target_date.strftime("%Y-%m-%d")
            url = (
                f"https://www.capoviagens.com.br/voos/"
                f"?fromAirport={orig}&toAirport={dest}"
                f"&departureDate={search_date_str}"
                f"&adult=1&child=0&cabin=Basic&isTwoWays=false"
            )

            logging.info(f"[{i}/{len(combos)}] INÍCIO | Trecho={trecho_str} | ADVP={dias} | URL={url}")
            tent = 1
            cia = hr_ida = hr_volta = por_adulto = taxa_embarque = taxa_servico = valor_total = ""
            num_voo = ""

            while tent <= max(1, args.attempts):
                try:
                    t_nav0 = time.time()
                    driver.get(url)
                    t_nav = time.time() - t_nav0
                    logging.info(f"  Tentativa {tent}/{args.attempts} | Navegação em {t_nav:.1f}s")

                    cia           = _capturar(wait, XPATH["cia"])
                    hr_ida        = _capturar(wait, XPATH["hr_ida"])
                    hr_volta      = _capturar(wait, XPATH["hr_volta"])
                    por_adulto    = _capturar(wait, XPATH["por_adulto"])
                    taxa_embarque = _capturar(wait, XPATH["taxa_embarque"])
                    taxa_servico  = _capturar(wait, XPATH["taxa_servico"])
                    valor_total   = _capturar(wait, XPATH["valor_total"])

                    logging.info(f"    Capturado: cia='{cia}' ida='{hr_ida}' volta='{hr_volta}' total='{valor_total}'")

                    if cia or valor_total:
                        try:
                            btn = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH["buy_button"])))
                            btn.click()
                            time.sleep(2)
                            num_voo = _capturar(wait, XPATH["flight_num"], cond=EC.presence_of_element_located)
                            logging.info(f"    Número do voo: '{num_voo}'")
                        except Exception:
                            logging.info("    Número do voo não disponível.")
                        break

                    tent += 1
                    if tent <= args.attempts:
                        logging.info(f"    Sem dados. Aguardando {args.sleep_retry}s…")
                        time.sleep(args.sleep_retry)

                except Exception as e:
                    logging.warning(f"  Erro tentativa {tent}: {e}")
                    tent += 1
                    if tent <= args.attempts:
                        time.sleep(args.sleep_retry)

            if not (cia or valor_total):
                vazio_cnt += 1
                logging.info("  >> Sem dados após tentativas.")
            else:
                ok_cnt += 1

            results.append({
                "captura_data": captura_data,
                "captura_hora": captura_hora,
                "trecho": trecho_str,
                "antecedencia": dias,
                "data_voo": search_date_str,
                "cia": cia,
                "hr_ida": hr_ida,
                "hr_volta": hr_volta,
                "por_adulto": por_adulto,
                "taxa_embarque": taxa_embarque,
                "taxa_servico": taxa_servico,
                "valor_total": valor_total,
                "numero_voo": num_voo,
            })

            dt = time.time() - t0
            logging.info(f"[{i}/{len(combos)}] FIM | {dt:.1f}s | OK={ok_cnt} | Vazio={vazio_cnt} | Erros={erros_cnt}")

    except Exception as e:
        erros_cnt += 1
        logging.error(f"[FATAL] Erro geral: {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # DataFrame e COLUNAS EXATAS / ORDEM EXATA
    df = pd.DataFrame(results)
    cols = [
        "captura_data","captura_hora","trecho","antecedencia","data_voo",
        "cia","hr_ida","hr_volta","por_adulto","taxa_embarque","taxa_servico",
        "valor_total","numero_voo"
    ]
    if not df.empty:
        df = df.reindex(columns=cols, fill_value="")
        # numéricos
        df["por_adulto"]    = df["por_adulto"].apply(_parse_money).astype(float).round(2)
        df["taxa_embarque"] = df["taxa_embarque"].apply(_parse_money).astype(float).round(2)
        df["taxa_servico"]  = df["taxa_servico"].apply(_parse_money).astype(float).round(2)
        df["valor_total"]   = df["valor_total"].apply(_parse_money).astype(float).round(2)
        df["antecedencia"]  = pd.to_numeric(df["antecedencia"], errors="coerce").astype("Int64")
        # numero_voo: extrai dígitos e converte
        num = df["numero_voo"].astype(str).str.extract(r"(\d+)", expand=False)
        df["numero_voo"] = pd.to_numeric(num, errors="coerce").astype("Int64")
    else:
        df = pd.DataFrame(columns=cols)

    # Persistência: data/CAPO_G{N}_YYYYMMDD_HHMMSS.xls
    root = Path(__file__).resolve().parent
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    fname = f"CAPO_{grupo}_{iter_ts}.xls"
    xls_path = data_dir / fname

    try:
        _save_xls(df, xls_path)
        logging.info(f"[SAVE] XLS: {xls_path}")
    except Exception as e:
        logging.error(f"[ERRO] Salvar XLS: {e}")
        raise

    total_exec = time.time() - t0_run
    logging.info("==== RESUMO ====")
    logging.info(f"Combos: {len(combos)} | OK: {ok_cnt} | Sem Ofertas: {vazio_cnt} | Erros: {erros_cnt}")
    logging.info(f"Duração total: {total_exec:.1f}s")
    logging.info("==== FIM ====")

    return xls_path

if __name__ == "__main__":
    run_once()
