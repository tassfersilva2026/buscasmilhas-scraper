#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Capo Viagens — Scraper para GitHub Actions (LOG detalhado + saída XLS por grupo)
- Gera **apenas XLS** em data/CAPO_G{N}_YYYYMMDD_HHMMSS.xls (um arquivo por parte)
- Aba: BUSCAS
- Requer env GROUP_NAME=G1..G5 (ou CLI --group-name G1)
- ADVP padrão: 1,5,11,17,30 (pode sobrescrever via ADVPS_CSV)

ENV/CLI:
  TRECHOS_CSV="CGH-SDU,SDU-CGH,..."   | --trechos
  ADVPS_CSV="1,5,11,17,30"            | --advps
  GROUP_NAME="G1"                      | --group-name
  WAIT_SECONDS=12                      | --wait-seconds
  MAX_ATTEMPTS=2                       | --attempts
  SLEEP_RETRY=4                        | --sleep-retry
  PAGELOAD_TIMEOUT=30                  | --pageload-timeout
  SLICE_IDX / TOTAL_SLICES             | --slice-idx / --total-slices

Dependências mínimas: selenium, pandas, xlwt, pyarrow (não usado aqui), webdriver-manager (opcional)
"""

import os
import re
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

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
    p.add_argument("--attempts", type[int], default=int(os.getenv("MAX_ATTEMPTS", "2")))
    p.add_argument("--sleep-retry", type=int, default=int(os.getenv("SLEEP_RETRY", "4")))
    p.add_argument("--pageload-timeout", type=int, default=int(os.getenv("PAGELOAD_TIMEOUT", "30")))
    p.add_argument("--slice-idx", type=int, default=int(os.getenv("SLICE_IDX", "0")))
    p.add_argument("--total-slices", type=int, default=int(os.getenv("TOTAL_SLICES", "1")))
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

def run_once(args=None) -> Path:
    if args is None:
        args = _parse_args()

    _setup_logging()
    logging.info("==== Iniciando CapoScraper (XLS por grupo) ====")
    if not args.group_name:
        logging.warning("GROUP_NAME não informado. Usarei 'G0' no nome do arquivo.")
    grupo = args.group_name or "G0"

    logging.info(f"Grupo: {grupo} | ADVPS={os.getenv('ADVPS_CSV', '') or ADVP_LIST} | "
                 f"WAIT={args.wait_seconds}s | ATTEMPTS={args.attempts} | RETRY_SLEEP={args.sleep_retry}s | "
                 f"PAGELOAD_TIMEOUT={args.pageload_timeout}s")

    trechos, advps = _resolve_lists(args)
    combos = [((o, d), a) for (o, d) in trechos for a in advps]

    total = max(1, int(args.total_slices))
    idx   = int(args.slice_idx) % total if total > 1 else 0
    selected = [combos[i] for i in range(len(combos)) if (total == 1 or i % total == idx)]

    logging.info(f"Total combos: {len(combos)} | Slice: {idx+1}/{total} | Executando: {len(selected)} combos")
    logging.info(f"TRECHOS alvo: {trechos}")
    logging.info(f"ADVPs alvo:   {advps}")

    driver, wait = _make_driver(wait_seconds=args.wait_seconds, pageload_timeout=args.pageload_timeout)

    results: list[dict] = []
    now = datetime.now()
    iter_ts = now.strftime("%Y%m%d_%H%M%S")
    captura_data = now.strftime("%Y-%m-%d")
    captura_hora = now.strftime("%H:%M:%S")

    ok_cnt = 0
    vazio_cnt = 0
    erros_cnt = 0
    t0_run = time.time()

    try:
        for i, ((orig, dest), dias) in enumerate(selected, start=1):
            t0 = time.time()
            trecho_str = f"{orig}-{dest}"
            target_date = datetime.now() + timedelta(days=dias)
            search_date_str = target_date.strftime("%Y-%m-%d")
            url = (
                f"https://www.capoviagens.com.br/voos/"
                f"?fromAirport={orig}&toAirport={dest}"
                f"&departureDate={search_date_str}"
                f"&adult=1&child=0&cabin=Basic&isTwoWays=false"
            )

            logging.info(f"[{i}/{len(selected)}] INÍCIO combo | Trecho={trecho_str} | ADVP={dias} | URL={url}")
            tent = 1
            cia = hr_ida = hr_volta = por_adulto = taxa_embarque = taxa_servico = valor_total = ""
            num_voo = ""

            while tent <= max(1, args.attempts):
                try:
                    t_nav0 = time.time()
                    driver.get(url)
                    t_nav = time.time() - t_nav0
                    logging.info(f"  Tentativa {tent}/{args.attempts} | Navegação OK em {t_nav:.1f}s")

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
                            logging.info("    Número do voo não disponível (segue).")
                        break

                    tent += 1
                    if tent <= args.attempts:
                        logging.info(f"    Sem dados visíveis. Aguardando {args.sleep_retry}s para nova tentativa…")
                        time.sleep(args.sleep_retry)

                except Exception as e:
                    logging.warning(f"  Erro tentativa {tent}: {e}")
                    tent += 1
                    if tent <= args.attempts:
                        time.sleep(args.sleep_retry)

            if not (cia or valor_total):
                vazio_cnt += 1
                logging.info("  >> Sem dados após tentativas. Registrando linha vazia (Sem Ofertas).")
            else:
                ok_cnt += 1

            results.append({
                "captura_data": captura_data,
                "captura_hora": captura_hora,
                "grupo":        grupo,
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
            logging.info(f"[{i}/{len(selected)}] FIM combo | Duração {dt:.1f}s | OK_acum={ok_cnt} | Vazio={vazio_cnt} | Erros={erros_cnt}")

    except Exception as e:
        erros_cnt += 1
        logging.error(f"[FATAL] Erro geral de execução: {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # DataFrame e limpeza
    df = pd.DataFrame(results)
    base_cols = ["captura_data", "captura_hora", "grupo", "trecho", "antecedencia", "data_voo"]
    rest = [c for c in df.columns if c not in base_cols]
    if not df.empty:
        df = df[base_cols + rest]
        for col in ["por_adulto", "taxa_embarque", "taxa_servico", "valor_total"]:
            df[col] = df[col].apply(_parse_money).round(2)
    else:
        logging.warning("Nenhum resultado coletado neste run.")

    # Persistência: sempre XLS com nome CAPO_G{N}_YYYYMMDD_HHMMSS.xls em data/
    root = Path(__file__).resolve().parent
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    iter_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    xls_path = data_dir / f"CAPO_{grupo}_{iter_ts}.xls"

    try:
        # exige xlwt
        with pd.ExcelWriter(xls_path, engine="xlwt") as wr:
            df.to_excel(wr, sheet_name="BUSCAS", index=False)
        logging.info(f"[SAVE] XLS: {xls_path}")
    except Exception as e:
        logging.error(f"[ERRO] Falha ao salvar XLS (instale xlwt): {e}")
        raise

    total_exec = time.time() - t0_run
    logging.info("==== RESUMO ====")
    logging.info(f"Combos executados: {len(selected)}")
    logging.info(f"OK: {ok_cnt} | Sem Ofertas: {vazio_cnt} | Erros: {erros_cnt}")
    logging.info(f"Duração total: {total_exec:.1f}s")
    logging.info("==== FIM ====")

    return xls_path

if __name__ == "__main__":
    run_once()
