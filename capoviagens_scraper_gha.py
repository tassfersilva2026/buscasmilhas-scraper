#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Capo Viagens — Scraper para GitHub Actions
- Headless (Chrome) e robusto p/ Actions
- 1 iteração por execução (sem loop infinito)
- Salva saída em data/CAPOVIAGENS_YYYYMMDD_HHMMSS.xlsx

Depêndencias: ver requirements.txt
"""

import os
import re
import time
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# fallback inteligente: usa chromedriver do setup-chrome se existir; senão, webdriver_manager
def _make_driver(wait_seconds: int = 20) -> tuple[webdriver.Chrome, WebDriverWait]:
    opts = ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--start-maximized")

    # Usa binários do action 'browser-actions/setup-chrome@v2' se presentes:
    chrome_bin = os.environ.get("GOOGLE_CHROME_SHIM") or os.environ.get("CHROME_BIN")
    chrome_driver_dir = os.environ.get("CHROMEWEBDRIVER")

    service = None
    if chrome_driver_dir:
        chromedriver_path = str(Path(chrome_driver_dir) / "chromedriver")
        service = Service(chromedriver_path)
    else:
        # Fallback: webdriver_manager
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())

    if chrome_bin:
        opts.binary_location = chrome_bin

    driver = webdriver.Chrome(service=service, options=opts)
    wait = WebDriverWait(driver, wait_seconds)
    return driver, wait


# ====================== CONFIG DO SCRAPE ======================
ADVP_LIST = [1, 3, 7, 14, 21, 30, 60, 90]
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
SITE_BASE = "https://www.capoviagens.com.br/voos/"

# XPaths fornecidos (mantidos):
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
    """
    Converte strings tipo 'R$ 1.234,56' -> 1234.56
    """
    if not s:
        return 0.0
    s2 = re.sub(r"[R$\s.]", "", s).replace(",", ".")
    try:
        return float(s2)
    except Exception:
        return 0.0


def run_once() -> Path:
    driver, wait = _make_driver(wait_seconds=20)

    results: list[dict] = []
    now = datetime.now()
    iter_ts = now.strftime("%Y%m%d_%H%M%S")
    captura_data = now.strftime("%Y-%m-%d")
    captura_hora = now.strftime("%H:%M:%S")

    try:
        for trecho in TRECHOS:
            orig, dest = trecho.split("-")
            for dias in INTERVALOS:
                target_date = datetime.today() + timedelta(days=dias)
                search_date_str = target_date.strftime("%Y-%m-%d")

                tent = 1
                print(f"[{trecho} | ADVP {dias}] {captura_hora} — iniciando…")
                while tent <= 3:
                    url = (
                        f"{SITE_BASE}"
                        f"?fromAirport={orig}&toAirport={dest}"
                        f"&departureDate={search_date_str}"
                        f"&adult=1&child=0&cabin=Basic&isTwoWays=false"
                    )
                    driver.get(url)

                    cia           = _capturar(wait, XPATH["cia"])
                    hr_ida        = _capturar(wait, XPATH["hr_ida"])
                    hr_volta      = _capturar(wait, XPATH["hr_volta"])
                    por_adulto    = _capturar(wait, XPATH["por_adulto"])
                    taxa_embarque = _capturar(wait, XPATH["taxa_embarque"])
                    taxa_servico  = _capturar(wait, XPATH["taxa_servico"])
                    valor_total   = _capturar(wait, XPATH["valor_total"])

                    if cia or valor_total:
                        break  # conseguiu dados
                    tent += 1
                    print("  Sem dados visíveis. Nova tentativa em 5s…")
                    time.sleep(5)

                # Número do voo (opcional)
                num_voo = ""
                try:
                    btn = wait.until(EC.element_to_be_clickable((By.XPATH, XPATH["buy_button"])))
                    btn.click()
                    time.sleep(2)
                    num_voo = _capturar(wait, XPATH["flight_num"], cond=EC.presence_of_element_located)
                except Exception:
                    pass

                print(f"  -> cia={cia} ida={hr_ida} volta={hr_volta} total={valor_total} voo={num_voo}")

                results.append({
                    "captura_data": captura_data,
                    "captura_hora": captura_hora,
                    "trecho": trecho,
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
    finally:
        driver.quit()

    df = pd.DataFrame(results)

    # Ordena colunas
    base_cols = ["captura_data", "captura_hora", "trecho", "antecedencia", "data_voo"]
    rest = [c for c in df.columns if c not in base_cols]
    df = df[base_cols + rest]

    # Converte monetários
    for col in ["por_adulto", "taxa_embarque", "taxa_servico", "valor_total"]:
        df[col] = df[col].apply(_parse_money)

    # Garante pasta data/
    root = Path(__file__).resolve().parent
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    out_path = data_dir / f"CAPOVIAGENS_{iter_ts}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as wr:
        df.to_excel(wr, index=False)

    print(f"OK: arquivo gerado em {out_path}")
    return out_path


if __name__ == "__main__":
    run_once()

