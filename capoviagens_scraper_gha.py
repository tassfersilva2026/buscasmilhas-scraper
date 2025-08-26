# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, time, tempfile, shutil, argparse, logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

import pandas as pd
import pytz

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# ============================= CONFIG GERAL =============================
VERSION = "capo-profile-clone-v1.0"
TZ = pytz.timezone("America/Sao_Paulo")

# Trechos/ADVP solicitados
TRECHOS = ["CNF-GRU", "REC-CGH", "POA-GRU"]
ADVP_LIST = [30,60]  # 1 = amanhã, 5 = +5 dias

# URL da Capo
BASE_URL = ("https://www.capoviagens.com.br/voos/"
            "?fromAirport={orig}&toAirport={dest}&departureDate={yyyy_mm_dd}"
            "&adult=1&child=0&cabin=Basic&isTwoWays=false")

# XPaths (primeiro card de resultado)
X_HORA_PARTIDA = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label/label/div/div/div[1]/span[1]'
X_HORA_CHEGADA = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label/label/div/div/div[3]/span[1]'
X_TARIFA       = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[1]/div/span[1]'
X_TX_EMBARQUE  = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[4]/span[2]'
X_TOTAL        = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[6]/span'
X_CIA          = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label/div[1]/div/span'

# Excel de saída
OUT_DIR  = r"C:\Users\tassiana.silva\Documents\ARQUIVO\DECOLAR"
OUT_FILE = os.path.join(OUT_DIR, "CAPO_BUSCAS.xlsx")
SHEET    = "BUSCAS"

# Perfil a clonar (igual ao anexo)
USE_PROFILE_CLONE = True
ORIG_PROFILE = r"C:\Users\tassiana.silva\AppData\Local\Google\Chrome\User Data\Profile 1"
SKIP_DIRS = {
    "Extensions","Local Extension Settings","Sync Data","Safe Browsing",
    "OptimizationGuide","Webstore Downloads","TransportSecurity",
    "Code Cache","GrShaderCache","ShaderCache","Service Worker",
    "BudgetDatabase","File System","GPUCache","Partition Metrics",
    "PlatformNotifications","Reporting and NEL","AutofillStrikeDatabase"
}
SKIP_FILES = {
    "Login Data","Login Data For Account","Web Data","Affiliation Database",
    "Network Persistent State","CertificateTransparency","QuotaManager",
    "QuotaManager-journal"
}

HEADLESS = False        # mude com --headless
PAGE_TIMEOUT = 50
RETRY_LOADS  = 1

# ============================= UTILS =============================
def now_sp(): return datetime.now(TZ)
def fmt_br_date(d: datetime) -> str: return d.strftime("%d/%m/%Y")
def fmt_sp_time(d: datetime) -> str: return d.strftime("%H:%M:%S")

def parse_money_br_to_float(txt: str) -> float:
    if not txt: return float('nan')
    raw = "".join(ch for ch in txt if ch.isdigit() or ch in ".,")
    raw = raw.replace(".", "").replace(",", ".")
    try: return float(raw)
    except Exception: return float('nan')

def wait_visible(driver, xpath, timeout=PAGE_TIMEOUT):
    WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((By.XPATH, xpath)))

def wait_non_empty_text(driver, xpath, timeout=20):
    WebDriverWait(driver, timeout).until(
        lambda d: (el := d.find_element(By.XPATH, xpath)) and el.text.strip() != ""
    )

def text_or(driver, xpath, default=""):
    try: return driver.find_element(By.XPATH, xpath).text.strip()
    except Exception: return default

def build_url(trecho: str, advp_days: int) -> Dict[str, Any]:
    orig, dest = trecho.split("-")
    dep_date = now_sp().date() + timedelta(days=advp_days)
    return {"url": BASE_URL.format(orig=orig, dest=dest, yyyy_mm_dd=dep_date.strftime("%Y-%m-%d")),
            "date_api": dep_date}

# ===================== CLONE DE PERFIL (do anexo, adaptado) =====================
def clone_profile_profile1(download_dir: str):
    tmp_dir = tempfile.mkdtemp(prefix="chrome_profile_clone_")
    CLONE_ROOT = os.path.join(tmp_dir, "Profile 1")
    os.makedirs(CLONE_ROOT, exist_ok=True)

    for root, dirs, files in os.walk(ORIG_PROFILE):
        rel = os.path.relpath(root, ORIG_PROFILE)
        # pula subpastas ruidosas
        if any(part in SKIP_DIRS for part in rel.replace("\\", "/").split("/")):
            continue
        dst_dir = os.path.join(CLONE_ROOT, rel)
        os.makedirs(dst_dir, exist_ok=True)
        for fn in files:
            if fn in SKIP_FILES:
                continue
            try:
                shutil.copy2(os.path.join(root, fn), os.path.join(dst_dir, fn))
            except Exception:
                pass
    return tmp_dir

def setup_driver(headless=False, use_clone=True, download_dir=None):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--hide-scrollbars")

    # Estabilidade/ruído baixo
    options.add_argument("--disable-gpu")
    options.add_argument("--use-gl=swiftshader")
    options.add_argument("--enable-unsafe-swiftshader")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--log-level=3")
    options.add_argument("--mute-audio")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-component-update")
    options.add_argument("--lang=pt-BR")

    # Clona perfil (igual ao padrão do anexo)
    tmp_dir = None
    if use_clone and os.path.isdir(ORIG_PROFILE):
        tmp_dir = clone_profile_profile1(download_dir or os.getcwd())
        options.add_argument(f"--user-data-dir={tmp_dir}")
        options.add_argument("--profile-directory=Profile 1")
        logging.info("Perfil clonado em: %s\\Profile 1", tmp_dir)
    elif use_clone:
        logging.warning("Perfil original não encontrado: %s. Seguindo sem clone.", ORIG_PROFILE)

    options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")

    prefs = {
        "download.default_directory": download_dir or os.getcwd(),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    try:
        service.log_output = open(os.devnull, "w")
    except Exception:
        pass

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGE_TIMEOUT)
    return driver, WebDriverWait(driver, 15), tmp_dir

# ============================= EXCEL =============================
def save_rows_to_excel(rows: List[Dict[str, Any]]):
    os.makedirs(OUT_DIR, exist_ok=True)
    df = pd.DataFrame(rows, columns=[
        "DATA DA BUSCA","HORA DA BUSCA","TRECHO",
        "DATA PARTIDA","HORA DA PARTIDA",
        "DATA CHEGADA","HORA DA CHEGADA",
        "TARIFA","TX DE EMBARQUE","TOTAL",
        "CIA DO VOO","URL","STATUS"
    ])

    if os.path.exists(OUT_FILE):
        # append
        old = pd.read_excel(OUT_FILE, sheet_name=SHEET)
        df = pd.concat([old, df], ignore_index=True)

    with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=SHEET)
        ws = w.sheets[SHEET]
        # formatos BR
        col_idx = {c:i+1 for i,c in enumerate(df.columns)}
        fmt_br = '#.##0,00'
        for name in ["TARIFA","TX DE EMBARQUE","TOTAL"]:
            j = col_idx[name]
            for r in range(2, len(df)+2):
                cell = ws.cell(row=r, column=j)
                try:
                    float(cell.value)
                    cell.number_format = fmt_br
                except Exception:
                    pass
        widths = {"A":15,"B":12,"C":10,"D":14,"E":12,"F":14,"G":12,"H":14,"I":16,"J":14,"K":18,"L":60,"M":12}
        for col,wid in widths.items():
            if col in ws.column_dimensions:
                ws.column_dimensions[col].width = wid

# ============================= SCRAPER =============================
def accept_cookies_if_any(driver):
    try:
        for xp in [
            "//button[contains(.,'Aceitar') or contains(.,'Concordo')]",
            "//button[contains(.,'Continuar') or contains(.,'Prosseguir')]",
            "//button[contains(.,'Accept') or contains(.,'Agree')]",
        ]:
            els = driver.find_elements(By.XPATH, xp)
            if els:
                driver.execute_script("arguments[0].click();", els[0])
                time.sleep(0.5)
                break
    except Exception:
        pass

def scrape_once(driver, trecho: str, advp: int) -> Dict[str, Any]:
    info = build_url(trecho, advp)
    url = info["url"]; dep_date = info["date_api"]

    dt_busca = now_sp(); data_busca_br = fmt_br_date(dt_busca); hora_busca_sp = fmt_sp_time(dt_busca)
    print(f"\n[{data_busca_br} {hora_busca_sp}] Trecho {trecho} | ADVP {advp}d → {url}")

    for attempt in range(1 + RETRY_LOADS):
        try:
            driver.get(url)
            accept_cookies_if_any(driver)
            wait_visible(driver, X_CIA, timeout=PAGE_TIMEOUT)
            # garante texto carregado de verdade
            for xp in (X_HORA_PARTIDA, X_HORA_CHEGADA, X_TARIFA, X_TX_EMBARQUE, X_TOTAL):
                wait_non_empty_text(driver, xp, timeout=20)
            break
        except Exception as e:
            if attempt < RETRY_LOADS:
                print(f"  - Tentativa {attempt} falhou ({type(e).__name__}). Retentando…")
                time.sleep(2)
            else:
                print(f"  - Falha ao carregar página: {e}")
                return {
                    "DATA DA BUSCA": data_busca_br,
                    "HORA DA BUSCA": hora_busca_sp,
                    "TRECHO": trecho,
                    "DATA PARTIDA": dep_date.strftime("%d/%m/%Y"),
                    "HORA DA PARTIDA": "",
                    "DATA CHEGADA": dep_date.strftime("%d/%m/%Y"),
                    "HORA DA CHEGADA": "",
                    "TARIFA": float('nan'),
                    "TX DE EMBARQUE": float('nan'),
                    "TOTAL": float('nan'),
                    "CIA DO VOO": "",
                    "URL": url,
                    "STATUS": "ERRO_CARREGAR"
                }

    # captura
    txt_partida = text_or(driver, X_HORA_PARTIDA)
    txt_chegada = text_or(driver, X_HORA_CHEGADA)
    txt_tarifa  = text_or(driver, X_TARIFA)
    txt_tx_emb  = text_or(driver, X_TX_EMBARQUE)
    txt_total   = text_or(driver, X_TOTAL)
    txt_cia     = text_or(driver, X_CIA)

    hora_partida = (txt_partida + ":00") if txt_partida and len(txt_partida) == 5 else txt_partida
    hora_chegada = (txt_chegada + ":00") if txt_chegada and len(txt_chegada) == 5 else txt_chegada
    tarifa = parse_money_br_to_float(txt_tarifa)
    tx_emb = parse_money_br_to_float(txt_tx_emb)
    total  = parse_money_br_to_float(txt_total)

    data_partida_br = dep_date.strftime("%d/%m/%Y")
    data_chegada_br = data_partida_br

    ok_campos = all([hora_partida, hora_chegada, not pd.isna(tarifa), not pd.isna(tx_emb), not pd.isna(total), bool(txt_cia)])
    status = "OK" if ok_campos else "INCOMPLETO"

    print(f"  CIA: {txt_cia or '-'}")
    print(f"  Partida: {hora_partida or '-'} | Chegada: {hora_chegada or '-'}")
    print(f"  Tarifa: {txt_tarifa or '-'} | Tx Embarque: {txt_tx_emb or '-'} | Total: {txt_total or '-'}")
    print("  ✅ Captura OK" if ok_campos else "  ⚠️  Captura incompleta — STATUS=INCOMPLETO")

    return {
        "DATA DA BUSCA": data_busca_br,
        "HORA DA BUSCA": hora_busca_sp,
        "TRECHO": trecho,
        "DATA PARTIDA": data_partida_br,
        "HORA DA PARTIDA": hora_partida,
        "DATA CHEGADA": data_chegada_br,
        "HORA DA CHEGADA": hora_chegada,
        "TARIFA": tarifa,
        "TX DE EMBARQUE": tx_emb,
        "TOTAL": total,
        "CIA DO VOO": txt_cia,
        "URL": url,
        "STATUS": status
    }

# ============================= MAIN =============================
def main():
    parser = argparse.ArgumentParser(description="Capo Viagens scraper (com clone de perfil).")
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--headless", dest="headless", action="store_true", help="Força modo invisível")
    g.add_argument("--gui",      dest="headless", action="store_false", help="Modo visível (default)")
    parser.set_defaults(headless=HEADLESS)

    parser.add_argument("--no-clone", action="store_true", help="Não clonar Profile 1 (usa perfil limpo)")
    args = parser.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
    logging.info("Versão: %s | Headless: %s | Saída: %s", VERSION, args.headless, OUT_FILE)

    driver, wait, tmp_dir = setup_driver(
        headless=args.headless,
        use_clone=(USE_PROFILE_CLONE and not args.no_clone),
        download_dir=OUT_DIR
    )

    print("== CAPO — Coleta iniciada ==")
    start = now_sp()
    print(f"Início: {fmt_br_date(start)} {fmt_sp_time(start)} (America/Sao_Paulo)")

    rows: List[Dict[str, Any]] = []
    try:
        for trecho in TRECHOS:
            for advp in ADVP_LIST:
                row = scrape_once(driver, trecho, advp)
                rows.append(row)
                time.sleep(1.0)
    finally:
        try: driver.quit()
        except Exception: pass
        # limpa clone
        if USE_PROFILE_CLONE and tmp_dir and os.path.isdir(tmp_dir):
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                logging.info("Clone temporário removido: %s", tmp_dir)
            except Exception:
                pass

    save_rows_to_excel(rows)
    end = now_sp()
    print(f"\nConcluído: {fmt_br_date(end)} {fmt_sp_time(end)} (America/Sao_Paulo)")
    print(f"Total de linhas: {len(rows)}")
    print(f"Excel salvo em: {OUT_FILE}")
    print("== CAPO — Coleta finalizada ==")

if __name__ == "__main__":
    main()
