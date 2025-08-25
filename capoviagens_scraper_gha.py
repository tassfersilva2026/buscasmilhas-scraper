# capoviagens_scraper_gha.py — versão p/ GitHub Actions (1 ciclo + headless)
# Saída: output/CAPOVIAGENS.xlsx (aba "BUSCAS")
# Execução típica (local): python capoviagens_scraper_gha.py --headless --once
# Execução no GitHub Actions: igual ao flip, basta trocar o arquivo no job

import os, re, time, argparse, logging
from datetime import datetime, timedelta, date, time as dtime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
from zipfile import ZipFile

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment

# ===== Selenium (robusto p/ CI) =====
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# ======= CONFIG PADRÃO =======
TZ = ZoneInfo("America/Sao_Paulo")
SHEET_NAME = "BUSCAS"

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

# ======= CAPO VIAGENS =======
def build_url(origin: str, destiny: str, departure_date: str) -> str:
    # ida simples
    return ("https://www.capoviagens.com.br/voos/"
            f"?fromAirport={origin}&toAirport={destiny}&departureDate={departure_date}"
            "&adult=1&child=0&cabin=Basic&isTwoWays=false")

# XPaths do 1º card (enviados por você)
XPATH_HORA_PARTIDA = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label/label/div/div/div[1]/span[1]'
XPATH_HORA_CHEGADA = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label/label/div/div/div[3]/span[1]'
XPATH_TARIFA       = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[1]/div/span[1]'
XPATH_TX_EMBARQUE  = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[4]/span[2]'
XPATH_TOTAL        = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[6]/span'
XPATH_CIA          = '//*[@id="__next"]/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label/div[1]/div/span'

CENTER = Alignment(horizontal="center", vertical="center")
HEADERS = [
    "DATA DA BUSCA","HORA DA BUSCA","TRECHO",
    "DATA PARTIDA","HORA DA PARTIDA","DATA CHEGADA","HORA DA CHEGADA",
    "TARIFA","TX DE EMBARQUE","TOTAL","CIA DO VOO",
]

# ======= Helpers =======
def brl_to_decimal(txt: str):
    if not txt: return None
    s = re.sub(r"[^\d,\.]", "", txt).replace(".", "").replace(",", ".")
    try: return Decimal(s)
    except (InvalidOperation, TypeError): return None

def parse_time_hhmm(txt: str, fallback_date: date | None) -> datetime | None:
    # Converte "HH:MM" (ou "HH:MM:SS") para datetime no fuso TZ usando fallback_date
    if not txt or not fallback_date: return None
    txt = txt.strip()
    m = re.fullmatch(r"(\d{2}):(\d{2})(?::(\d{2}))?", txt)
    if not m: return None
    hh, mm, ss = m.groups()
    ss = ss or "00"
    try:
        return datetime(fallback_date.year, fallback_date.month, fallback_date.day,
                        int(hh), int(mm), int(ss), tzinfo=TZ)
    except ValueError:
        return None

def clean_cia_text(txt: str | None) -> str:
    if not txt: return ""
    s = txt.strip()
    s = re.sub(r'(?i)\blinhas a[eé]reas\b(?:\s+(s\.?\s*/?\s*a\.?)\b)?', '', s)
    s = re.sub(r'\s{2,}', ' ', s).strip(" -–,.;:/\t\n\r")
    return s

def to_excel_naive(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, dtime):
        return value.replace(tzinfo=None)
    return value

# ======= Excel =======
def _is_valid_xlsx(path: str) -> bool:
    try:
        if not os.path.isfile(path): return False
        with ZipFile(path, "r") as zf:
            return "[Content_Types].xml" in zf.namelist()
    except Exception:
        return False

def _create_new_workbook(path: str):
    wb = Workbook()
    ws = wb.active
    ws.title = SHEET_NAME
    ws.append(HEADERS)
    widths = {
        "DATA DA BUSCA": 14, "HORA DA BUSCA": 12, "TRECHO": 12,
        "DATA PARTIDA": 14, "HORA DA PARTIDA": 14,
        "DATA CHEGADA": 14, "HORA DA CHEGADA": 14,
        "TARIFA": 14, "TX DE EMBARQUE": 16, "TOTAL": 14, "CIA DO VOO": 20,
    }
    for j, hdr in enumerate(HEADERS, start=1):
        ws.column_dimensions[get_column_letter(j)].width = widths.get(hdr, 16)
        ws.cell(row=1, column=j).alignment = CENTER
    wb.save(path); wb.close()

def ensure_workbook(path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path) or not _is_valid_xlsx(path):
        _create_new_workbook(path); return
    wb = load_workbook(path)
    if SHEET_NAME not in wb.sheetnames:
        ws = wb.create_sheet(SHEET_NAME)
        ws.append(HEADERS)
        for j in range(1, len(HEADERS)+1):
            ws.cell(row=1, column=j).alignment = CENTER
        wb.save(path)
    wb.close()

def append_row(path: str, row_values: dict):
    ensure_workbook(path)
    wb = load_workbook(path)
    ws = wb[SHEET_NAME]
    if ws.max_row == 1 and all((c.value is None for c in ws[1])):
        ws.append(HEADERS)
        for j in range(1, len(HEADERS)+1):
            ws.cell(row=1, column=j).alignment = CENTER

    clean = {k: to_excel_naive(v) for k, v in row_values.items()}
    row = [clean.get(h) for h in HEADERS]
    ws.append(row)
    r = ws.max_row

    fmt = {
        "DATA DA BUSCA": "DD/MM/YYYY",
        "HORA DA BUSCA": "HH:MM:SS",
        "DATA PARTIDA": "DD/MM/YYYY",
        "HORA DA PARTIDA": "HH:MM:SS",
        "DATA CHEGADA": "DD/MM/YYYY",
        "HORA DA CHEGADA": "HH:MM:SS",
        "TARIFA": "#,##0.00",
        "TX DE EMBARQUE": "#,##0.00",
        "TOTAL": "#,##0.00",
    }
    for c_idx, hdr in enumerate(HEADERS, start=1):
        cell = ws.cell(row=r, column=c_idx)
        if hdr in fmt and cell.value is not None:
            cell.number_format = fmt[hdr]
        cell.alignment = Alignment(horizontal="center", vertical="center")

    wb.save(path)
    wb.close()

# ======= Selenium =======
def _maybe_set_binary_location(opts: Options):
    """Usa CHROME_PATH do GitHub Actions se existir; caso contrário Selenium Manager resolve."""
    chrome_path = os.getenv("CHROME_PATH") or os.getenv("GOOGLE_CHROME_SHIM")
    if chrome_path and os.path.exists(chrome_path):
        opts.binary_location = chrome_path
        logging.info("Usando CHROME_PATH: %s", chrome_path)

def setup_driver(headless: bool = True):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--hide-scrollbars")
    _maybe_set_binary_location(options)

    # Flags estáveis no CI
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--log-level=3")
    options.add_argument("--mute-audio")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-notifications")
    options.add_argument("--lang=pt-BR")
    options.add_argument("--use-gl=swiftshader")
    options.add_argument("--enable-unsafe-swiftshader")

    driver_path = os.getenv("CHROMEDRIVER_PATH")
    service = ChromeService(executable_path=driver_path) if driver_path and os.path.exists(driver_path) else ChromeService()
    driver = webdriver.Chrome(service=service, options=options)
    logging.info("Chrome binary: %s | chromedriver: %s", getattr(options, "binary_location", None), (driver_path or "selenium-manager"))
    wait = WebDriverWait(driver, 15)
    return driver, wait

def navigate_same_tab(driver, url):
    try:
        driver.execute_script("window.location.assign(arguments[0]);", url)
    except Exception:
        driver.get(url)

def wait_any_text(driver, xpaths, max_wait):
    """Espera até algum dos XPaths ter texto (Capo 1º card)."""
    start = time.time()
    while time.time() - start < max_wait:
        for xp in xpaths:
            try:
                el = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, xp)))
                if (el.text or "").strip():
                    return "ready"
            except TimeoutException:
                pass
        time.sleep(1)
    return "timeout"

def get_text_or_none(driver, xpath, tries=3, delay=2):
    for _ in range(tries):
        try:
            el = WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.XPATH, xpath)))
            txt = (el.text or "").strip()
            if txt:
                return txt
        except TimeoutException:
            pass
        time.sleep(delay)
    return None

# ======= 1 passo (trecho+ADVP) =======
def processar_trecho_advp(driver, base_tab, out_path, origin, destiny, advp, espera):
    data_voo = (datetime.now(TZ) + timedelta(days=advp)).date()
    url = build_url(origin, destiny, data_voo.strftime("%Y-%m-%d"))
    trecho_str = f"{origin}-{destiny}"

    try: driver.switch_to.window(base_tab)
    except Exception: base_tab = driver.current_window_handle
    navigate_same_tab(driver, url)

    # Espera o 1º card ter conteúdo (qualquer um dos campos com texto)
    status = wait_any_text(driver,
                           [XPATH_CIA, XPATH_TARIFA, XPATH_HORA_PARTIDA, XPATH_TOTAL],
                           max_wait=espera)

    now = datetime.now(TZ)

    if status == "timeout":
        row = {
            "DATA DA BUSCA": now.date(),
            "HORA DA BUSCA": dtime(now.hour, now.minute, now.second),
            "TRECHO": trecho_str,
            "DATA PARTIDA": None,
            "HORA DA PARTIDA": None,
            "DATA CHEGADA": None,
            "HORA DA CHEGADA": None,
            "TARIFA": None,
            "TX DE EMBARQUE": None,
            "TOTAL": None,
            "CIA DO VOO": "Sem Ofertas",
        }
        append_row(out_path, row)
        return base_tab

    # Extrai textos do 1º card
    partida_txt = get_text_or_none(driver, XPATH_HORA_PARTIDA)
    chegada_txt = get_text_or_none(driver, XPATH_HORA_CHEGADA)
    tarifa_txt  = get_text_or_none(driver, XPATH_TARIFA)
    taxa_txt    = get_text_or_none(driver, XPATH_TX_EMBARQUE)
    total_txt   = get_text_or_none(driver, XPATH_TOTAL)
    cia_txt     = get_text_or_none(driver, XPATH_CIA)

    partida_dt = parse_time_hhmm(partida_txt, fallback_date=data_voo) if partida_txt else None
    chegada_dt = parse_time_hhmm(chegada_txt, fallback_date=data_voo) if chegada_txt else None
    tarifa_val = brl_to_decimal(tarifa_txt) if tarifa_txt else None
    taxa_val   = brl_to_decimal(taxa_txt)   if taxa_txt   else None
    total_val  = brl_to_decimal(total_txt)  if total_txt  else None
    if total_val is None and (tarifa_val is not None or taxa_val is not None):
        total_val = (tarifa_val or Decimal(0)) + (taxa_val or Decimal(0))

    data_partida = partida_dt.date() if partida_dt else data_voo
    hora_partida = partida_dt.time() if partida_dt else None
    data_chegada = chegada_dt.date() if chegada_dt else data_voo
    hora_chegada = chegada_dt.time() if chegada_dt else None

    cia_clean = clean_cia_text(cia_txt) if cia_txt else ("Sem Ofertas" if (tarifa_val is None and taxa_val is None and total_val is None) else "")

    row = {
        "DATA DA BUSCA": now.date(),
        "HORA DA BUSCA": dtime(now.hour, now.minute, now.second),
        "TRECHO": trecho_str,
        "DATA PARTIDA": data_partida,
        "HORA DA PARTIDA": hora_partida,
        "DATA CHEGADA": data_chegada,
        "HORA DA CHEGADA": hora_chegada,
        "TARIFA": float(tarifa_val) if tarifa_val is not None else None,
        "TX DE EMBARQUE": float(taxa_val) if taxa_val is not None else None,
        "TOTAL": float(total_val) if total_val is not None else None,
        "CIA DO VOO": cia_clean,
    }
    append_row(out_path, row)
    return base_tab

# ======= MAIN =======
def main():
    parser = argparse.ArgumentParser(description="Capo Viagens scraper p/ GitHub Actions (1 ciclo opcional).")
    parser.add_argument("--saida",    default="output", help="Pasta para salvar Excel")
    parser.add_argument("--file",     default="CAPOVIAGENS.xlsx", help="Nome do arquivo Excel")
    parser.add_argument("--espera",   type=int, default=20, help="Segundos máx para aparecer o 1º card")
    parser.add_argument("--headless", action="store_true", help="Força headless")
    parser.add_argument("--gui",      dest="headless", action="store_false", help="Abre janela (debug local)")
    parser.add_argument("--once",     action="store_true", help="Roda apenas 1 ciclo e finaliza")
    parser.set_defaults(headless=True)  # no GitHub: headless por padrão
    args = parser.parse_args()

    os.makedirs(args.saida, exist_ok=True)
    out_path = os.path.join(args.saida, args.file)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s",
                        datefmt="%H:%M:%S")
    logging.info("Saída: %s | Planilha: %s | Aba: %s | Headless: %s", args.saida, args.file, SHEET_NAME, args.headless)

    driver, _wait = setup_driver(headless=args.headless)
    base_tab = driver.current_window_handle

    try:
        def ciclo():
            nonlocal base_tab
            for (origin, destiny) in TRECHOS:
                for advp in ADVP_LIST:
                    try:
                        base_tab = processar_trecho_advp(
                            driver=driver, base_tab=base_tab, out_path=out_path,
                            origin=origin, destiny=destiny, advp=advp, espera=args.espera
                        )
                    except Exception as e:
                        logging.exception("Falha em %s-%s ADVP %d: %s", origin, destiny, advp, e)

        if args.once:
            ciclo()
        else:
            while True:
                ciclo()
                logging.info("Ciclo terminado. Aguardando 5 minutos…")
                time.sleep(300)

    finally:
        try: driver.quit()
        except Exception: pass
        logging.info("Finalizado.")

if __name__ == "__main__":
    main()
