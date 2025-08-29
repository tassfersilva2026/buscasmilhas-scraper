#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Capo Viagens — Scraper para GitHub Actions (headless)
- Hard-timeout por busca (default 60s)
- Checagem "Não encontramos voo" aos 30s
- Fallback por texto visível (innerText) se XPath quebrar
- Datas/Horas como tipos reais no Excel (sem texto)
- Timezone: America/Sao_Paulo
- Saída: <out_dir>/CAPO_<YYYYMMDD_HHMMSS>.xlsx

Uso (Actions):
  python -u scripts/capoviagens_scraper_gha.py \
    --out-dir data/G1 \
    --trechos "CGH-SDU,SDU-CGH,GRU-POA,POA-GRU" \
    --advps "1,5,11,17,30" \
    --timeout 60 --check-no-results 30 --poll 1 --headless
"""

import os, re, time, argparse
from datetime import datetime, timedelta, date, time as dtime
from typing import Tuple, List, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# ============================== Defaults ==============================
TZ = ZoneInfo("America/Sao_Paulo")
DEFAULT_ADVP = [1, 3, 7, 14, 21, 30, 60, 90]
DEFAULT_TRECHOS = [
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

# ============================== XPaths ==============================
X_CIA           = "//*[@id='__next']/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label/div[1]/div/span"
X_HR_PARTIDA    = "//*[@id='__next']/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label/label/div/div/div[1]/span[1]"
X_HR_CHEGADA    = "//*[@id='__next']/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[1]/div[1]/label/label/div/div/div[3]/span[1]"
X_TARIFA        = "//*[@id='__next']/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[1]/div/span[1]"
X_TX_EMBARQUE   = "//*[@id='__next']/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[4]/span[2]"
X_TX_SERVICO    = "//*[@id='__next']/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[5]/span[2]"
X_TOTAL         = "//*[@id='__next']/div[4]/div[5]/div/main/div[2]/div/div[1]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[6]/span"
X_CIA_FALLBACK  = "//span[contains(@style,'font-size') and normalize-space(text())!='' and string-length(normalize-space(text()))<=10]"
X_NO_RESULTS_H1 = "//*[@id='__next']/div[4]/div[1]/div/div/div/h1"

# ============================== Utilitários ==============================
CURRENCY_RE = re.compile(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}")
TIME_RE     = re.compile(r"\b(\d{1,2}):(\d{2})(?::(\d{2}))?\b")
CIAS_LIST   = ["AZUL", "LATAM", "GOL", "VOEPASS", "PASSAREDO"]

def _parse_trechos_csv(s: Optional[str]) -> List[Tuple[str, str]]:
    if not s:
        return DEFAULT_TRECHOS
    pairs = []
    for part in s.split(","):
        part = part.strip()
        if not part or "-" not in part:
            continue
        o, d = part.split("-", 1)
        pairs.append((o.strip().upper(), d.strip().upper()))
    return pairs or DEFAULT_TRECHOS

def _parse_advp_csv(s: Optional[str]) -> List[int]:
    if not s:
        return DEFAULT_ADVP
    out = []
    for x in s.split(","):
        x = x.strip()
        if x.isdigit():
            out.append(int(x))
    return out or DEFAULT_ADVP

def _mk_driver(headless: bool, pageload_timeout: int) -> webdriver.Chrome:
    opts = Options()
    if headless or os.environ.get("CI", "").lower() == "true":
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--start-maximized")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    # DOM básico sem esperar tudo
    opts.set_capability("pageLoadStrategy", "eager")

    exe = os.environ.get("CHROMEDRIVER_PATH")
    service = Service(executable_path=exe) if exe else Service()

    driver = webdriver.Chrome(service=service, options=opts)
    try:
        driver.set_page_load_timeout(int(pageload_timeout))
    except Exception:
        driver.set_page_load_timeout(25)

    # Anti-detecção simples
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
        )
    except Exception:
        pass
    return driver

def _find_text(driver: webdriver.Chrome, xpath: str) -> str:
    try:
        els = driver.find_elements(By.XPATH, xpath)
        if els:
            return (els[0].text or "").strip()
        return ""
    except Exception:
        return ""

def _money_to_float(txt: str) -> float:
    if not txt:
        return 0.0
    t = re.sub(r"[^0-9,.-]", "", txt).replace(".", "").replace(",", ".")
    try:
        return round(float(t), 2)
    except Exception:
        return 0.0

def _norm_hhmmss(txt: str) -> str:
    s = (txt or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$", s)
    if m:
        hh, mm, ss = m.group(1), m.group(2), m.group(3) or "00"
        return f"{int(hh):02d}:{int(mm):02d}:{int(ss):02d}"
    return ""

def _to_timeobj(s: str) -> Optional[dtime]:
    if not s:
        return None
    try:
        hh, mm, ss = s.split(":")
        return dtime(int(hh), int(mm), int(ss))
    except Exception:
        return None

def _dismiss_cookies(driver: webdriver.Chrome):
    try:
        el = driver.find_elements(By.CSS_SELECTOR, "button#onetrust-accept-btn-handler")
        if el:
            el[0].click(); return
    except Exception:
        pass
    xps = [
        "//button[contains(translate(., 'ACEITAROKENTENDI', 'aceitarokentendi'),'aceitar')]",
        "//button[contains(translate(., 'ACEITAROKENTENDI', 'aceitarokentendi'),'ok')]",
        "//button[contains(translate(., 'ACEITAROKENTENDI', 'aceitarokentendi'),'entendi')]",
    ]
    for xp in xps:
        try:
            el = driver.find_elements(By.XPATH, xp)
            if el:
                el[0].click(); return
        except Exception:
            continue

def _tem_no_results(driver: webdriver.Chrome) -> bool:
    try:
        els = driver.find_elements(By.XPATH, X_NO_RESULTS_H1)
        if not els:
            return False
        txt = (els[0].text or "").strip().lower()
        return ("não encontramos" in txt) and ("voo" in txt or "result" in txt)
    except Exception:
        return False

def _scrape_text_main(driver: webdriver.Chrome) -> str:
    try:
        return driver.execute_script("""
            const m = document.querySelector('main') || document.body;
            return (m.innerText || '').slice(0, 150000);
        """) or ""
    except Exception:
        return ""

def _fallback_parse(text: str) -> Dict[str, Optional[str]]:
    t = text.replace("\xa0", " ").strip()
    if not t:
        return {"cia": None, "h_ida": None, "h_volta": None, "tarifa": None, "tx_emb": None, "tx_serv": None, "total": None}
    up = t.upper()

    cia = None
    for c in ["AZUL", "LATAM", "GOL", "VOEPASS", "PASSAREDO"]:
        if c in up:
            cia = c.title(); break

    times = re.findall(r"\b(\d{1,2}):(\d{2})(?::(\d{2}))?\b", t)
    hhmm = []
    for h, m, s in times:
        h, m = int(h), int(m)
        if 0 <= h <= 23 and 0 <= m <= 59:
            hhmm.append(f"{h:02d}:{m:02d}:00")
    h_ida = hhmm[0] if hhmm else None
    h_volta = hhmm[1] if len(hhmm) > 1 else None

    prices = list(re.finditer(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", t))
    total = tarifa = tx_emb = tx_serv = None
    if prices:
        idx_total = up.find("TOTAL")
        if idx_total != -1:
            best, best_dist = None, 10**9
            for m in prices:
                d = abs(m.start() - idx_total)
                if d < best_dist:
                    best, best_dist = m, d
            total = best.group(0) if best else prices[0].group(0)
        else:
            total = prices[0].group(0)
        if len(prices) >= 2: tarifa = prices[0].group(0)
        if len(prices) >= 3: tx_emb = prices[1].group(0)
        if len(prices) >= 4: tx_serv = prices[2].group(0)

    return {"cia": cia, "h_ida": h_ida, "h_volta": h_volta, "tarifa": tarifa, "tx_emb": tx_emb, "tx_serv": tx_serv, "total": total}

def _money_str_to_float(s: Optional[str]) -> float:
    if not s:
        return 0.0
    t = re.sub(r"[^0-9,.-]", "", s).replace(".", "").replace(",", ".")
    try:
        return round(float(t), 2)
    except Exception:
        return 0.0

def _fmt_time_for_log(t: Optional[dtime]) -> str:
    return t.strftime("%H:%M:%S") if isinstance(t, dtime) else "-"

def _print_row_log(reg: Dict, motivo: str = ""):
    tag = f" [{motivo}]" if motivo else ""
    print(
        f"[CAPTURA{tag}] "
        f"DATA={reg['DATA DA BUSCA'].strftime('%d/%m/%Y')} "
        f"HORA={_fmt_time_for_log(reg['HORA DA BUSCA'])} "
        f"TRECHO={reg['TRECHO']} "
        f"DATA_PARTIDA={reg['DATA PARTIDA'].strftime('%d/%m/%Y')} "
        f"HR_PARTIDA={_fmt_time_for_log(reg['HORA DA PARTIDA'])} "
        f"HR_CHEGADA={_fmt_time_for_log(reg['HORA DA CHEGADA'])} "
        f"TARIFA={reg['TARIFA']:.2f} "
        f"TX_EMBARQUE={reg['TX DE EMBARQUE']:.2f} "
        f"TX_SERVICO={reg['TX DE SERVIÇO']:.2f} "
        f"TOTAL={reg['TOTAL']:.2f} "
        f"CIA={reg['CIA DO VOO']}",
        flush=True
    )

def _busca(driver: webdriver.Chrome, orig: str, dest: str, dias: int,
           timeout_per_search: int, check_no_results_at: int, poll_interval: int) -> Dict:
    agora = datetime.now(TZ)
    data_busca = agora.date()
    hora_busca = agora.time().replace(microsecond=0)

    data_partida_dt = datetime.now(TZ) + timedelta(days=dias)
    data_partida_url = data_partida_dt.strftime("%Y-%m-%d")
    data_partida = data_partida_dt.date()

    url = (
        f"https://www.capoviagens.com.br/voos/?fromAirport={orig}&toAirport={dest}"
        f"&departureDate={data_partida_url}&adult=1&child=0&cabin=Basic&isTwoWays=false"
    )

    try:
        driver.get(url)
    except Exception:
        pass

    start = time.time()
    motivo = ""
    _dismiss_cookies(driver)

    cia = ""
    hr_partida = None
    hr_chegada = None
    tarifa = tx_embarque = tx_servico = total = 0.0

    while True:
        elapsed = time.time() - start

        # 1) XPaths
        cia_txt        = _find_text(driver, X_CIA) or _find_text(driver, X_CIA_FALLBACK)
        hr_partida_txt = _find_text(driver, X_HR_PARTIDA)
        hr_chegada_txt = _find_text(driver, X_HR_CHEGADA)
        tarifa_txt     = _find_text(driver, X_TARIFA)
        tx_emb_txt     = _find_text(driver, X_TX_EMBARQUE)
        tx_serv_txt    = _find_text(driver, X_TX_SERVICO)
        total_txt      = _find_text(driver, X_TOTAL)

        cia_1        = (cia_txt or "").strip()
        part_s       = _norm_hhmmss(hr_partida_txt)
        cheg_s       = _norm_hhmmss(hr_chegada_txt)
        tarifa_1     = _money_to_float(tarifa_txt)
        tx_emb_1     = _money_to_float(tx_emb_txt)
        tx_serv_1    = _money_to_float(tx_serv_txt)
        total_1      = _money_to_float(total_txt)

        if cia_1 and total_1 > 0:
            cia = cia_1
            hr_partida = _to_timeobj(part_s)
            hr_chegada = _to_timeobj(cheg_s)
            tarifa, tx_embarque, tx_servico, total = tarifa_1, tx_emb_1, tx_serv_1, total_1
            motivo = "OK_XPATH"
            break

        # 2) Fallback texto
        t = _scrape_text_main(driver)
        if t:
            fb = _fallback_parse(t)
            total_fb = _money_str_to_float(fb.get("total"))
            if (fb.get("cia") and total_fb > 0) or (total_fb > 0):
                cia = (fb.get("cia") or "DESCONHECIDA").strip()
                hr_partida = _to_timeobj(_norm_hhmmss(fb.get("h_ida") or ""))
                hr_chegada = _to_timeobj(_norm_hhmmss(fb.get("h_volta") or ""))
                tarifa, tx_embarque, tx_servico, total = (
                    _money_str_to_float(fb.get("tarifa")),
                    _money_str_to_float(fb.get("tx_emb")),
                    _money_str_to_float(fb.get("tx_serv")),
                    total_fb
                )
                motivo = "OK_FALLBACK_TEXT"
                break

        # 3) "Não encontramos voo"
        if elapsed >= check_no_results_at and _tem_no_results(driver):
            cia = "SEM OFERTAS"
            motivo = "NO_RESULTS"
            break

        # 4) Hard-timeout
        if elapsed >= timeout_per_search:
            cia = "SEM OFERTAS"
            motivo = "TIMEOUT"
            break

        time.sleep(poll_interval)

    row = {
        "DATA DA BUSCA": data_busca,
        "HORA DA BUSCA": hora_busca,
        "TRECHO": f"{orig}-{dest}",
        "DATA PARTIDA": data_partida,
        "HORA DA PARTIDA": hr_partida,
        "HORA DA CHEGADA": hr_chegada,
        "TARIFA": round(float(tarifa), 2),
        "TX DE EMBARQUE": round(float(tx_embarque), 2),
        "TOTAL": round(float(total), 2),
        "CIA DO VOO": cia,
        "TX DE SERVIÇO": round(float(tx_servico), 2),
    }
    _print_row_log(row, motivo=motivo)
    return row

def main():
    parser = argparse.ArgumentParser(description="Scraper Capo Viagens (CI-ready)")
    parser.add_argument("--out-dir", default=os.environ.get("OUT_DIR", "data"))
    parser.add_argument("--trechos", default=os.environ.get("TRECHOS_CSV"))
    parser.add_argument("--advps", default=os.environ.get("ADVPS_CSV"))
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("TIMEOUT_PER_SEARCH", "60")))
    parser.add_argument("--check-no-results", type=int, default=int(os.environ.get("CHECK_NO_RESULTS_AT", "30")))
    parser.add_argument("--poll", type=int, default=int(os.environ.get("POLL_INTERVAL", "1")))
    parser.add_argument("--pageload-timeout", type=int, default=int(os.environ.get("PAGELOAD_TIMEOUT", "25")))
    parser.add_argument("--headless", action="store_true", default=(os.environ.get("CI","").lower()=="true"))
    args = parser.parse_args()

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    trechos = _parse_trechos_csv(args.trechos)
    advps = _parse_advp_csv(args.advps)

    driver = _mk_driver(headless=args.headless, pageload_timeout=args.pageload_timeout)
    try:
        iter_ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        saida = os.path.join(out_dir, f"CAPO_{iter_ts}.xlsx")
        registros: List[Dict] = []

        print(f"\n=== INÍCIO ({iter_ts}) — {len(trechos)} trechos x {len(advps)} ADVPs ===", flush=True)
        print(f"Timeout/busca: {args.timeout}s | 'No results' aos {args.check_no_results}s | Headless={args.headless}\n", flush=True)

        for (orig, dest) in trechos:
            for dias in advps:
                print(f"[BUSCA] {orig}-{dest} | ADVP={dias}d", flush=True)
                registros.append(_busca(driver, orig, dest, dias, args.timeout, args.check_no_results, args.poll))

        colunas = [
            "DATA DA BUSCA","HORA DA BUSCA","TRECHO","DATA PARTIDA",
            "HORA DA PARTIDA","HORA DA CHEGADA","TARIFA","TX DE EMBARQUE",
            "TOTAL","CIA DO VOO","TX DE SERVIÇO",
        ]
        df = pd.DataFrame(registros, columns=colunas)

        # Escreve XLSX com formatos de data/hora
        with pd.ExcelWriter(saida, engine="openpyxl") as writer:
            sheet = "DADOS"
            df.to_excel(writer, index=False, sheet_name=sheet)
            ws = writer.sheets[sheet]

            header_map = {cell.value: cell.column for cell in ws[1]}
            DATE_FMT = "DD/MM/YYYY"
            TIME_FMT = "HH:MM:SS"

            for col_name in ["DATA DA BUSCA", "DATA PARTIDA"]:
                c = header_map.get(col_name)
                if c:
                    for r in range(2, ws.max_row + 1):
                        cell = ws.cell(row=r, column=c)
                        if cell.value is not None:
                            cell.number_format = DATE_FMT

            for col_name in ["HORA DA BUSCA", "HORA DA PARTIDA", "HORA DA CHEGADA"]:
                c = header_map.get(col_name)
                if c:
                    for r in range(2, ws.max_row + 1):
                        cell = ws.cell(row=r, column=c)
                        if cell.value is not None:
                            cell.number_format = TIME_FMT

        print(f"\nArquivo gerado: {saida}\n", flush=True)

    finally:
        try: driver.quit()
        except Exception: pass

if __name__ == "__main__":
    main()
