import os
import time
import re
from datetime import datetime, timedelta, time as dtime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# =========================
# Helpers de conversão
# =========================
def money_to_float(txt: str):
    """R$ 1.234,56 -> 1234.56 (float, 2 casas)"""
    if not txt:
        return None
    raw = "".join(ch for ch in str(txt) if ch.isdigit() or ch in ".,")
    raw = raw.replace(".", "").replace(",", ".")
    try:
        return round(float(raw), 2)
    except Exception:
        return None

def hhmm_to_time(txt: str):
    """'03:15'|'03:15:00' -> time(3,15,0). Qualquer coisa inválida -> None"""
    if not txt:
        return None
    s = str(txt).strip()
    if re.fullmatch(r"\d{2}:\d{2}", s):
        s += ":00"
    m = re.fullmatch(r"(\d{2}):(\d{2}):(\d{2})", s)
    if not m:
        return None
    h, m_, s_ = map(int, m.groups())
    try:
        return dtime(h, m_, s_)
    except ValueError:
        return None

def close_popup_if_present(driver):
    try:
        btn = driver.find_element(By.XPATH,
            '//button[contains(@class,"close") or contains(@aria-label,"Fechar")]'
        )
        btn.click()
        time.sleep(1)
    except Exception:
        pass

def accept_cookies_if_present(driver):
    try:
        btn = driver.find_element(By.XPATH,
            '//button[contains(translate(text(),"ACEITAR","aceitar"),"aceitar")]'
        )
        btn.click()
        time.sleep(1)
    except Exception:
        pass

def main():
    dias_offset = 30
    pasta_saida = r"C:\Users\tassiana.silva\Documents\BUSCA DE VOOS"
    os.makedirs(pasta_saida, exist_ok=True)

    trechos = [
        "CGH-SDU", "SDU-CGH", "GRU-REC", "REC-GRU",
        "BSB-CGH", "POA-CGH", "POA-GIG", "SSA-CGH",
        "CGH-SSA", "CGH-BSB", "CGH-POA", "REC-GIG",
        "GRU-FOR", "REC-CGH", "GIG-REC", "GIG-POA",
        "CGH-REC", "CWB-GIG", "GIG-CWB", "FOR-GRU",
        "BEL-GRU", "GIG-FOR", "GRU-SSA",
    ]
    data_voo = (datetime.today() + timedelta(days=dias_offset)).strftime("%d-%m-%Y")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()
    resultados = []

    try:
        for trecho in trechos:
            origem, destino = trecho.split("-")
            search_id = int(time.time() * 1000)
            url = (
                f"https://123milhas.com/v2/busca?"
                f"de={origem}&para={destino}&adultos=1&criancas=0&bebes=0&"
                f"ida={data_voo}&classe=3&is_loyalty=0&search_id={search_id}"
            )
            driver.get(url)

            for attempt in range(3):
                try:
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.XPATH,
                            "//div[starts-with(@id,'outbound-section-0-') and .//label]"
                        ))
                    )
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.XPATH,
                            "//div[starts-with(@id,'price-section-0-')]"
                        ))
                    )
                    close_popup_if_present(driver)
                    accept_cookies_if_present(driver)
                    break
                except TimeoutException:
                    close_popup_if_present(driver)
                    time.sleep(5)

            # inicializa variáveis
            cia = primeiro_horario = segundo_horario = adulto = None
            valor_taxas = desconto_pix = total_pix = numero_voo = tarifa_text = None

            # bloco de dados de voo
            try:
                bloco_out = driver.find_elements(By.XPATH,
                    "//div[starts-with(@id,'outbound-section-0-') and .//label]"
                )[0]
                label = bloco_out.find_element(By.TAG_NAME, "label")
                raw_label = label.text.strip()
                print(f"Raw outbound label text: {raw_label}")
                try:
                    cia = label.find_element(By.XPATH, ".//div[1]/span[1]").text.strip()
                except:
                    cia = None
                try:
                    numero_voo = label.find_element(By.XPATH, ".//div[1]/span[2]").text.strip()
                except:
                    numero_voo = None
                if not numero_voo:
                    m = re.search(r"(\d{3,6})", raw_label)
                    numero_voo = m.group(1) if m else None
                spans = label.find_elements(By.XPATH, ".//div[2]/div/span")
                primeiro_horario = spans[0].text.strip() if spans else None
                segundo_horario = spans[1].text.strip() if len(spans)>1 else None
                print(f"Captura Voo - Cia: {cia}, Numero: {numero_voo}, Horarios: {primeiro_horario}/{segundo_horario}")
            except Exception as e:
                print(f"Erro bloco_out: {e}")

            # bloco de preço
            try:
                bloco_price = driver.find_elements(By.XPATH,
                    "//div[starts-with(@id,'price-section-0-')]"
                )[0]
                cont = bloco_price.find_element(By.XPATH, ".//div[3]")
                adulto = cont.find_element(By.XPATH, ".//div[1]/span[2]").text.strip()
                valor_taxas = cont.find_element(By.XPATH, ".//div[2]/span[2]").text.strip()
                desconto_pix = cont.find_element(By.XPATH, ".//div[3]/span[2]").text.strip()
                total_pix = cont.find_element(By.XPATH, ".//div[4]/span[2]").text.strip()
                print(f"Captura Preco - Adulto: {adulto}, Taxas: {valor_taxas}, DescPix: {desconto_pix}, TotalPix: {total_pix}")
            except Exception as e:
                print(f"Erro bloco_price: {e}")

            # detalhe tarifa
            try:
                btn_info = driver.find_element(By.XPATH, "//*[contains(@id,'-button-info')]")
                btn_info.click()
                time.sleep(5)
                tarifa_text = WebDriverWait(driver, 20).until(
                    EC.visibility_of_element_located((By.XPATH,
                        '//*[@id="app-layout"]/div[1]/div/div/div[2]/flight-itinerary/div[2]/span'
                    ))
                ).text.strip()
                print(f"Captura Tarifa: {tarifa_text}")
            except Exception as e:
                print(f"Erro tarifa_text: {e}")

            resultado = {
                "trecho": trecho,
                "timestamp_busca": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "cia": cia,
                "primeiro_horario": primeiro_horario,   # será convertido ao salvar
                "segundo_horario": segundo_horario,     # será convertido ao salvar
                "adulto": adulto,                       # será convertido ao salvar
                "tarifa_texto": tarifa_text,
                "valor_taxas": valor_taxas,             # será convertido ao salvar
                "desconto_pix": desconto_pix,           # será convertido ao salvar
                "total_pix": total_pix,                 # será convertido ao salvar
                "numero_voo": numero_voo,
            }
            resultados.append(resultado)

            print("\n=== Resultado Capturado ===")
            for k, v in resultado.items():
                print(f"{k:16}: {v}")
            print("="*40 + "\n")

            time.sleep(15)

    finally:
        driver.quit()

    # =========================
    # SALVAR EM XLS (refinado)
    # =========================
    # 1) Converte horas e valores antes de criar o DF
    for r in resultados:
        r["primeiro_horario"] = hhmm_to_time(r.get("primeiro_horario"))
        r["segundo_horario"]  = hhmm_to_time(r.get("segundo_horario"))
        r["adulto"]        = money_to_float(r.get("adulto"))
        r["valor_taxas"]   = money_to_float(r.get("valor_taxas"))
        r["desconto_pix"]  = money_to_float(r.get("desconto_pix"))
        r["total_pix"]     = money_to_float(r.get("total_pix"))

    df = pd.DataFrame(resultados)

    nome_arquivo = datetime.now().strftime("123MILHAS_%Y%m%d_%H%M.xlsx")
    caminho_arquivo = os.path.join(pasta_saida, nome_arquivo)

    # 2) Salva com openpyxl e aplica number_format nas colunas
    from openpyxl import load_workbook
    with pd.ExcelWriter(caminho_arquivo, engine="openpyxl") as writer:
        sheet = "BUSCAS"
        df.to_excel(writer, index=False, sheet_name=sheet)
        ws = writer.sheets[sheet]

        # mapa col->índice
        col_idx = {name: idx+1 for idx, name in enumerate(df.columns)}

        # Horas
        for col in ["primeiro_horario", "segundo_horario"]:
            if col in col_idx:
                j = col_idx[col]
                for r in range(2, len(df)+2):
                    cell = ws.cell(row=r, column=j)
                    if cell.value is not None:
                        cell.number_format = "hh:mm:ss"

        # Valores em dinheiro
        for col in ["adulto", "valor_taxas", "desconto_pix", "total_pix"]:
            if col in col_idx:
                j = col_idx[col]
                for r in range(2, len(df)+2):
                    cell = ws.cell(row=r, column=j)
                    if cell.value is not None:
                        cell.number_format = "#,##0.00"

        # (opcional) larguras de coluna rápidas
        widths = {"trecho":12, "timestamp_busca":20, "cia":16, "numero_voo":12,
                  "primeiro_horario":12, "segundo_horario":12,
                  "adulto":14, "valor_taxas":14, "desconto_pix":14, "total_pix":14}
        for name, width in widths.items():
            if name in col_idx:
                ws.column_dimensions[chr(64 + col_idx[name])].width = width

    print(f"\nTodas as buscas concluídas. Arquivo salvo em:\n{caminho_arquivo}")

if __name__ == "__main__":
    main()
