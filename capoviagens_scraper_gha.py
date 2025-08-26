# Espera conteúdo real
scroll_jiggle(driver)
ok = wait_results_ready(driver, max_wait=espera)
now = datetime.now(TZ)

if not ok:
    row = {
        "DATA DA BUSCA": now.date(),
        "HORA DA BUSCA": dtime(now.hour, now.minute, now.second),
        "TRECHO": trecho_str,
        "DATA PARTIDA": data_voo,
        "HORA DA PARTIDA": None,
        "DATA CHEGADA": data_voo,
        "HORA DA CHEGADA": None,
        "TARIFA": None,
        "TX DE EMBARQUE": None,
        "TOTAL": None,
        "CIA DO VOO": "Sem Ofertas",
    }
    append_row(out_path, row)
    return base_tab

# 1º card visível
card = first_visible_card(driver)
if card is None:
    # fallback: usa o main inteiro por regex
    row = {
        "DATA DA BUSCA": now.date(),
        "HORA DA BUSCA": dtime(now.hour, now.minute, now.second),
        "TRECHO": trecho_str,
        "DATA PARTIDA": data_voo,
        "HORA DA PARTIDA": None,
        "DATA CHEGADA": data_voo,
        "HORA DA CHEGADA": None,
        "TARIFA": None,
        "TX DE EMBARQUE": None,
        "TOTAL": None,
        "CIA DO VOO": "Sem Ofertas",
    }
    append_row(out_path, row)
    return base_tab

# tenta pelos XPaths originais RELATIVOS ao card
rel_partida = ".//label/label//div/div/div[1]/span[1]"
rel_chegada = ".//label/label//div/div/div[3]/span[1]"
rel_tarifa  = ".//ancestor::div[1]/following::div[contains(@class,'price')][1]//span[1]"  # fallback amplo
rel_tx_emb  = ".//ancestor::div[1]/following::div//span[contains(.,',')][last()]"
rel_total   = ".//ancestor::div[1]/following::div//span[contains(.,'R$')][last()]"
rel_cia     = ".//div[contains(@class,'div')]/span | .//div/span"

# 1ª tentativa: XPaths “oficiais” que você tinha (convertidos para relativo ao card)
hora_partida_txt = text_or_inner(card, rel_partida)
hora_chegada_txt = text_or_inner(card, rel_chegada)
tarifa_txt       = text_or_inner(card, rel_tarifa)
tx_emb_txt       = text_or_inner(card, rel_tx_emb)
total_txt        = text_or_inner(card, rel_total)
cia_txt          = text_or_inner(card, rel_cia)

# Fallback final: regex no innerText do card
if not (hora_partida_txt and total_txt and (tarifa_txt or tx_emb_txt)):
    parsed = regex_from_inner_text(card)
    hora_partida_txt = hora_partida_txt or parsed.get("partida")
    hora_chegada_txt = hora_chegada_txt or parsed.get("chegada")
    if not (tarifa_txt or total_txt):
        anyp = parsed.get("qualquer_preco")
        tarifa_txt = tarifa_txt or anyp
        total_txt  = total_txt  or anyp

# Normalizações
def brl_to_float(s):
    if not s: return None
    s1 = re.sub(r"[^\d,\.]", "", s).replace(".", "").replace(",", ".")
    try:
        return float(s1)
    except Exception:
        return None

def hhmm_to_time(s):
    if not s: return None
    m = re.fullmatch(r"(\d{2}):(\d{2})(?::(\d{2}))?", s.strip())
    if not m: return None
    hh, mm, ss = m.groups(); ss = ss or "00"
    try:
        return dtime(int(hh), int(mm), int(ss))
    except ValueError:
        return None

hora_partida = hhmm_to_time(hora_partida_txt)
hora_chegada = hhmm_to_time(hora_chegada_txt)
tarifa_val   = brl_to_float(tarifa_txt)
tx_emb_val   = brl_to_float(tx_emb_txt)
total_val    = brl_to_float(total_txt)
cia_clean    = (cia_txt or "").strip()

row = {
    "DATA DA BUSCA": now.date(),
    "HORA DA BUSCA": dtime(now.hour, now.minute, now.second),
    "TRECHO": trecho_str,
    "DATA PARTIDA": data_voo,
    "HORA DA PARTIDA": hora_partida,
    "DATA CHEGADA": data_voo,
    "HORA DA CHEGADA": hora_chegada,
    "TARIFA": tarifa_val,
    "TX DE EMBARQUE": tx_emb_val,
    "TOTAL": total_val,
    "CIA DO VOO": cia_clean,
}
append_row(out_path, row)
return base_tab
