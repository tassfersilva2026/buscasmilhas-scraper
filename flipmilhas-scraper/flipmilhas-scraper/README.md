# FlipMilhas Scraper (GitHub Actions)

Estrutura pronta para rodar scraper **headless** da FlipMilhas em agendamento via **GitHub Actions**.

## Estrutura
```
flipmilhas-scraper/
├─ .github/
│  └─ workflows/
│     └─ scrape.yml
├─ flipmilhas_scraper_gha.py
├─ requirements.txt
└─ README.md
```

## Como usar
1. Crie um repositório no GitHub e suba estes arquivos.
2. Vá em **Actions** e rode manualmente o workflow `scrape-flipmilhas` (ou aguarde o cron).
3. Resultado:
   - **Artifact**: `FLIPMILHAS-planilha` com `output/FLIPMILHAS.xlsx`.
   - (Opcional) Cópia versionada em `data/FLIPMILHAS_YYYYMMDD_HHMMSS.xlsx` (commit automático).

### Rodar local (opcional)
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate
pip install -r requirements.txt
python flipmilhas_scraper_gha.py --headless --once
```

### Ajustar agenda
Edite `.github/workflows/scrape.yml` → `cron` (UTC).

> Dica: para 07:00 de Brasília (UTC-3), use `0 10 * * *`.
