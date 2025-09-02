name: scrape-capo-5partes-xls

on:
  schedule:
    - cron: "*/10 * * * *"
  workflow_dispatch:

jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: 25
    permissions:
      contents: write
    env:
      TZ: America/Sao_Paulo
      PYTHONUNBUFFERED: "1"
      CI: "true"

    strategy:
      fail-fast: false
      matrix:
        grupo:
          - { nome: G1, trechos: "CGH-SDU,SDU-CGH,GRU-POA,POA-GRU" }
          - { nome: G2, trechos: "CGH-GIG,GIG-CGH,BSB-CGH,CGH-BSB" }
          - { nome: G3, trechos: "CGH-REC,REC-CGH,CGH-SSA,SSA-CGH" }
          - { nome: G4, trechos: "BSB-GIG,GIG-BSB,GIG-REC,REC-GIG" }
          - { nome: G5, trechos: "GIG-SSA,SSA-GIG,BSB-SDU,SDU-BSB" }

    concurrency:
      group: capo-xls-${{ matrix.grupo.nome }}
      cancel-in-progress: false

    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          persist-credentials: true
          ref: ${{ github.ref_name }}

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: "pip"

      - name: Setup Chrome + Chromedriver
        id: chrome
        uses: browser-actions/setup-chrome@v2
        with:
          install-chromedriver: true

      # REMOVIDO: passo que deletava o chromedriver

      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          pip install "selenium>=4.24.0" "pandas==2.2.2" "openpyxl==3.1.5"

      - name: Sanity check (versions)
        run: |
          echo "Chrome:       ${{ steps.chrome.outputs.chrome-version }}"
          echo "Chromedriver: ${{ steps.chrome.outputs.chromedriver-version }}"
          ${{ steps.chrome.outputs.chrome-path }} --version
          which chromedriver || true
          chromedriver --version

      - name: Run ${{ matrix.grupo.nome }}
        shell: bash
        env:
          TRECHOS_CSV: ${{ matrix.grupo.trechos }}
          ADVPS_CSV: "1,5,11,17,30"
          CHROME_PATH: ${{ steps.chrome.outputs.chrome-path }}
          # Não defina CHROMEDRIVER_PATH. Use o PATH resolvido pelo action.
        run: |
          set -e
          mkdir -p "data/${{ matrix.grupo.nome }}"

          CANDIDATES=("scripts/capoviagens_scraper_gha.py" \
                      "scripts/capo_scraper.py" \
                      "capoviagens_scraper_gha.py" \
                      "capo_scraper.py")

          SCRIPT_PATH=""
          for c in "${CANDIDATES[@]}"; do
            if [ -f "$c" ]; then SCRIPT_PATH="$c"; break; fi
          done
          if [ -z "$SCRIPT_PATH" ]; then
            FOUND=$(git ls-files | grep -E '(capoviagens_scraper_gha|capo_scraper)\.py$' | head -n1 || true)
            if [ -n "$FOUND" ]; then SCRIPT_PATH="$FOUND"; fi
          fi
          if [ -z "$SCRIPT_PATH" ]; then
            echo "ERRO: script Python não encontrado. Esperado scripts/capoviagens_scraper_gha.py" >&2
            exit 1
          fi

          echo "Usando script: $SCRIPT_PATH"
          timeout 22m python -u "$SCRIPT_PATH" \
            --out-dir "data/${{ matrix.grupo.nome }}" \
            --trechos "${TRECHOS_CSV}" \
            --advps "${ADVPS_CSV}" \
            --timeout 60 \
            --check-no-results 30 \
            --poll 1 \
            --pageload-timeout 30 \
            --headless

      - name: Prepare branch (pull rebase)
        shell: bash
        run: |
          set -e
          git config user.name  "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          BRANCH="${GITHUB_REF_NAME}"
          git fetch origin "$BRANCH"
          git checkout "$BRANCH"
          git pull --rebase origin "$BRANCH" || git reset --hard "origin/$BRANCH"

      - name: Commit only this group's files
        id: commit_data
        shell: bash
        run: |
          set -e
          git add "data/${{ matrix.grupo.nome }}/" || true
          if git diff --cached --quiet; then
            echo "did_commit=0" >> "$GITHUB_OUTPUT"
            echo "Sem mudanças para commit."
          else
            git commit -m "CAPO ${{ matrix.grupo.nome }} XLS $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
            echo "did_commit=1" >> "$GITHUB_OUTPUT"
          fi

      - name: Push
        if: ${{ steps.commit_data.outputs.did_commit == '1' }}
        shell: bash
        run: |
          BRANCH="${GITHUB_REF_NAME}"
          n=0
          until [ $n -ge 5 ]; do
            if git push origin "$BRANCH"; then
              echo "Push OK"
              break
            fi
            n=$((n+1))
            echo "Push falhou, retry $n/5..."
            git pull --rebase origin "$BRANCH" || git reset --hard "origin/$BRANCH"
            sleep $((2*n))
          done
