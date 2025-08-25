def render_empresa(df_emp: pd.DataFrame, key_suffix: str):
    # Toggle com key único por aba para evitar IDs duplicados
    menor_preco = st.toggle(
        "Menor preço",
        value=True,
        key=f"toggle_menor_preco_{key_suffix}",
        help="Ligado: usa menor preço; Desligado: usa média. Vale para gráficos e tabela."
    )

    if df_emp.empty:
        st.info("Sem dados para os filtros atuais.")
        return

    # Limite dinâmico do eixo Y (pedido)
    y_max_lim = 1500 if menor_preco else 3000

    # KPIs
    k1, k2 = st.columns(2)
    with k1:
        st.metric("Buscas", f"{len(df_emp):,}".replace(",", "."))
    with k2:
        preco_val = df_emp["TOTAL"].min() if menor_preco else df_emp["TOTAL"].mean()
        st.metric("Preço", fmt_moeda0(preco_val))

    # 1) Preço por hora
    horas = pd.DataFrame({"HORA_HH": list(range(24))})
    if menor_preco:
        by_hora = df_emp.groupby("HORA_HH", as_index=False)["TOTAL"].min().rename(columns={"TOTAL": "PRECO"})
    else:
        by_hora = df_emp.groupby("HORA_HH", as_index=False)["TOTAL"].mean().rename(columns={"TOTAL": "PRECO"})
    by_hora = horas.merge(by_hora, on="HORA_HH", how="left").fillna({"PRECO": 0})
    barras_com_tendencia(
        by_hora, "HORA_HH", "PRECO", "O",
        "Preço por hora", x_title="HORA",
        y_max=y_max_lim, sort=list(range(24))
    )

    # 2) Preço por ADVP
    if menor_preco:
        by_advp = df_emp.groupby("ADVP", as_index=False)["TOTAL"].min().rename(columns={"TOTAL": "PRECO"}).sort_values("ADVP")
    else:
        by_advp = df_emp.groupby("ADVP", as_index=False)["TOTAL"].mean().rename(columns={"TOTAL": "PRECO"}).sort_values("ADVP")
    barras_com_tendencia(by_advp, "ADVP", "PRECO", "O", "Preço por ADVP", y_max=y_max_lim)

    # 3) Preço Top 20 trechos
    if menor_preco:
        by_trecho = (
            df_emp.groupby("TRECHO", as_index=False)["TOTAL"].min()
            .rename(columns={"TOTAL": "PRECO"})
            .sort_values("PRECO", ascending=False)
            .head(20)
        )
    else:
        by_trecho = (
            df_emp.groupby("TRECHO", as_index=False)["TOTAL"].mean()
            .rename(columns={"TOTAL": "PRECO"})
            .sort_values("PRECO", ascending=False)
            .head(20)
        )
    barras_com_tendencia(by_trecho, "TRECHO", "PRECO", "N", "Preço Top 20 trechos", y_max=y_max_lim)

    # 4) Tabela Top 3 (segue o toggle)
    top3_tabela(df_emp, agg="min" if menor_preco else "mean")

    # 5) (final) SHARE CIAS
    chart_cia_stack_trecho(df_emp)
