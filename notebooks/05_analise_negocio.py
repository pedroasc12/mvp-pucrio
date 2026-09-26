# Databricks notebook source
# MAGIC %md
# MAGIC # 📈 Etapa 4.5: Análise de Dados e Resposta às Perguntas de Negócio
# MAGIC 
# MAGIC **Objetivo:** Responder com consultas SQL analíticas e visualizações gráficas às 4 perguntas de negócio fundamentadas no início do projeto, conectando os resultados técnicos a decisões estratégicas reais de e-commerce.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuração e Importações

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

spark.sql("USE CATALOG projeto_mvp;")
spark.sql("USE SCHEMA gold;")

plt.rcParams.update({'font.size': 11})

# COMMAND ----------

# MAGIC %md
# MAGIC ## ❓ Pergunta 1: Rentabilidade por Categoria de Produto
# MAGIC **Questão:** Quais categorias de produtos geram maior receita total e qual a relação entre o volume de vendas e o ticket médio?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 categorias por Faturamento Bruto Total
# MAGIC SELECT 
# MAGIC     categoria_nome,
# MAGIC     total_itens_vendidos,
# MAGIC     faturamento_bruto_total,
# MAGIC     ticket_medio_produto,
# MAGIC     nota_media_satisfacao
# MAGIC FROM projeto_mvp.gold.kpi_categoria_performance
# MAGIC WHERE categoria_nome != 'nao_informado'
# MAGIC ORDER BY faturamento_bruto_total DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# Visualização gráfica da Pergunta 1
df_cat = spark.sql("""
    SELECT categoria_nome, faturamento_bruto_total, ticket_medio_produto 
    FROM projeto_mvp.gold.kpi_categoria_performance 
    WHERE categoria_nome != 'nao_informado' 
    ORDER BY faturamento_bruto_total DESC LIMIT 10
""").toPandas()

fig, ax1 = plt.subplots(figsize=(10, 5))

color = 'tab:blue'
ax1.set_xlabel('Categoria de Produto', fontweight='bold')
ax1.set_ylabel('Faturamento Total (R$)', color=color, fontweight='bold')
bars = ax1.bar(df_cat['categoria_nome'], df_cat['faturamento_bruto_total'], color=color, alpha=0.85)
ax1.tick_params(axis='y', labelcolor=color)
plt.xticks(rotation=45, ha='right')

ax2 = ax1.twinx()  
color = 'tab:red'
ax2.set_ylabel('Ticket Médio (R$)', color=color, fontweight='bold')
lines = ax2.plot(df_cat['categoria_nome'], df_cat['ticket_medio_produto'], color=color, marker='o', linewidth=2.5)
ax2.tick_params(axis='y', labelcolor=color)
ax2.grid(False)

plt.title('Top 10 Categorias: Faturamento Total vs Ticket Médio', fontsize=13, fontweight='bold')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Discussão de Negócio (Pergunta 1):**
# MAGIC > Categorias como `beleza_saude` e `relogios_presentes` lideram o faturamento bruto. No entanto, `relogios_presentes` atinge essa receita através de um ticket médio significativamente mais elevado (~R$ 200+) com menor volume de operações logísticas, enquanto categorias como `cama_mesa_banho` dependem de altíssima escala transacional. Estrategicamente, campanhas de margem devem focar no segmento premium, enquanto o ganho em escala logística otimiza itens de ticket menor.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ❓ Pergunta 2: Logística vs Satisfação do Cliente (NPS / Review)
# MAGIC **Questão:** Qual é a correlação entre atrasos na entrega e a nota média de avaliação dada pelo cliente?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Análise comparativa de satisfação: Pedidos no Prazo vs Pedidos Atrasados
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN foi_entregue_com_atraso = true THEN 'Entregue com Atraso'
# MAGIC         WHEN foi_entregue_com_atraso = false THEN 'Entregue no Prazo / Adiantado'
# MAGIC         ELSE 'Não Entregue / Cancelado'
# MAGIC     END as status_logistico,
# MAGIC     COUNT(DISTINCT pedido_id) as total_pedidos,
# MAGIC     ROUND(AVG(dias_entrega_real), 1) as media_dias_entrega,
# MAGIC     ROUND(AVG(nota_avaliacao_media), 2) as media_nota_avaliacao,
# MAGIC     ROUND(SUM(CASE WHEN nota_avaliacao_media = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) as pct_avaliacoes_nota_1
# MAGIC FROM projeto_mvp.gold.fato_vendas
# MAGIC WHERE status_pedido = 'delivered'
# MAGIC GROUP BY foi_entregue_com_atraso
# MAGIC ORDER BY media_nota_avaliacao DESC;

# COMMAND ----------

# Visualização gráfica da Pergunta 2
df_log = spark.sql("""
    SELECT 
        CASE WHEN foi_entregue_com_atraso THEN 'Com Atraso' ELSE 'No Prazo / Adiantado' END as status,
        ROUND(AVG(nota_avaliacao_media), 2) as nota_media,
        ROUND(SUM(CASE WHEN nota_avaliacao_media = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) as pct_detratores
    FROM projeto_mvp.gold.fato_vendas
    WHERE status_pedido = 'delivered'
    GROUP BY foi_entregue_com_atraso
""").toPandas()

fig, ax = plt.subplots(figsize=(7, 4.5))
colors = ['#2ca02c', '#d62728']
bars = ax.bar(df_log['status'], df_log['nota_media'], color=colors, width=0.45)
ax.set_ylim(0, 5)
ax.set_ylabel('Nota Média de Avaliação (1 a 5)', fontweight='bold')
ax.set_xlabel('Desempenho da Entrega', fontweight='bold')
plt.title('Impacto do Atraso Logístico na Satisfação do Consumidor', fontsize=12, fontweight='bold')

for bar in bars:
    yval = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2.0, yval / 2, f"{yval:.2f}", 
            ha='center', va='center', color='white', fontweight='bold', fontsize=14)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Discussão de Negócio (Pergunta 2):**
# MAGIC > Os dados comprovam que o atraso na entrega é o principal vetor de destruição da experiência do cliente no marketplace:
# MAGIC > * Pedidos entregues no prazo obtêm uma nota média de **~4.29**, com índices mínimos de insatisfação.
# MAGIC > * Em contrapartida, quando ocorre atraso, a nota média despenca para **~2.26**, e a taxa de notas 1 (detratores severos) explode para mais de 50%. A engenharia de rotas e o cumprimento de SLAs logísticos são determinantes para a retenção do cliente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ❓ Pergunta 3: Meios de Pagamento e Alavancagem Financeira
# MAGIC **Questão:** Qual a distribuição das modalidades de pagamento e o impacto do parcelamento no ticket médio?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuição por modalidade de pagamento
# MAGIC SELECT 
# MAGIC     tipo_pagamento,
# MAGIC     COUNT(1) as total_transacoes,
# MAGIC     ROUND(SUM(valor_pagamento), 2) as receita_total,
# MAGIC     ROUND(AVG(valor_pagamento), 2) as ticket_medio,
# MAGIC     ROUND(AVG(qtd_parcelas), 1) as media_parcelas
# MAGIC FROM projeto_mvp.silver.pagamentos
# MAGIC GROUP BY tipo_pagamento
# MAGIC ORDER BY receita_total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Correlação entre número de parcelas no Cartão de Crédito e Ticket Médio
# MAGIC SELECT 
# MAGIC     qtd_parcelas,
# MAGIC     COUNT(1) as total_pedidos,
# MAGIC     ROUND(AVG(valor_pagamento), 2) as ticket_medio_parcelado
# MAGIC FROM projeto_mvp.silver.pagamentos
# MAGIC WHERE tipo_pagamento = 'credit_card' AND qtd_parcelas BETWEEN 1 AND 10
# MAGIC GROUP BY qtd_parcelas
# MAGIC ORDER BY qtd_parcelas ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **Discussão de Negócio (Pergunta 3):**
# MAGIC > O cartão de crédito responde por mais de 75% da receita transacionada. Existe uma correlação estritamente positiva entre número de parcelas e ticket médio: compras à vista no cartão registram ticket médio em torno de R$ 90 a R$ 100, enquanto pedidos parcelados em 10 vezes atingem ticket médio superior a R$ 400. Ofertar opções flexíveis de parcelamento sem juros é uma ferramenta primária de aumento do ticket médio no e-commerce brasileiro.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ❓ Pergunta 4: Geografia e Fluxo Logístico Interestadual
# MAGIC **Questão:** Quais estados concentram o maior volume de vendas em comparação aos vendedores, e quais sofrem com os maiores prazos de entrega?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Performance logística e faturamento por Estado do Comprador (UF)
# MAGIC SELECT 
# MAGIC     c.uf_cliente,
# MAGIC     c.regiao_cliente,
# MAGIC     COUNT(DISTINCT f.pedido_id) as total_pedidos,
# MAGIC     ROUND(SUM(f.valor_total_item), 2) as faturamento_total,
# MAGIC     ROUND(AVG(f.dias_entrega_real), 1) as media_dias_entrega,
# MAGIC     ROUND(AVG(f.valor_frete), 2) as custo_medio_frete
# MAGIC FROM projeto_mvp.gold.fato_vendas f
# MAGIC JOIN projeto_mvp.gold.dim_clientes c ON f.cliente_id = c.cliente_id
# MAGIC WHERE f.status_pedido = 'delivered'
# MAGIC GROUP BY c.uf_cliente, c.regiao_cliente
# MAGIC ORDER BY faturamento_total DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC > **Discussão de Negócio (Pergunta 4):**
# MAGIC > Constata-se uma forte assimetria regional:
# MAGIC > * O estado de São Paulo (SP) e a região Sudeste concentram mais de 65% tanto da demanda de consumidores quanto da base instalada de vendedores, resultando em prazos médios de entrega rápidos (8 a 10 dias) e fretes reduzidos (~R$ 15).
# MAGIC > * Em contraste, regiões como Norte e Nordeste registram prazos médios de entrega superiores a 20-28 dias e fretes médios consideravelmente mais caros (~R$ 35+).
# MAGIC > * **Recomendação Estratégica:** Abertura de centros de distribuição (CDs) avançados ou parcerias com operadores regionais no Nordeste para viabilizar prazos competitivos e destravar o potencial de vendas fora do eixo Sul-Sudeste.
