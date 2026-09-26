# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Etapa 4.5: Auditoria de Qualidade de Dados (Data Quality)
# MAGIC 
# MAGIC **Objetivo:** Avaliar e comprovar a integridade dos dados através dos **5 Pilares Fundamentais da Qualidade de Dados**:
# MAGIC 1. **Completude (Completeness):** Existência e proporção de valores nulos ou ausentes.
# MAGIC 2. **Consistência (Consistency):** Aderência a formatos e padrões esperados.
# MAGIC 3. **Unicidade (Uniqueness):** Ausência de duplicações indevidas em chaves primárias.
# MAGIC 4. **Acurácia (Accuracy):** Veracidade e coerência com regras de negócio no mundo real.
# MAGIC 5. **Outliers:** Identificação e tratamento de valores atípicos ou extremos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuração

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan, isnull, round, min, max, avg, stddev, desc
spark.sql("USE CATALOG projeto_mvp;")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Pilar: Completude (Valores Nulos e Proporção)

# COMMAND ----------

# Verificação de nulos na tabela de pedidos (Bronze vs Silver)
df_pedidos_bronze = spark.table("projeto_mvp.bronze.pedidos")
total_pedidos = df_pedidos_bronze.count()

print(f"Total de registros analisados: {total_pedidos}")

df_completude = df_pedidos_bronze.select([
    round((count(when(col(c).isNull() | (col(c) == ""), c)) / total_pedidos) * 100, 2).alias(f"pct_nulos_{c}")
    for c in df_pedidos_bronze.columns if not c.startswith("_")
])

display(df_completude)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Diagnóstico de Completude:**
# MAGIC > * A coluna `order_delivered_customer_date` apresenta ~3% de valores nulos.
# MAGIC > * **Causa Raiz:** Pedidos com status 'shipped', 'processing' ou 'canceled' ainda não foram entregues ao cliente.
# MAGIC > * **Tratamento na Camada Silver:** O campo foi mantido como `null`, e foram criadas as métricas condicionais `dias_entrega_real` e `foi_entregue_com_atraso`, que só operam quando o status é 'delivered'.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Pilar: Consistência (Formatos e Padrões)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checagem de consistência: Verificar se existem siglas de estados (UF) fora do padrão de 2 caracteres
# MAGIC SELECT 
# MAGIC     uf_cliente,
# MAGIC     COUNT(1) as total,
# MAGIC     CASE WHEN LENGTH(uf_cliente) = 2 THEN 'Válido' ELSE 'Inconsistente' END as status_padrao
# MAGIC FROM projeto_mvp.silver.clientes
# MAGIC GROUP BY uf_cliente
# MAGIC ORDER BY total DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Pilar: Unicidade (Duplicatas em Chaves Primárias)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Teste de unicidade na tabela silver.pedidos pela chave primária 'pedido_id'
# MAGIC SELECT 
# MAGIC     pedido_id, 
# MAGIC     COUNT(1) as contagem
# MAGIC FROM projeto_mvp.silver.pedidos
# MAGIC GROUP BY pedido_id
# MAGIC HAVING COUNT(1) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Teste de unicidade na chave composta de silver.itens_pedido
# MAGIC SELECT 
# MAGIC     pedido_id, 
# MAGIC     numero_item, 
# MAGIC     COUNT(1) as contagem
# MAGIC FROM projeto_mvp.silver.itens_pedido
# MAGIC GROUP BY pedido_id, numero_item
# MAGIC HAVING COUNT(1) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC > **Diagnóstico de Unicidade:**
# MAGIC > * Zero duplicatas retornadas em ambas as consultas, confirmando que a cláusula `dropDuplicates` na Camada Silver garantiu 100% de integridade das chaves primárias.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Pilar: Acurácia (Veracidade e Limites de Domínio)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificação de violações de acurácia: Preços ou fretes menores ou iguais a zero, ou avaliações fora da escala 1 a 5
# MAGIC SELECT 
# MAGIC     'Preço Negativo/Zero' as regra, COUNT(1) as total_violacoes FROM projeto_mvp.silver.itens_pedido WHERE preco <= 0
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Frete Negativo' as regra, COUNT(1) as total_violacoes FROM projeto_mvp.silver.itens_pedido WHERE valor_frete < 0
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Nota Fora do Intervalo (1-5)' as regra, COUNT(1) as total_violacoes FROM projeto_mvp.silver.avaliacoes WHERE nota_avaliacao NOT BETWEEN 1 AND 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Pilar: Detecção e Análise de Outliers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Estatísticas descritivas para identificar valores extremos em preços e fretes
# MAGIC SELECT 
# MAGIC     MIN(preco) as preco_min,
# MAGIC     ROUND(AVG(preco), 2) as preco_medio,
# MAGIC     ROUND(STDDEV(preco), 2) as preco_desvio_padrao,
# MAGIC     MAX(preco) as preco_max,
# MAGIC     MIN(valor_frete) as frete_min,
# MAGIC     ROUND(AVG(valor_frete), 2) as frete_medio,
# MAGIC     MAX(valor_frete) as frete_max
# MAGIC FROM projeto_mvp.silver.itens_pedido;

# COMMAND ----------

# MAGIC %md
# MAGIC > **Diagnóstico de Outliers:**
# MAGIC > * Foram identificados preços de itens superiores a R$ 6.000,00 e fretes superiores a R$ 400,00.
# MAGIC > * **Decisão Técnica de Engenharia:** Não foram eliminados na Silver porque correspondem a produtos de luxo/equipamentos pesados legítimos. No entanto, nas análises de negócio agregadas (Gold), utilizamos métricas robustas (como mediana e agrupamento por categoria) para evitar distorções nas tomadas de decisão.
