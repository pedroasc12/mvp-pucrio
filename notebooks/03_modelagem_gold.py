# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Etapa 4.3 e 4.4: Modelagem Dimensional - Camada Gold
# MAGIC 
# MAGIC **Objetivo:** Construir o modelo dimensional em **Esquema Estrela (Star Schema)** e tabelas analíticas agregadas (Data Marts) a partir dos dados limpos da Camada Silver, otimizando o ambiente para consultas analíticas de alta performance.
# MAGIC 
# MAGIC **Princípio da Camada Gold:** Dados modelados, enriquecidos e prontos para consumo por ferramentas de BI, relatórios gerenciais e tomadas de decisão.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuração e Importação

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, dayofweek,
    date_format, quarter, when, lit, count, sum, avg, round
)

spark.sql("USE CATALOG projeto_mvp;")
spark.sql("USE SCHEMA gold;")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Dimensão: `gold.dim_clientes`
# MAGIC * Enriquecida com a macrorregião geográfica brasileira para análises espaciais.

# COMMAND ----------

df_clientes = spark.table("projeto_mvp.silver.clientes")

df_dim_clientes = (
    df_clientes
    .select(
        col("cliente_id"),
        col("cliente_unico_id"),
        col("cep_prefixo"),
        col("cidade_cliente"),
        col("uf_cliente"),
        when(col("uf_cliente").isin("SP", "RJ", "MG", "ES"), lit("Sudeste"))
        .when(col("uf_cliente").isin("PR", "SC", "RS"), lit("Sul"))
        .when(col("uf_cliente").isin("BA", "PE", "CE", "MA", "PB", "RN", "AL", "SE", "PI"), lit("Nordeste"))
        .when(col("uf_cliente").isin("GO", "MT", "MS", "DF"), lit("Centro-Oeste"))
        .otherwise(lit("Norte")).alias("regiao_cliente")
    )
)

(df_dim_clientes.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.gold.dim_clientes"))

print(f"✅ gold.dim_clientes criada com {spark.table('projeto_mvp.gold.dim_clientes').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Dimensão: `gold.dim_produtos`
# MAGIC * Enriquecida com faixa de peso e cubagem estimada.

# COMMAND ----------

df_produtos = spark.table("projeto_mvp.silver.produtos")

df_dim_produtos = (
    df_produtos
    .select(
        col("produto_id"),
        col("categoria_nome"),
        col("peso_gramas"),
        when(col("peso_gramas") <= 500, lit("Ate 500g"))
        .when(col("peso_gramas") <= 2000, lit("500g a 2kg"))
        .when(col("peso_gramas") <= 5000, lit("2kg a 5kg"))
        .otherwise(lit("Acima de 5kg")).alias("faixa_peso"),
        col("comprimento_cm"),
        col("altura_cm"),
        col("largura_cm"),
        round((col("comprimento_cm") * col("altura_cm") * col("largura_cm")), 2).alias("volume_cm3")
    )
)

(df_dim_produtos.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.gold.dim_produtos"))

print(f"✅ gold.dim_produtos criada com {spark.table('projeto_mvp.gold.dim_produtos').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Dimensão: `gold.dim_vendedores`

# COMMAND ----------

df_vendedores = spark.table("projeto_mvp.silver.vendedores")

df_dim_vendedores = (
    df_vendedores
    .select(
        col("vendedor_id"),
        col("cep_vendedor"),
        col("cidade_vendedor"),
        col("uf_vendedor"),
        when(col("uf_vendedor").isin("SP", "RJ", "MG", "ES"), lit("Sudeste"))
        .when(col("uf_vendedor").isin("PR", "SC", "RS"), lit("Sul"))
        .when(col("uf_vendedor").isin("BA", "PE", "CE", "MA", "PB", "RN", "AL", "SE", "PI"), lit("Nordeste"))
        .when(col("uf_vendedor").isin("GO", "MT", "MS", "DF"), lit("Centro-Oeste"))
        .otherwise(lit("Norte")).alias("regiao_vendedor")
    )
)

(df_dim_vendedores.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.gold.dim_vendedores"))

print(f"✅ gold.dim_vendedores criada com {spark.table('projeto_mvp.gold.dim_vendedores').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Dimensão: `gold.dim_tempo`
# MAGIC * Gerada a partir de todas as datas únicas de compra registradas no histórico.

# COMMAND ----------

df_pedidos = spark.table("projeto_mvp.silver.pedidos")

df_dim_tempo = (
    df_pedidos
    .select(to_date(col("data_compra")).alias("data_completa"))
    .distinct()
    .filter(col("data_completa").isNotNull())
    .select(
        col("data_completa"),
        year(col("data_completa")).alias("ano"),
        month(col("data_completa")).alias("mes"),
        dayofmonth(col("data_completa")).alias("dia"),
        dayofweek(col("data_completa")).alias("dia_semana_numero"),
        date_format(col("data_completa"), "EEEE").alias("nome_dia_semana"),
        date_format(col("data_completa"), "MMMM").alias("nome_mes"),
        quarter(col("data_completa")).alias("trimestre"),
        when(dayofweek(col("data_completa")).isin(1, 7), lit(True)).otherwise(lit(False)).alias("eh_fim_de_semana")
    )
    .orderBy("data_completa")
)

(df_dim_tempo.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.gold.dim_tempo"))

print(f"✅ gold.dim_tempo criada com {spark.table('projeto_mvp.gold.dim_tempo').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Tabela Fato Central: `gold.fato_vendas`
# MAGIC * Grão: Cada item de venda registrado em um pedido.
# MAGIC * Reúne chaves estrangeiras para as dimensões e métricas operacionais, logísticas e financeiras.

# COMMAND ----------

df_itens = spark.table("projeto_mvp.silver.itens_pedido")
df_avaliacoes = spark.table("projeto_mvp.silver.avaliacoes")

# Agregação prévia de avaliações por pedido (média da nota caso haja múltiplas)
df_review_agg = (
    df_avaliacoes
    .groupBy("pedido_id")
    .agg(round(avg("nota_avaliacao"), 1).alias("nota_avaliacao_media"))
)

df_fato_vendas = (
    df_itens.alias("i")
    .join(df_pedidos.alias("p"), "pedido_id", "inner")
    .join(df_review_agg.alias("r"), "pedido_id", "left")
    .select(
        col("i.pedido_id"),
        col("i.numero_item"),
        col("p.cliente_id"),
        col("i.produto_id"),
        col("i.vendedor_id"),
        to_date(col("p.data_compra")).alias("data_compra"),
        col("p.status_pedido"),
        col("i.preco"),
        col("i.valor_frete"),
        col("i.valor_total_item"),
        col("p.dias_entrega_real"),
        col("p.dias_atraso"),
        col("p.foi_entregue_com_atraso"),
        col("r.nota_avaliacao_media")
    )
)

(df_fato_vendas.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.gold.fato_vendas"))

print(f"✅ gold.fato_vendas criada com {spark.table('projeto_mvp.gold.fato_vendas').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Visão Agregada / Data Mart: `gold.kpi_categoria_performance`
# MAGIC * Tabela analítica agregada com métricas consolidadas por categoria de produto para responder à Pergunta 1.

# COMMAND ----------

df_dim_prod = spark.table("projeto_mvp.gold.dim_produtos")
df_fato = spark.table("projeto_mvp.gold.fato_vendas")

df_kpi_categoria = (
    df_fato.alias("f")
    .join(df_dim_prod.alias("p"), "produto_id", "inner")
    .groupBy(col("p.categoria_nome"))
    .agg(
        count("f.pedido_id").alias("total_itens_vendidos"),
        round(sum("f.preco"), 2).alias("receita_total_produtos"),
        round(sum("f.valor_frete"), 2).alias("total_frete"),
        round(sum("f.valor_total_item"), 2).alias("faturamento_bruto_total"),
        round(avg("f.preco"), 2).alias("ticket_medio_produto"),
        round(avg("f.nota_avaliacao_media"), 2).alias("nota_media_satisfacao")
    )
    .orderBy(col("faturamento_bruto_total").desc())
)

(df_kpi_categoria.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.gold.kpi_categoria_performance"))

print(f"✅ gold.kpi_categoria_performance criada com sucesso!")
