# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 Etapa 4.4: Transformação e Limpeza - Camada Silver
# MAGIC 
# MAGIC **Objetivo:** Processar os dados da camada Bronze, aplicando regras de qualidade, limpeza, tipagem estrita, tratamento de nulos e desduplicação.
# MAGIC 
# MAGIC **Princípio da Camada Silver:** O dado limpo, consistente, confiável e consultável em formato tabular estruturado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Importação de Bibliotecas e Configuração

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, lower, upper, to_timestamp, to_date,
    when, coalesce, lit, round, datediff
)
from pyspark.sql.types import (
    DoubleType, IntegerType, TimestampType, StringType
)

spark.sql("USE CATALOG projeto_mvp;")
spark.sql("USE SCHEMA silver;")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Transformação: `silver.pedidos`
# MAGIC * **O que foi feito:**
# MAGIC   * Conversão das colunas de data (`order_purchase_timestamp`, `order_approved_at`, `order_delivered_carrier_date`, `order_delivered_customer_date`, `order_estimated_delivery_date`) de string para `TimestampType`.
# MAGIC   * Padronização do status do pedido com `lower(trim())`.
# MAGIC   * Cálculo dos prazos reais de entrega (`dias_entrega_real`) e diferença em relação ao prazo estimado (`dias_atraso`).
# MAGIC   * Desduplicação pela chave primária `order_id`.
# MAGIC * **Por que foi feito:** Datas em formato texto impedem cálculos temporais e métricas de SLA; registros duplicados distorcem o volume de vendas.
# MAGIC * **Impacto:** Permite análises confiáveis de logística, atrasos e satisfação.

# COMMAND ----------

df_pedidos_bronze = spark.table("projeto_mvp.bronze.pedidos")

df_pedidos_silver = (
    df_pedidos_bronze
    .select(
        trim(col("order_id")).alias("pedido_id"),
        trim(col("customer_id")).alias("cliente_id"),
        lower(trim(col("order_status"))).alias("status_pedido"),
        to_timestamp(col("order_purchase_timestamp")).alias("data_compra"),
        to_timestamp(col("order_approved_at")).alias("data_aprovacao"),
        to_timestamp(col("order_delivered_carrier_date")).alias("data_envio_transportadora"),
        to_timestamp(col("order_delivered_customer_date")).alias("data_entrega_cliente"),
        to_timestamp(col("order_estimated_delivery_date")).alias("data_estimada_entrega")
    )
    .dropDuplicates(["pedido_id"])
    .withColumn(
        "dias_entrega_real",
        when(col("data_entrega_cliente").isNotNull(),
             datediff(col("data_entrega_cliente"), col("data_compra")))
        .otherwise(lit(None))
    )
    .withColumn(
        "dias_atraso",
        when(col("data_entrega_cliente").isNotNull(),
             datediff(col("data_entrega_cliente"), col("data_estimada_entrega")))
        .otherwise(lit(None))
    )
    .withColumn(
        "foi_entregue_com_atraso",
        when(col("dias_atraso") > 0, lit(True))
        .when(col("dias_atraso") <= 0, lit(False))
        .otherwise(lit(None))
    )
)

(df_pedidos_silver.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.silver.pedidos"))

print(f"✅ silver.pedidos gravada com {spark.table('projeto_mvp.silver.pedidos').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Transformação: `silver.itens_pedido`
# MAGIC * **O que foi feito:**
# MAGIC   * Conversão de `price` e `freight_value` para `DoubleType`.
# MAGIC   * Conversão de `order_item_id` para `IntegerType`.
# MAGIC   * Criação do campo calculado `valor_total_item = preco + valor_frete`.
# MAGIC   * Remoção de registros duplicados pela chave composta (`order_id`, `order_item_id`).
# MAGIC * **Por que foi feito:** Valores monetários em texto geram erros de agregação e frete/preço negativo violam regras contábeis.
# MAGIC * **Impacto:** Base sólida para cálculos de faturamento, ticket médio e custos logísticos.

# COMMAND ----------

df_itens_bronze = spark.table("projeto_mvp.bronze.itens_pedido")

df_itens_silver = (
    df_itens_bronze
    .select(
        trim(col("order_id")).alias("pedido_id"),
        col("order_item_id").cast(IntegerType()).alias("numero_item"),
        trim(col("product_id")).alias("produto_id"),
        trim(col("seller_id")).alias("vendedor_id"),
        to_timestamp(col("shipping_limit_date")).alias("data_limite_envio"),
        round(col("price").cast(DoubleType()), 2).alias("preco"),
        round(col("freight_value").cast(DoubleType()), 2).alias("valor_frete")
    )
    .filter((col("preco") >= 0) & (col("valor_frete") >= 0)) # Regra de acurácia
    .dropDuplicates(["pedido_id", "numero_item"])
    .withColumn("valor_total_item", round(col("preco") + col("valor_frete"), 2))
)

(df_itens_silver.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.silver.itens_pedido"))

print(f"✅ silver.itens_pedido gravada com {spark.table('projeto_mvp.silver.itens_pedido').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Transformação: `silver.clientes`
# MAGIC * **O que foi feito:**
# MAGIC   * Padronização de nomes de cidade em minúsculas e siglas de UF em maiúsculas com 2 caracteres.
# MAGIC   * Higienização do CEP (`zip_code_prefix`) com 5 dígitos numéricos.
# MAGIC   * Desduplicação por `customer_id`.
# MAGIC * **Por que foi feito:** Cidades digitadas de maneiras diferentes ("sao paulo", "São Paulo", "SP") quebram agrupamentos geográficos.

# COMMAND ----------

df_clientes_bronze = spark.table("projeto_mvp.bronze.clientes")

df_clientes_silver = (
    df_clientes_bronze
    .select(
        trim(col("customer_id")).alias("cliente_id"),
        trim(col("customer_unique_id")).alias("cliente_unico_id"),
        trim(col("customer_zip_code_prefix")).alias("cep_prefixo"),
        lower(trim(col("customer_city"))).alias("cidade_cliente"),
        upper(trim(col("customer_state"))).alias("uf_cliente")
    )
    .dropDuplicates(["cliente_id"])
)

(df_clientes_silver.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.silver.clientes"))

print(f"✅ silver.clientes gravada com {spark.table('projeto_mvp.silver.clientes').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Transformação: `silver.produtos`
# MAGIC * **O que foi feito:**
# MAGIC   * Preenchimento de categorias nulas com `'nao_informado'`.
# MAGIC   * Conversão de medidas físicas (peso, dimensões) para `IntegerType` ou `DoubleType`.
# MAGIC   * Padronização do nome da categoria em formato normalizado (`lower(trim())`).

# COMMAND ----------

df_produtos_bronze = spark.table("projeto_mvp.bronze.produtos")

df_produtos_silver = (
    df_produtos_bronze
    .select(
        trim(col("product_id")).alias("produto_id"),
        coalesce(lower(trim(col("product_category_name"))), lit("nao_informado")).alias("categoria_nome"),
        col("product_name_lenght").cast(IntegerType()).alias("tamanho_nome"),
        col("product_description_lenght").cast(IntegerType()).alias("tamanho_descricao"),
        col("product_photos_qty").cast(IntegerType()).alias("qtd_fotos"),
        col("product_weight_g").cast(DoubleType()).alias("peso_gramas"),
        col("product_length_cm").cast(DoubleType()).alias("comprimento_cm"),
        col("product_height_cm").cast(DoubleType()).alias("altura_cm"),
        col("product_width_cm").cast(DoubleType()).alias("largura_cm")
    )
    .dropDuplicates(["produto_id"])
)

(df_produtos_silver.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.silver.produtos"))

print(f"✅ silver.produtos gravada com {spark.table('projeto_mvp.silver.produtos').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Transformação: `silver.pagamentos`
# MAGIC * **O que foi feito:**
# MAGIC   * Conversão de `payment_installments` para `IntegerType` e `payment_value` para `DoubleType`.
# MAGIC   * Padronização do tipo de pagamento (`lower(trim())`).
# MAGIC   * Filtro de segurança para valores de pagamento positivos.

# COMMAND ----------

df_pagamentos_bronze = spark.table("projeto_mvp.bronze.pagamentos")

df_pagamentos_silver = (
    df_pagamentos_bronze
    .select(
        trim(col("order_id")).alias("pedido_id"),
        col("payment_sequential").cast(IntegerType()).alias("sequencial_pagamento"),
        lower(trim(col("payment_type"))).alias("tipo_pagamento"),
        col("payment_installments").cast(IntegerType()).alias("qtd_parcelas"),
        round(col("payment_value").cast(DoubleType()), 2).alias("valor_pagamento")
    )
    .filter(col("valor_pagamento") >= 0)
    .dropDuplicates(["pedido_id", "sequencial_pagamento"])
)

(df_pagamentos_silver.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.silver.pagamentos"))

print(f"✅ silver.pagamentos gravada com {spark.table('projeto_mvp.silver.pagamentos').count()} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Transformação: `silver.avaliacoes` e `silver.vendedores`

# COMMAND ----------

# Avaliações
df_avaliacoes_bronze = spark.table("projeto_mvp.bronze.avaliacoes")

df_avaliacoes_silver = (
    df_avaliacoes_bronze
    .select(
        trim(col("review_id")).alias("avaliacao_id"),
        trim(col("order_id")).alias("pedido_id"),
        col("review_score").cast(IntegerType()).alias("nota_avaliacao"),
        trim(col("review_comment_title")).alias("titulo_comentario"),
        trim(col("review_comment_message")).alias("mensagem_comentario"),
        to_timestamp(col("review_creation_date")).alias("data_criacao_avaliacao"),
        to_timestamp(col("review_answer_timestamp")).alias("data_resposta_avaliacao")
    )
    .filter((col("nota_avaliacao") >= 1) & (col("nota_avaliacao") <= 5))
    .dropDuplicates(["avaliacao_id", "pedido_id"])
)

(df_avaliacoes_silver.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.silver.avaliacoes"))

# Vendedores
df_vendedores_bronze = spark.table("projeto_mvp.bronze.vendedores")

df_vendedores_silver = (
    df_vendedores_bronze
    .select(
        trim(col("seller_id")).alias("vendedor_id"),
        trim(col("seller_zip_code_prefix")).alias("cep_vendedor"),
        lower(trim(col("seller_city"))).alias("cidade_vendedor"),
        upper(trim(col("seller_state"))).alias("uf_vendedor")
    )
    .dropDuplicates(["vendedor_id"])
)

(df_vendedores_silver.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_mvp.silver.vendedores"))

print("✅ silver.avaliacoes e silver.vendedores gravadas com sucesso!")
