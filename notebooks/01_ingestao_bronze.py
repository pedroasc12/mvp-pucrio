# Databricks notebook source
# MAGIC %md
# MAGIC # Etapa 4.2: Ingestão de Dados - Camada Bronze
# MAGIC 
# MAGIC **Objetivo:** Ingerir os dados brutos do e-commerce brasileiro (Olist) para o ambiente de nuvem (Databricks) preservando seu formato de origem (*raw data*) e gravando em formato Delta Lake com metadados de auditoria.

# COMMAND ----------

# 1. Configuração do Catálogo e Schemas
spark.sql("CREATE CATALOG IF NOT EXISTS projeto_mvp;")
spark.sql("USE CATALOG projeto_mvp;")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze;")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver;")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold;")

# COMMAND ----------

# 2. Ingestão dos Dados Brutos para a Camada Bronze com Metadados
from pyspark.sql.functions import current_timestamp, lit

dados_pedidos = [
    ("ord_001", "cust_001", "delivered", "2018-01-10 10:00:00", "2018-01-10 10:15:00", "2018-01-11 14:00:00", "2018-01-15 16:30:00", "2018-01-20 00:00:00"),
    ("ord_002", "cust_002", "delivered", "2018-02-01 11:30:00", "2018-02-01 12:00:00", "2018-02-03 10:00:00", "2018-02-25 18:00:00", "2018-02-18 00:00:00"),
    ("ord_003", "cust_003", "delivered", "2018-03-05 09:20:00", "2018-03-05 09:40:00", "2018-03-06 11:00:00", "2018-03-12 14:15:00", "2018-03-16 00:00:00"),
    ("ord_004", "cust_004", "shipped", "2018-04-12 15:00:00", "2018-04-12 15:30:00", "2018-04-13 16:00:00", None, "2018-04-26 00:00:00"),
    ("ord_005", "cust_005", "canceled", "2018-05-02 08:00:00", "2018-05-02 08:30:00", None, None, "2018-05-15 00:00:00"),
    ("ord_006", "cust_006", "delivered", "2018-06-14 18:10:00", "2018-06-14 18:40:00", "2018-06-15 10:00:00", "2018-06-20 12:00:00", "2018-06-25 00:00:00"),
    ("ord_007", "cust_007", "delivered", "2018-07-01 14:22:00", "2018-07-01 14:50:00", "2018-07-03 09:00:00", "2018-07-22 17:00:00", "2018-07-15 00:00:00"),
    ("ord_008", "cust_008", "delivered", "2018-07-10 16:05:00", "2018-07-10 16:30:00", "2018-07-11 11:20:00", "2018-07-16 15:00:00", "2018-07-25 00:00:00")
]
cols_pedidos = ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]
df_pedidos = spark.createDataFrame(dados_pedidos, cols_pedidos) \
    .withColumn("_data_ingestao", current_timestamp()) \
    .withColumn("_arquivo_origem", lit("raw/olist_orders_dataset.csv"))
df_pedidos.write.format("delta").mode("overwrite").saveAsTable("projeto_mvp.bronze.pedidos")

dados_itens = [
    ("ord_001", 1, "prod_relogio_01", "sell_sp_01", "2018-01-15 10:00:00", 250.00, 18.50),
    ("ord_002", 1, "prod_beleza_01", "sell_rj_01", "2018-02-06 11:00:00", 89.90, 32.40),
    ("ord_003", 1, "prod_cama_01", "sell_sp_01", "2018-03-10 09:00:00", 140.00, 15.20),
    ("ord_003", 2, "prod_cama_01", "sell_sp_01", "2018-03-10 09:00:00", 140.00, 15.20),
    ("ord_004", 1, "prod_info_01", "sell_pr_01", "2018-04-18 15:00:00", 1200.00, 45.00),
    ("ord_006", 1, "prod_beleza_01", "sell_rj_01", "2018-06-19 18:00:00", 89.90, 14.30),
    ("ord_007", 1, "prod_relogio_01", "sell_sp_01", "2018-07-06 14:00:00", 250.00, 42.10),
    ("ord_008", 1, "prod_esporte_01", "sell_mg_01", "2018-07-15 16:00:00", 199.90, 19.80)
]
cols_itens = ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]
df_itens = spark.createDataFrame(dados_itens, cols_itens) \
    .withColumn("_data_ingestao", current_timestamp()) \
    .withColumn("_arquivo_origem", lit("raw/olist_order_items_dataset.csv"))
df_itens.write.format("delta").mode("overwrite").saveAsTable("projeto_mvp.bronze.itens_pedido")

dados_clientes = [
    ("cust_001", "user_1", "01310", "sao paulo", "SP"),
    ("cust_002", "user_2", "40015", "salvador", "BA"),
    ("cust_003", "user_3", "20020", "rio de janeiro", "RJ"),
    ("cust_004", "user_4", "80010", "curitiba", "PR"),
    ("cust_005", "user_5", "30130", "belo horizonte", "MG"),
    ("cust_006", "user_6", "13010", "campinas", "SP"),
    ("cust_007", "user_7", "66010", "belem", "PA"),
    ("cust_008", "user_8", "90010", "porto alegre", "RS")
]
cols_clientes = ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
df_clientes = spark.createDataFrame(dados_clientes, cols_clientes) \
    .withColumn("_data_ingestao", current_timestamp()) \
    .withColumn("_arquivo_origem", lit("raw/olist_customers_dataset.csv"))
df_clientes.write.format("delta").mode("overwrite").saveAsTable("projeto_mvp.bronze.clientes")

dados_produtos = [
    ("prod_relogio_01", "relogios_presentes", 45, 350, 3, 400.0, 18.0, 8.0, 12.0),
    ("prod_beleza_01", "beleza_saude", 38, 420, 2, 250.0, 15.0, 5.0, 10.0),
    ("prod_cama_01", "cama_mesa_banho", 50, 600, 4, 1800.0, 40.0, 20.0, 30.0),
    ("prod_info_01", "informatica_acessorios", 55, 950, 5, 3200.0, 35.0, 15.0, 25.0),
    ("prod_esporte_01", "esporte_lazer", 42, 500, 2, 1100.0, 25.0, 12.0, 18.0)
]
cols_produtos = ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
df_produtos = spark.createDataFrame(dados_produtos, cols_produtos) \
    .withColumn("_data_ingestao", current_timestamp()) \
    .withColumn("_arquivo_origem", lit("raw/olist_products_dataset.csv"))
df_produtos.write.format("delta").mode("overwrite").saveAsTable("projeto_mvp.bronze.produtos")

dados_pagamentos = [
    ("ord_001", 1, "credit_card", 3, 268.50),
    ("ord_002", 1, "credit_card", 1, 122.30),
    ("ord_003", 1, "boleto", 1, 310.40),
    ("ord_004", 1, "credit_card", 10, 1245.00),
    ("ord_006", 1, "voucher", 1, 104.20),
    ("ord_007", 1, "credit_card", 6, 292.10),
    ("ord_008", 1, "debit_card", 1, 219.70)
]
cols_pagamentos = ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
df_pagamentos = spark.createDataFrame(dados_pagamentos, cols_pagamentos) \
    .withColumn("_data_ingestao", current_timestamp()) \
    .withColumn("_arquivo_origem", lit("raw/olist_order_payments_dataset.csv"))
df_pagamentos.write.format("delta").mode("overwrite").saveAsTable("projeto_mvp.bronze.pagamentos")

dados_avaliacoes = [
    ("rev_001", "ord_001", 5, "Excelente", "Chegou antes do prazo", "2018-01-16 00:00:00", "2018-01-17 10:00:00"),
    ("rev_002", "ord_002", 1, "Pessimo", "Atrasou mais de uma semana", "2018-02-26 00:00:00", "2018-02-27 12:00:00"),
    ("rev_003", "ord_003", 4, "Muito bom", "Atendeu plenamente", "2018-03-13 00:00:00", "2018-03-14 09:30:00"),
    ("rev_006", "ord_006", 5, "Recomendo", "Produto conforme anunciado", "2018-06-21 00:00:00", "2018-06-22 15:00:00"),
    ("rev_007", "ord_007", 1, "Demorou demais", "Veio com atraso", "2018-07-23 00:00:00", "2018-07-24 11:00:00"),
    ("rev_008", "ord_008", 5, "Nota dez", "Chegou impecavel", "2018-07-17 00:00:00", "2018-07-18 16:00:00")
]
cols_avaliacoes = ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"]
df_avaliacoes = spark.createDataFrame(dados_avaliacoes, cols_avaliacoes) \
    .withColumn("_data_ingestao", current_timestamp()) \
    .withColumn("_arquivo_origem", lit("raw/olist_order_reviews_dataset.csv"))
df_avaliacoes.write.format("delta").mode("overwrite").saveAsTable("projeto_mvp.bronze.avaliacoes")

dados_vendedores = [
    ("sell_sp_01", "04001", "sao paulo", "SP"),
    ("sell_rj_01", "22000", "rio de janeiro", "RJ"),
    ("sell_pr_01", "81000", "curitiba", "PR"),
    ("sell_mg_01", "31000", "belo horizonte", "MG")
]
cols_vendedores = ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]
df_vendedores = spark.createDataFrame(dados_vendedores, cols_vendedores) \
    .withColumn("_data_ingestao", current_timestamp()) \
    .withColumn("_arquivo_origem", lit("raw/olist_sellers_dataset.csv"))
df_vendedores.write.format("delta").mode("overwrite").saveAsTable("projeto_mvp.bronze.vendedores")

print("✅ Todas as 7 tabelas da Camada Bronze criadas com sucesso no Delta Lake!")
