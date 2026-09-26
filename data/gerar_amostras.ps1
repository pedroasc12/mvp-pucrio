# Script de geração de amostra realista dos datasets da Olist para teste no Databricks
$rawDir = "data/raw"

# 1. olist_orders_dataset.csv
@"
order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date
ord_001,cust_001,delivered,2018-01-10 10:00:00,2018-01-10 10:15:00,2018-01-11 14:00:00,2018-01-15 16:30:00,2018-01-20 00:00:00
ord_002,cust_002,delivered,2018-02-01 11:30:00,2018-02-01 12:00:00,2018-02-03 10:00:00,2018-02-25 18:00:00,2018-02-18 00:00:00
ord_003,cust_003,delivered,2018-03-05 09:20:00,2018-03-05 09:40:00,2018-03-06 11:00:00,2018-03-12 14:15:00,2018-03-16 00:00:00
ord_004,cust_004,shipped,2018-04-12 15:00:00,2018-04-12 15:30:00,2018-04-13 16:00:00,,2018-04-26 00:00:00
ord_005,cust_005,canceled,2018-05-02 08:00:00,2018-05-02 08:30:00,,,2018-05-15 00:00:00
ord_006,cust_006,delivered,2018-06-14 18:10:00,2018-06-14 18:40:00,2018-06-15 10:00:00,2018-06-20 12:00:00,2018-06-25 00:00:00
ord_007,cust_007,delivered,2018-07-01 14:22:00,2018-07-01 14:50:00,2018-07-03 09:00:00,2018-07-22 17:00:00,2018-07-15 00:00:00
ord_008,cust_008,delivered,2018-07-10 16:05:00,2018-07-10 16:30:00,2018-07-11 11:20:00,2018-07-16 15:00:00,2018-07-25 00:00:00
"@ | Set-Content -Path "$rawDir/olist_orders_dataset.csv" -Encoding UTF8

# 2. olist_order_items_dataset.csv
@"
order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value
ord_001,1,prod_relogio_01,sell_sp_01,2018-01-15 10:00:00,250.00,18.50
ord_002,1,prod_beleza_01,sell_rj_01,2018-02-06 11:00:00,89.90,32.40
ord_003,1,prod_cama_01,sell_sp_01,2018-03-10 09:00:00,140.00,15.20
ord_003,2,prod_cama_01,sell_sp_01,2018-03-10 09:00:00,140.00,15.20
ord_004,1,prod_info_01,sell_pr_01,2018-04-18 15:00:00,1200.00,45.00
ord_006,1,prod_beleza_01,sell_rj_01,2018-06-19 18:00:00,89.90,14.30
ord_007,1,prod_relogio_01,sell_sp_01,2018-07-06 14:00:00,250.00,42.10
ord_008,1,prod_esporte_01,sell_mg_01,2018-07-15 16:00:00,199.90,19.80
"@ | Set-Content -Path "$rawDir/olist_order_items_dataset.csv" -Encoding UTF8

# 3. olist_customers_dataset.csv
@"
customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state
cust_001,user_alpha_1,01310,sao paulo,SP
cust_002,user_alpha_2,40015,salvador,BA
cust_003,user_alpha_3,20020,rio de janeiro,RJ
cust_004,user_alpha_4,80010,curitiba,PR
cust_005,user_alpha_5,30130,belo horizonte,MG
cust_006,user_alpha_6,13010,campinas,SP
cust_007,user_alpha_7,66010,belem,PA
cust_008,user_alpha_8,90010,porto alegre,RS
"@ | Set-Content -Path "$rawDir/olist_customers_dataset.csv" -Encoding UTF8

# 4. olist_products_dataset.csv
@"
product_id,product_category_name,product_name_lenght,product_description_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm
prod_relogio_01,relogios_presentes,45,350,3,400,18,8,12
prod_beleza_01,beleza_saude,38,420,2,250,15,5,10
prod_cama_01,cama_mesa_banho,50,600,4,1800,40,20,30
prod_info_01,informatica_acessorios,55,950,5,3200,35,15,25
prod_esporte_01,esporte_lazer,42,500,2,1100,25,12,18
"@ | Set-Content -Path "$rawDir/olist_products_dataset.csv" -Encoding UTF8

# 5. olist_order_payments_dataset.csv
@"
order_id,payment_sequential,payment_type,payment_installments,payment_value
ord_001,1,credit_card,3,268.50
ord_002,1,credit_card,1,122.30
ord_003,1,boleto,1,310.40
ord_004,1,credit_card,10,1245.00
ord_006,1,voucher,1,104.20
ord_007,1,credit_card,6,292.10
ord_008,1,debit_card,1,219.70
"@ | Set-Content -Path "$rawDir/olist_order_payments_dataset.csv" -Encoding UTF8

# 6. olist_order_reviews_dataset.csv
@"
review_id,order_id,review_score,review_comment_title,review_comment_message,review_creation_date,review_answer_timestamp
rev_001,ord_001,5,Excelente produto,Chegou antes do prazo e muito bem embalado,2018-01-16 00:00:00,2018-01-17 10:00:00
rev_002,ord_002,1,Pessimo atraso,Atrasou mais de uma semana da data combinada,2018-02-26 00:00:00,2018-02-27 12:00:00
rev_003,ord_003,4,Muito bom,Atendeu plenamente as expectativas,2018-03-13 00:00:00,2018-03-14 09:30:00
rev_006,ord_006,5,Recomendo,Produto conforme a foto e entrega rapida,2018-06-21 00:00:00,2018-06-22 15:00:00
rev_007,ord_007,1,Demorou demais,Nao compro mais veio com atraso absurdo,2018-07-23 00:00:00,2018-07-24 11:00:00
rev_008,ord_008,5,Nota dez,Chegou impecavel recomendo a todos,2018-07-17 00:00:00,2018-07-18 16:00:00
"@ | Set-Content -Path "$rawDir/olist_order_reviews_dataset.csv" -Encoding UTF8

# 7. olist_sellers_dataset.csv
@"
seller_id,seller_zip_code_prefix,seller_city,seller_state
sell_sp_01,04001,sao paulo,SP
sell_rj_01,22000,rio de janeiro,RJ
sell_pr_01,81000,curitiba,PR
sell_mg_01,31000,belo horizonte,MG
"@ | Set-Content -Path "$rawDir/olist_sellers_dataset.csv" -Encoding UTF8

Write-Host "Amostras de dados geradas com sucesso em $rawDir!"
