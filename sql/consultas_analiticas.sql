-- ============================================================================
-- PROJETO MVP: CONSULTAS ANALÍTICAS PARA AS PERGUNTAS DE NEGÓCIO
-- ============================================================================

USE CATALOG projeto_mvp;
USE SCHEMA gold;

-- ----------------------------------------------------------------------------
-- PERGUNTA 1: Rentabilidade e Volume por Categoria
-- Quais categorias geram maior receita total e qual a relação entre volume e ticket médio?
-- ----------------------------------------------------------------------------
SELECT 
    categoria_nome,
    total_itens_vendidos,
    faturamento_bruto_total,
    ticket_medio_produto,
    nota_media_satisfacao
FROM projeto_mvp.gold.kpi_categoria_performance
WHERE categoria_nome != 'nao_informado'
ORDER BY faturamento_bruto_total DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- PERGUNTA 2: Logística vs Satisfação do Cliente (NPS / Avaliações)
-- Qual é a correlação entre atrasos na entrega e a nota média de avaliação do cliente?
-- ----------------------------------------------------------------------------
SELECT 
    CASE 
        WHEN foi_entregue_com_atraso = true THEN 'Entregue com Atraso'
        WHEN foi_entregue_com_atraso = false THEN 'Entregue no Prazo / Adiantado'
        ELSE 'Não Entregue / Cancelado'
    END AS status_logistico,
    COUNT(DISTINCT pedido_id) AS total_pedidos,
    ROUND(AVG(dias_entrega_real), 1) AS media_dias_entrega,
    ROUND(AVG(nota_avaliacao_media), 2) AS media_nota_avaliacao,
    ROUND(SUM(CASE WHEN nota_avaliacao_media = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS pct_detratores_nota_1
FROM projeto_mvp.gold.fato_vendas
WHERE status_pedido = 'delivered'
GROUP BY foi_entregue_com_atraso
ORDER BY media_nota_avaliacao DESC;

-- ----------------------------------------------------------------------------
-- PERGUNTA 3: Formas de Pagamento e Alavancagem do Ticket Médio
-- Qual a distribuição dos meios de pagamento e o impacto do parcelamento no ticket médio?
-- ----------------------------------------------------------------------------
-- A) Distribuição geral por método de pagamento
SELECT 
    tipo_pagamento,
    COUNT(1) AS total_transacoes,
    ROUND(SUM(valor_pagamento), 2) AS faturamento_total,
    ROUND(AVG(valor_pagamento), 2) AS ticket_medio,
    ROUND(AVG(qtd_parcelas), 1) AS media_parcelas
FROM projeto_mvp.silver.pagamentos
GROUP BY tipo_pagamento
ORDER BY faturamento_total DESC;

-- B) Impacto do número de parcelas no Cartão de Crédito
SELECT 
    qtd_parcelas,
    COUNT(1) AS total_compras,
    ROUND(AVG(valor_pagamento), 2) AS ticket_medio_por_faixa_parcela
FROM projeto_mvp.silver.pagamentos
WHERE tipo_pagamento = 'credit_card' AND qtd_parcelas BETWEEN 1 AND 10
GROUP BY qtd_parcelas
ORDER BY qtd_parcelas ASC;

-- ----------------------------------------------------------------------------
-- PERGUNTA 4: Geografia e Fluxo Logístico Interestadual
-- Quais estados concentram o maior volume de vendas em comparação aos vendedores?
-- ----------------------------------------------------------------------------
SELECT 
    c.uf_cliente,
    c.regiao_cliente,
    COUNT(DISTINCT f.pedido_id) AS total_pedidos,
    ROUND(SUM(f.valor_total_item), 2) AS faturamento_total,
    ROUND(AVG(f.dias_entrega_real), 1) AS media_dias_entrega,
    ROUND(AVG(f.valor_frete), 2) AS custo_medio_frete
FROM projeto_mvp.gold.fato_vendas f
JOIN projeto_mvp.gold.dim_clientes c ON f.cliente_id = c.cliente_id
WHERE f.status_pedido = 'delivered'
GROUP BY c.uf_cliente, c.regiao_cliente
ORDER BY faturamento_total DESC
LIMIT 10;
