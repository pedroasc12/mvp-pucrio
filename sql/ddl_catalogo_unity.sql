-- ============================================================================
-- PROJETO MVP: DATA LAKEHOUSE E-COMMERCE BRASIL (PUC-RIO)
-- DDL DE CRIAÇÃO DO CATÁLOGO DE DADOS E SCHEMAS NO UNITY CATALOG
-- ============================================================================

-- 1. Catálogo Principal
CREATE CATALOG IF NOT EXISTS projeto_mvp
COMMENT 'Catálogo central do projeto de Engenharia de Dados para análise de marketplace';

USE CATALOG projeto_mvp;

-- 2. Schemas da Arquitetura Medalhão
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Camada Bronze: Armazenamento dos dados brutos com metadados de auditoria e formato original';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Camada Silver: Dados limpos, tipados, deduplicados e enriquecidos com regras de negócio';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Camada Gold: Modelo dimensional Star Schema e tabelas agregadas analíticas';

-- 3. Documentação e Metadados das Tabelas da Camada Gold

-- Dimensão Clientes
COMMENT ON TABLE gold.dim_clientes IS 'Dimensão de clientes com localização geográfica e macrorregião brasileira';
COMMENT ON COLUMN gold.dim_clientes.cliente_id IS 'Chave primária do cliente em relação ao pedido';
COMMENT ON COLUMN gold.dim_clientes.cliente_unico_id IS 'Identificador persistente do cliente no ecossistema Olist';
COMMENT ON COLUMN gold.dim_clientes.cidade_cliente IS 'Nome normalizado da cidade do cliente (em minúsculas)';
COMMENT ON COLUMN gold.dim_clientes.uf_cliente IS 'Sigla da Unidade Federativa com 2 caracteres';
COMMENT ON COLUMN gold.dim_clientes.regiao_cliente IS 'Macrorregião IBGE (Sudeste, Sul, Nordeste, Norte, Centro-Oeste)';

-- Dimensão Produtos
COMMENT ON TABLE gold.dim_produtos IS 'Dimensão com características físicas e categorização de produtos';
COMMENT ON COLUMN gold.dim_produtos.produto_id IS 'Chave primária única do produto';
COMMENT ON COLUMN gold.dim_produtos.categoria_nome IS 'Categoria mercadológica padronizada do produto';
COMMENT ON COLUMN gold.dim_produtos.peso_gramas IS 'Peso do produto em gramas';
COMMENT ON COLUMN gold.dim_produtos.faixa_peso IS 'Classificação em faixas de peso para logística (Até 500g, 500g a 2kg, etc.)';
COMMENT ON COLUMN gold.dim_produtos.volume_cm3 IS 'Volume físico calculado (comprimento * altura * largura)';

-- Dimensão Vendedores
COMMENT ON TABLE gold.dim_vendedores IS 'Dimensão com os dados cadastrais e localização geográfica dos lojistas';
COMMENT ON COLUMN gold.dim_vendedores.vendedor_id IS 'Chave primária única do vendedor';
COMMENT ON COLUMN gold.dim_vendedores.cidade_vendedor IS 'Cidade do lojista normalizada';
COMMENT ON COLUMN gold.dim_vendedores.uf_vendedor IS 'Estado do lojista com 2 dígitos';
COMMENT ON COLUMN gold.dim_vendedores.regiao_vendedor IS 'Macrorregião de origem do produto';

-- Dimensão Tempo
COMMENT ON TABLE gold.dim_tempo IS 'Dimensão temporal para suporte a agregações históricas e sazonais';
COMMENT ON COLUMN gold.dim_tempo.data_completa IS 'Data de calendário (formato YYYY-MM-DD)';
COMMENT ON COLUMN gold.dim_tempo.ano IS 'Ano numérico da transação';
COMMENT ON COLUMN gold.dim_tempo.mes IS 'Mês numérico (1 a 12)';
COMMENT ON COLUMN gold.dim_tempo.trimestre IS 'Trimestre do ano (1 a 4)';
COMMENT ON COLUMN gold.dim_tempo.eh_fim_de_semana IS 'Flag booleano indicando se a data corresponde a sábado ou domingo';

-- Tabela Fato Central: Vendas
COMMENT ON TABLE gold.fato_vendas IS 'Tabela fato transacional com grão a nível de item individual vendido';
COMMENT ON COLUMN gold.fato_vendas.pedido_id IS 'Chave de relacionamento do pedido (FK com pedidos)';
COMMENT ON COLUMN gold.fato_vendas.numero_item IS 'Sequencial do item no carrinho de compras';
COMMENT ON COLUMN gold.fato_vendas.cliente_id IS 'Chave estrangeira para a dimensão dim_clientes';
COMMENT ON COLUMN gold.fato_vendas.produto_id IS 'Chave estrangeira para a dimensão dim_produtos';
COMMENT ON COLUMN gold.fato_vendas.vendedor_id IS 'Chave estrangeira para a dimensão dim_vendedores';
COMMENT ON COLUMN gold.fato_vendas.data_compra IS 'Chave estrangeira para a dimensão dim_tempo';
COMMENT ON COLUMN gold.fato_vendas.preco IS 'Valor unitário de venda do produto em Reais';
COMMENT ON COLUMN gold.fato_vendas.valor_frete IS 'Custo logístico do frete atribuído ao item em Reais';
COMMENT ON COLUMN gold.fato_vendas.valor_total_item IS 'Soma de preço e frete representando a receita transacionada';
COMMENT ON COLUMN gold.fato_vendas.dias_entrega_real IS 'Tempo decorrido entre a compra e a entrega ao cliente em dias corridos';
COMMENT ON COLUMN gold.fato_vendas.dias_atraso IS 'Diferença entre a data real de entrega e a data estimada';
COMMENT ON COLUMN gold.fato_vendas.foi_entregue_com_atraso IS 'Flag indicando se o pedido violou o prazo estimado acordado';
COMMENT ON COLUMN gold.fato_vendas.nota_avaliacao_media IS 'Nota de avaliação atribuída pelo comprador (escala 1 a 5)';
