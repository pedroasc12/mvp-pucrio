# 📖 Catálogo de Dados (Data Catalog) - Projeto MVP E-commerce

Este documento detalha o Catálogo de Dados da plataforma implementada no **Databricks Unity Catalog**, contendo o contexto, esquema relacional, tipagem estrita, domínios de valores e linhagem (*data lineage*) de todas as tabelas das camadas da Arquitetura Medalhão.

---

## 1. Visão Geral da Arquitetura Dimensional (Star Schema)

A modelagem analítica da Camada Gold foi estruturada sob o padrão **Esquema Estrela (Star Schema)**:
* **Tabela Fato Central:** `gold.fato_vendas` (Grão: item de produto transacionado por pedido).
* **Tabelas de Dimensão:**
  * `gold.dim_clientes`: Atributos cadastrais e geográficos do consumidor.
  * `gold.dim_produtos`: Características físicas, peso e categoria mercadológica.
  * `gold.dim_vendedores`: Dados cadastrais e região de origem do lojista.
  * `gold.dim_tempo`: Atributos de calendário para análise histórica e sazonalidade.

---

## 2. Catálogo Detalhado das Tabelas da Camada Gold

### 📊 Tabela Fato: `gold.fato_vendas`
* **Contexto:** Tabela central que registra todas as transações de compra e venda de itens individuais, correlacionando indicadores operacionais, prazos logísticos, receita financeira e nota de satisfação.
* **Linhagem (Lineage):** Originada do cruzamento (*inner join*) de `silver.itens_pedido` com `silver.pedidos`, enriquecida com agregação (*left join*) de `silver.avaliacoes`.

| Campo | Tipo de Dado | Descrição | Domínio / Valores Válidos | Linhagem de Origem |
| :--- | :--- | :--- | :--- | :--- |
| `pedido_id` | STRING | Identificador único do pedido | Hash alfanumérico de 32 caracteres | `silver.itens_pedido.pedido_id` |
| `numero_item` | INTEGER | Sequencial do item no mesmo carrinho | Inteiro >= 1 (1, 2, 3...) | `silver.itens_pedido.numero_item` |
| `cliente_id` | STRING | Chave estrangeira para `dim_clientes` | Hash alfanumérico | `silver.pedidos.cliente_id` |
| `produto_id` | STRING | Chave estrangeira para `dim_produtos` | Hash alfanumérico | `silver.itens_pedido.produto_id` |
| `vendedor_id` | STRING | Chave estrangeira para `dim_vendedores` | Hash alfanumérico | `silver.itens_pedido.vendedor_id` |
| `data_compra` | DATE | Chave de data para `dim_tempo` | Formato YYYY-MM-DD | `silver.pedidos.data_compra` |
| `status_pedido` | STRING | Situação operacional do pedido | `delivered`, `shipped`, `canceled`, etc. | `silver.pedidos.status_pedido` |
| `preco` | DOUBLE | Valor de venda do item em R$ | Valor numérico positivo (0.85 a 6.735,00) | `silver.itens_pedido.preco` |
| `valor_frete` | DOUBLE | Valor do frete cobrado pelo item | Valor numérico positivo (0.00 a 409.68) | `silver.itens_pedido.valor_frete` |
| `valor_total_item` | DOUBLE | Preço unitário + frete | Valor numérico >= preco | Calculado (`preco + valor_frete`) |
| `dias_entrega_real` | INTEGER | Dias decorridos entre compra e entrega | Inteiro positivo (0 a 209 dias) | Calculado (`datediff(entrega, compra)`) |
| `dias_atraso` | INTEGER | Diferença de dias entre real e estimado | Inteiro (Negativo = adiantado; Positivo = atrasado) | Calculado (`datediff(entrega, estimado)`) |
| `foi_entregue_com_atraso`| BOOLEAN | Flag de violação do SLA prometido | `true`, `false` ou `null` (se não entregue) | Regra condicional (`dias_atraso > 0`) |
| `nota_avaliacao_media` | DOUBLE | Nota atribuída pelo consumidor | Escala numérica de 1.0 a 5.0 | Média de `silver.avaliacoes.nota_avaliacao` |

---

### 👤 Tabela Dimensão: `gold.dim_clientes`
* **Contexto:** Registra as características cadastrais e espaciais dos compradores cadastrados.
* **Linhagem:** Derivada diretamente de `silver.clientes`, com adição de regra condicional para categorização das macrorregiões brasileiras.

| Campo | Tipo de Dado | Descrição | Domínio / Valores Válidos | Linhagem de Origem |
| :--- | :--- | :--- | :--- | :--- |
| `cliente_id` | STRING (PK) | Chave primária única da dimensão | Hash alfanumérico de 32 caracteres | `silver.clientes.cliente_id` |
| `cliente_unico_id` | STRING | Identificador persistente do cliente | Hash alfanumérico | `silver.clientes.cliente_unico_id` |
| `cep_prefixo` | STRING | 5 primeiros dígitos do CEP | String numérica de 5 dígitos | `silver.clientes.cep_prefixo` |
| `cidade_cliente` | STRING | Município de residência normalizado | Texto minúsculo sem acentuação inválida | `silver.clientes.cidade_cliente` |
| `uf_cliente` | STRING | Estado da federação | 27 siglas oficiais (SP, RJ, MG, BA...) | `silver.clientes.uf_cliente` |
| `regiao_cliente` | STRING | Macrorregião geográfica brasileira | Sudeste, Sul, Nordeste, Centro-Oeste, Norte | Classificação geográfica por UF |

---

### 📦 Tabela Dimensão: `gold.dim_produtos`
* **Contexto:** Cataloga o portfólio de produtos vendidos na plataforma, suas categorias mercadológicas e dimensões volumétricas.
* **Linhagem:** Derivada de `silver.produtos`, com tratamento de nulos em categoria e cálculo de volume em cm³.

| Campo | Tipo de Dado | Descrição | Domínio / Valores Válidos | Linhagem de Origem |
| :--- | :--- | :--- | :--- | :--- |
| `produto_id` | STRING (PK) | Chave primária do produto | Hash alfanumérico | `silver.produtos.produto_id` |
| `categoria_nome` | STRING | Categoria mercadológica normalizada | 74 categorias (ex: beleza_saude, relogios...) | `silver.produtos.categoria_nome` |
| `peso_gramas` | DOUBLE | Peso bruto em gramas | Numérico > 0 | `silver.produtos.peso_gramas` |
| `faixa_peso` | STRING | Agrupamento logístico de peso | Até 500g, 500g a 2kg, 2kg a 5kg, Acima de 5kg | Classificação condicional por peso |
| `comprimento_cm` | DOUBLE | Comprimento do pacote | Numérico > 0 | `silver.produtos.comprimento_cm` |
| `altura_cm` | DOUBLE | Altura do pacote | Numérico > 0 | `silver.produtos.altura_cm` |
| `largura_cm` | DOUBLE | Largura do pacote | Numérico > 0 | `silver.produtos.largura_cm` |
| `volume_cm3` | DOUBLE | Volume cúbico do pacote em cm³ | Numérico > 0 | Calculado (`comprimento * altura * largura`) |

---

### 🏪 Tabela Dimensão: `gold.dim_vendedores`
* **Contexto:** Informações cadastrais e geográficas dos lojistas parceiros integrados ao marketplace.
* **Linhagem:** Derivada de `silver.vendedores`.

| Campo | Tipo de Dado | Descrição | Domínio / Valores Válidos | Linhagem de Origem |
| :--- | :--- | :--- | :--- | :--- |
| `vendedor_id` | STRING (PK) | Chave primária do vendedor | Hash alfanumérico | `silver.vendedores.vendedor_id` |
| `cep_vendedor` | STRING | Prefixo do CEP de postagem | String numérica de 5 dígitos | `silver.vendedores.cep_vendedor` |
| `cidade_vendedor`| STRING | Município do lojista | Texto minúsculo padronizado | `silver.vendedores.cidade_vendedor` |
| `uf_vendedor` | STRING | Estado de origem da postagem | Sigla com 2 caracteres | `silver.vendedores.uf_vendedor` |
| `regiao_vendedor`| STRING | Região de origem | Sudeste, Sul, Nordeste, Centro-Oeste, Norte | Classificação geográfica por UF |

---

### 📅 Tabela Dimensão: `gold.dim_tempo`
* **Contexto:** Dimensão conformada de datas para suporte a relatórios temporais, sazonalidade e séries históricas.
* **Linhagem:** Gerada dinamicamente a partir dos valores únicos de `silver.pedidos.data_compra`.

| Campo | Tipo de Dado | Descrição | Domínio / Valores Válidos | Linhagem de Origem |
| :--- | :--- | :--- | :--- | :--- |
| `data_completa` | DATE (PK) | Data do calendário | 2016-09-04 até 2018-10-17 | `silver.pedidos.data_compra` |
| `ano` | INTEGER | Ano civil da transação | 2016, 2017, 2018 | Extraído da data |
| `mes` | INTEGER | Mês civil | 1 a 12 | Extraído da data |
| `dia` | INTEGER | Dia do mês | 1 a 31 | Extraído da data |
| `dia_semana_numero`| INTEGER | Número do dia da semana (Spark) | 1 (Domingo) a 7 (Sábado) | Extraído da data |
| `nome_dia_semana`| STRING | Nome por extenso do dia | Monday, Tuesday, etc. | Formatado da data |
| `nome_mes` | STRING | Nome por extenso do mês | January, February, etc. | Formatado da data |
| `trimestre` | INTEGER | Trimestre do ano | 1, 2, 3, 4 | Extraído da data |
| `eh_fim_de_semana`| BOOLEAN | Indicador de sábado ou domingo | `true`, `false` | Condicional (`dia_semana in (1, 7)`) |

---

## 3. Catálogo das Tabelas da Camada Silver (Higienizadas)

* `silver.pedidos`: Pedidos higienizados, datas convertidas para timestamp, duplicatas removidas por `pedido_id`, métricas de atraso pré-calculadas.
* `silver.itens_pedido`: Itens de pedidos validados, preços e fretes convertidos para decimal/double, checagem de preços não negativos.
* `silver.clientes`: Clientes únicos, cidades padronizadas em caixa baixa, siglas de estado validadas.
* `silver.produtos`: Catálogo de produtos com categorias tratadas para nulos (`nao_informado`) e medidas tipadas.
* `silver.pagamentos`: Registros financeiros tipados e ordenados por sequencial de transação.
* `silver.avaliacoes`: Avaliações tratadas com notas estritamente limitadas ao intervalo [1, 5].
* `silver.vendedores`: Localização e cadastro de lojistas higienizados.
