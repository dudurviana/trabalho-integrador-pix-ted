# Trabalho Integrador – Aquisição e Preparação de Dados

## Pix e TED: do dado bruto à tabela estruturada

**Aluno:** Vinicius Galoa Fonseca Santos  
**RA:** 2026110561  
**Professor:** Prof. Me. Heron Bussolim Barbosa  

---

## Sumário

- Etapa 0 – Configuração e funções auxiliares
- Etapa 1 – Aquisição das bases e armazenamento dos dados brutos
- Etapa 2 – Classificação das bases e inspeção das amostras
- Etapa 3 – Construção das tabelas estruturadas
- Etapa 4 – Diagnóstico antes do tratamento
- Etapa 5 – Tratamentos e registro de operações
- Etapa 6 – Integração, validações e exportação
- Etapa 7 – Diagnóstico depois do tratamento e reflexão ETL × ELT
- Etapa 8 – Gráfico de comparação entre Pix e TED
- Considerações metodológicas

## Apresentação

Este trabalho apresenta um processo de aquisição, preparação, diagnóstico e integração de dados públicos sobre transações Pix e TED. As informações são obtidas por meio das APIs OData do Banco Central do Brasil (BCB), armazenadas em formato bruto e transformadas em uma tabela mensal para análise comparativa.

O período informado para o Pix começa em novembro de 2020. A coleta descrita no material foi realizada em 13 de novembro de 2026. Como as bases têm calendários de atualização diferentes, a integração considera somente os meses comuns disponíveis nas duas séries.

## Etapa 0 – Configuração e funções auxiliares

Nesta etapa são definidos os diretórios de trabalho, os parâmetros de coleta, os endpoints do BCB e as funções auxiliares. O ambiente utiliza Python com `pandas`, `numpy`, `requests` e `matplotlib`. Os dados brutos são armazenados em JSON compactado (`.json.gz`), enquanto as saídas analíticas são gravadas em CSV e PNG.

Parâmetros principais informados:

| Parâmetro | Valor |
|---|---|
| Início da coleta Pix | 2020-11 |
| Limite superior exclusivo | 202611 |
| Data da coleta | 2026-11-13 |
| Fuso horário | America/Sao_Paulo |
| Diretório de dados brutos | `/content/raw` |
| Diretório de saídas | `/content/saidas` |

As funções auxiliares do código original incluem: avanço de mês (`prox_mes`), requisições com tentativas e espera (`_get`), paginação OData (`baixar_odata`), coleta mensal do Pix (`baixar_pix`), leitura e gravação gzip (`ler_gz`, `salvar_gz`), leitura de amostra dos dados brutos (`ler_inicio_bruto`), normalização e identificação de colunas (`norm_nome`, `achar_col`), conversão de datas e números (`parse_mes`, `para_numero`), contagem de casas decimais (`casas_dec`) e diagnóstico de qualidade (`diagnosticar`).

Dependências sugeridas para reproduzir o processamento:

```text
matplotlib
numpy
pandas
requests
```

## Etapa 1 – Aquisição das bases e armazenamento dos dados brutos

Foram consultadas duas APIs oficiais do Banco Central do Brasil, disponibilizadas pelo portal Olinda/OData:

1. **Pix – Estatísticas de Transações Pix** (`Pix_DadosAbertos / EstatisticasTransacoesPix`): fornece quantidade e valor das transações por mês de referência, além de dimensões e desagregações. A consulta é parametrizada pelo campo `Database` no formato `AAAAMM`. A coleta foi feita mês a mês desde `202011`; os arquivos foram armazenados em `/content/raw/pix_mensal/pix_AAAAMM.json.gz`.
2. **TED – Meios de Pagamentos Mensal** (`MPV_DadosAbertos / MeiosdePagamentosMensalDA`): apresenta indicadores mensais dos meios de pagamento em formato largo, incluindo `quantidadeTED` e `valorTED`. O retorno foi salvo em `/content/raw/ted_bruto_<timestamp>.json.gz`.

Os arquivos brutos preservam o envelope e os registros retornados pela API, acompanhados de metadados de fonte, endpoint, data da coleta e total de registros. A paginação é percorrida usando `@odata.nextLink`.

**Resultado informado:** foram salvos 72 arquivos mensais do Pix, totalizando 32.261.695 registros nos arquivos; a consulta TED retornou 71 registros. O código usa retentativas para erros HTTP e permite reutilizar arquivos Pix já existentes para facilitar a retomada da coleta.

## Etapa 2 – Classificação das bases e inspeção das amostras

As bases têm origem **externa, pública e aberta**: são produzidas pelo BCB, consultáveis publicamente e disponibilizadas por API web sem autenticação, conforme a descrição do projeto.

No transporte, os retornos são documentos JSON em um envelope OData, com metadados (`@odata.context`) e uma lista de registros (`value`). Por isso, o formato bruto é semiestruturado. Na amostra examinada, os registros de `value` apresentaram chaves uniformes e nenhum campo aninhado; extraído o array, os registros podem ser organizados em formato tabular.

| Base | Envelope com `@odata.context` | Esquema uniforme na amostra | Campos aninhados | Campos por registro |
|---|---:|---:|---:|---:|
| Pix | Sim | Sim | Não | 3 |
| TED | Sim | Sim | Não | 13 |

A classificação das principais variáveis da tabela analítica é:

| Variável | Tipo estatístico | Tipo computacional após tratamento | Papel analítico |
|---|---|---|---|
| `mes_referencia` | Temporal/ordinal mensal | `datetime64[ns]`, primeiro dia do mês | Chave de integração e eixo temporal |
| Quantidade | Quantitativa discreta na origem | `float64`, milhões de transações | Volume operacional |
| Valor | Quantitativa contínua | `float64`, R$ bilhões | Volume financeiro |
| Modalidade | Categórica nominal | Identificada pelos prefixos das colunas | Distingue Pix e TED |

## Etapa 3 – Construção das tabelas estruturadas

O conteúdo de `value` foi extraído dos arquivos JSON compactados. Para cada arquivo Pix, o código utiliza o mês associado ao arquivo e conserva os registros cujo `AnoMes` corresponde a esse mês, evitando repetir registros que possam aparecer em consultas de bases mensais sucessivas. Em seguida, foram projetadas as colunas necessárias à análise:

- Pix: `AnoMes`, `QUANTIDADE` e `VALOR`;
- TED: `AnoMes`, `quantidadeTED` e `valorTED`.

Antes da conversão, o schema informado foi:

| Tabela | Colunas projetadas | Tipos originais | Linhas |
|---|---|---|---:|
| `df_pix_raw` | `AnoMes`, `QUANTIDADE`, `VALOR` | `int64`, `int64`, `float64` | 768.106 |
| `df_ted_raw` | `AnoMes`, `quantidadeTED`, `valorTED` | `object`, `float64`, `float64` | 71 |

A chave planejada para a integração é `mes_referencia`, obtida pela conversão de `AnoMes` para uma data correspondente ao primeiro dia do mês. O join é validado como um relacionamento `one_to_one` depois da agregação mensal.

## Etapa 4 – Diagnóstico antes do tratamento

O diagnóstico considera completude, consistência, precisão e atualidade. São avaliados nulos e valores inválidos; tipos de dados; linhas duplicadas e multiplicidade por mês; valores negativos e zeros; casas decimais; variações mensais atípicas, identificadas por `|z-score| > 3`; período coberto e defasagem em relação à data de coleta.

**Resultados informados antes do tratamento:**

| Métrica | Pix | TED |
|---|---:|---:|
| Linhas | 768.106 | 71 |
| Tipo de `AnoMes` | `int64` | `object` |
| Nulos em mês, quantidade e valor | 0 | 0 |
| Duplicidades de linhas | 70.831 | 0 |
| Registros repetidos por mês (`dup_mes`) | 768.034 | 0 |
| Valores negativos | 0 | 0 |
| Primeiro mês | 2020-11 | 2020-11 |
| Último mês | 2026-10 | 2026-09 |
| Meses únicos | 72 | 71 |
| Defasagem informada na coleta | 1 mês | 2 meses |

O diagnóstico também registrou variações percentuais atípicas na quantidade do Pix em 2020-12 e na quantidade da TED em 2021-01. Uma variação atípica é um sinal para inspeção, não uma evidência automática de erro: pode refletir a implantação ou o crescimento inicial de um meio de pagamento.

## Etapa 5 – Tratamentos e registro de operações

As regras descritas no código são:

- **Datas:** converter `AnoMes` para `mes_referencia`, usando o primeiro dia do mês. Linhas com mês inválido seriam removidas e registradas no log.
- **Ausências:** manter métricas ausentes como `NaN`, sem imputação, para não inventar movimentação financeira.
- **Valores negativos:** manter e registrar para auditoria, pois podem representar ajustes ou estornos; no resultado informado não havia valores negativos.
- **Granularidade:** agrupar por mês para produzir uma linha por mês. O material informa que o Pix tem vários registros por mês e a TED já é mensal.
- **Escalas:** expressar quantidade em milhões de transações e valor em R$ bilhões. Os multiplicadores declarados foram 1 para quantidade e 1 para valor do Pix; 1.000 para quantidade e 1.000.000 para valor da TED.

O log apresentado informa que nenhuma linha foi removida por data inválida, não havia ausências ou valores negativos e que o Pix foi agregado de 768.106 linhas para 72 meses. A TED manteve 71 meses. As transformações são realizadas com `pandas`, sem imputação de métricas.

## Etapa 6 – Integração, validações e exportação

As tabelas mensais de Pix e TED são integradas por `mes_referencia` com `inner join`. Assim, são mantidos apenas os meses presentes em ambas as séries. O código valida a cardinalidade `one_to_one`, ordena a tabela por data e calcula o ticket médio em reais:

- `ticket_medio = valor_financeiro / quantidade_de_transacoes`.

A tabela final possui, conforme o material fornecido, as colunas `mes_referencia`, `pix_quantidade_mi`, `pix_valor_bi`, `ted_quantidade_mi`, `ted_valor_bi`, `pix_ticket_medio_rs` e `ted_ticket_medio_rs`.

**Validação do join informada:** 72 meses Pix, 71 meses TED, 71 linhas na tabela integrada; nenhuma chave duplicada, nenhum nulo, período comum de 2020-11-01 a 2026-09-01 e nenhum mês faltante dentro do intervalo final. A tabela foi exportada como CSV em `/content/saidas/pix_vs_ted_mensal_20261113_000000.csv`.

### Estatísticas descritivas informadas

| Variável | Média | Desvio-padrão | Mínimo | Mediana | Máximo |
|---|---:|---:|---:|---:|---:|
| Pix – quantidade (milhões) | 3.577,058958 | 2.127,097458 | 32,893545 | 3.732,036924 | 6.959,622300 |
| Pix – valor (R$ bilhões) | 1.508,791846 | 954,477600 | 28,972074 | 1.435,565360 | 3.255,146244 |
| TED – quantidade (milhões) | 0,088662 | 0,029758 | 0,056630 | 0,077250 | 0,190449 |
| TED – valor (R$ bilhões) | 0,003483 | 0,000436 | 0,002263 | 0,003568 | 0,004422 |
| Pix – ticket médio (R$) | 453,835963 | 109,167079 | 356,256410 | 412,660602 | 880,782959 |
| TED – ticket médio (R$) | 43,249899 | 14,075031 | 14,812420 | 46,639321 | 62,665642 |

Os números acima reproduzem os resultados apresentados no material original. Recomenda-se confirmar novamente as estatísticas depois da validação de granularidade descrita em “Considerações metodológicas”.

## Etapa 7 – Diagnóstico depois do tratamento e comparação antes × depois

Após o tratamento, o material informa que as duas bases possuem `mes_referencia` como `datetime64[ns]`, métricas como `float64` e uma linha por mês, sem duplicidades mensais. O Pix passa de 768.106 linhas para 72 meses; a TED permanece com 71 meses. A tabela integrada tem 71 linhas, 7 colunas, nenhum nulo e nenhuma duplicidade na chave, abrangendo 2020-11-01 a 2026-09-01.

As principais melhorias descritas são:

1. **Unificação temporal:** `AnoMes`, originalmente `int64` no Pix e `object` na TED, passa a ser uma data mensal padronizada.
2. **Alinhamento da granularidade:** as métricas são agregadas para uma linha mensal antes do relacionamento.
3. **Padronização de escala:** as métricas são convertidas para milhões de transações e R$ bilhões; o ticket médio é calculado em reais.
4. **Rastreabilidade:** ausências, valores negativos e linhas removidas são registrados sem imputações automáticas.

### Reflexão: ETL ou ELT?

O fluxo é mais próximo de **ETL (Extract, Transform, Load)**. Os dados são extraídos das APIs do BCB e guardados em formato bruto; as transformações (normalização, tipagem, agregação, escalonamento e integração) são executadas em Python/pandas; por fim, a tabela analítica é carregada em CSV. O processo não depende de um data warehouse para executar as transformações no destino, embora a cópia bruta também preserve a possibilidade de reprocessamento.

## Etapa 8 – Gráfico de comparação entre Pix e TED

O gráfico descrito contém dois painéis sincronizados no tempo:

- **Quantidade mensal:** milhões de transações, em escala logarítmica para facilitar a visualização das séries com magnitudes diferentes.
- **Valor financeiro mensal:** R$ bilhões.

As séries são identificadas por cores distintas (Pix em verde e TED em azul), com grade, legendas, títulos e indicação das fontes. O período informado no título é novembro de 2020 a setembro de 2026. O arquivo PNG foi salvo em `/content/saidas/grafico_pix_vs_ted_20261113_000000.png`.

## Considerações metodológicas importantes

Antes de apresentar os valores como resultados definitivos, é importante verificar a granularidade da fonte Pix. O endpoint pode apresentar diferentes dimensões ou categorias no mesmo mês. O código descrito projeta somente `AnoMes`, `QUANTIDADE` e `VALOR` e depois soma todas as linhas por mês. Se a resposta contiver totais gerais junto com categorias detalhadas, ou recortes que não sejam mutuamente exclusivos, essa soma pode contar os mesmos movimentos mais de uma vez. O próprio log do processamento sinaliza essa questão.

Portanto, deve-se consultar a definição dos campos e dimensões do BCB e selecionar os registros que representem o total mensal, ou agregar somente categorias mutuamente exclusivas apropriadas. Em seguida, devem ser recalculados a tabela, os tickets médios, as estatísticas e o gráfico. Até essa validação, os números da Etapa 6 devem ser entendidos como resultados reproduzidos do processamento informado, não como validação independente dos totais oficiais.

Também convém registrar no relatório que a coleta TED terminou em setembro de 2026, enquanto o Pix chegou a outubro de 2026; por isso, o `inner join` exclui outubro e termina em setembro. Essa diferença é esperada quando as fontes têm ritmos de atualização distintos.

## Fontes

- Banco Central do Brasil – API Olinda, serviço `Pix_DadosAbertos`, endpoint `EstatisticasTransacoesPix`.
- Banco Central do Brasil – API Olinda, serviço `MPV_DadosAbertos`, endpoint `MeiosdePagamentosMensalDA`.

## Arquivos gerados no ambiente original

- Dados brutos Pix: `/content/raw/pix_mensal/pix_AAAAMM.json.gz`
- Dados brutos TED: `/content/raw/ted_bruto_<timestamp>.json.gz`
- Tabela integrada: `/content/saidas/pix_vs_ted_mensal_20261113_000000.csv`
- Gráfico: `/content/saidas/grafico_pix_vs_ted_20261113_000000.png`

> Os caminhos `/content/...` pertencem ao ambiente Google Colab original. Os arquivos não foram incluídos neste repositório, pois não foram anexados à conversa. Este documento foi organizado a partir do material e dos resultados fornecidos pelo autor.
