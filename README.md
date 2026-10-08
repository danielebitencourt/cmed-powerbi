# Automação CMED → Power BI

Importação automática mensal dos preços oficiais de medicamentos da CMED/ANVISA (PMC e PF/PMVG) para o Power BI, com relacionamento EAN × Estado e histórico de alíquotas de ICMS.

---

## Visão geral

**Camada ETL (Python + GitHub Actions)** — o `etl_cmed.py` baixa os arquivos PMC e PF/PMVG do site da ANVISA, trata a qualidade dos dados (EAN, separadores decimais, asteriscos) e faz o unpivot para uma tabela fato com granularidade Apresentação (GGREM × EAN) × Tipo de Preço × Alíquota × Competência. A fato guarda só **chaves** (`CHAVE_PRODUTO`, `CODIGO_GGREM`, `EAN`) e **medidas** (preço, regime de preço); as descrições ficam na `dim_medicamento`. A fato **não tem UF**: os estados se ligam a ela pela alíquota, através de uma tabela ponte. O GitHub Actions executa o script automaticamente e versiona o resultado neste repositório.

**Camada analítica (Power BI)** — consome os arquivos de `dados/processed/` em um modelo estrela com as dimensões Medicamento, Estado e Calendário, mais as tabelas de histórico de alíquotas.

---

## Estrutura do repositório

```
cmed-powerbi/
├── etl_cmed.py                    ← ETL principal (download, tratamento, exportação)
├── gerar_historico_aliquotas.py   ← Gera o histórico de alíquotas de ICMS
├── aliquotas_icms.csv             ← FONTE DA VERDADE das alíquotas por UF
├── config.yaml                    ← Configurações (URL, pastas, retry, alertas)
├── requirements.txt               ← Dependências Python (versões fixadas)
│
├── dados/
│   └── processed/                 ← Saída para o Power BI
│       ├── fato/
│       │   └── fato_precos_AAAA-MM.parquet   ← um arquivo por competência
│       ├── dim_medicamento.csv
│       ├── dim_estado.csv
│       ├── dim_calendario.csv
│       ├── dim_aliquota.csv
│       ├── ponte_aliquota_estado.csv
│       ├── dim_aliquota_historico.csv
│       └── fato_aliquota_mensal.csv
│
└── .github/
    ├── workflows/cmed.yml         ← Agendamento do ETL no GitHub Actions
    └── cmed/estado.json           ← Última competência processada
```

Pastas geradas só na execução e **não versionadas** (ver `.gitignore`): `dados/raw/` (arquivos originais da ANVISA), `dados/historico/` e `logs/`.

---

## Execução automática (GitHub Actions)

O workflow `ETL CMED Mensal` roda nos dias **1, 5, 10 e 15 de cada mês às 11:00 UTC (08:00 de Brasília)**. As execuções extras garantem a captura do arquivo mesmo que a ANVISA publique com atraso.

A cada execução ele:
1. Instala as dependências de `requirements.txt`.
2. Executa `python etl_cmed.py`.
3. Executa `python gerar_historico_aliquotas.py` (dAliquota, ponte e histórico de alíquotas).
4. Faz commit de `dados/processed/` e `.github/cmed/estado.json`, abortando se algum arquivo passar de 100 MB.

Para rodar manualmente: aba **Actions → ETL CMED Mensal → Run workflow**.

### Alertas por e-mail (opcional)

Desativados por padrão. Para ativar, cadastre em **Settings → Secrets and variables → Actions**:

| Secret | Conteúdo |
|---|---|
| `EMAIL_ALERTA_ATIVO` | `true` |
| `SMTP_REMETENTE` | e-mail remetente |
| `SMTP_SENHA` | senha de app do e-mail |
| `SMTP_DESTINATARIOS` | destinatários |

Nunca coloque senhas no `config.yaml` — ele é versionado.

---

## Execução local

Pré-requisitos: Python 3.10+ (o Actions usa 3.12) e acesso a gov.br/anvisa.

```bash
pip install -r requirements.txt
python etl_cmed.py                         # competência atual
python etl_cmed.py --competencia 2026-07   # forçar uma competência
python etl_cmed.py --config outro.yaml     # configuração alternativa
```

---

## Alíquotas de ICMS

As alíquotas por UF **não ficam mais no código**. A fonte da verdade é `aliquotas_icms.csv`, um log com vigência:

```
UF,ALIQUOTA_ICMS,VIGENCIA_INICIO,FONTE,OBS
BA,"20,5%",2024-01-01,RICMS-BA,
```

Para cada competência e UF vale a linha com a `VIGENCIA_INICIO` mais recente até o 1º dia do mês. É esse arquivo que define a ponte alíquota → estados.

### Registrar uma mudança de alíquota

1. **Não edite a linha antiga.** Adicione uma **nova linha** em `aliquotas_icms.csv` com a nova alíquota, a `VIGENCIA_INICIO` e a fonte legal.
2. Rode localmente:
   ```bash
   python gerar_historico_aliquotas.py                # grade até o mês atual
   python gerar_historico_aliquotas.py --ate 2027-06  # projetar até jun/2027
   ```
3. Faça commit de `aliquotas_icms.csv` e dos dois CSVs gerados em `dados/processed/`.

O script fecha automaticamente a vigência anterior e recalcula:
- `dim_aliquota.csv` — uma linha por `CHAVE_ALIQUOTA` (`AAAA-MM|alíquota`), com as UFs que usavam aquela alíquota no mês.
- `ponte_aliquota_estado.csv` — `CHAVE_ALIQUOTA` × `ESTADO_UF`: liga a fato aos estados.
- `dim_aliquota_historico.csv` — uma linha por período de vigência de cada UF (início, fim, vigente, fonte).
- `fato_aliquota_mensal.csv` — grade mês × UF com a alíquota vigente; a coluna `MUDOU` marca o mês da mudança.

> O workflow do GitHub Actions executa `gerar_historico_aliquotas.py` em toda execução agendada. Rodar o passo 2 manualmente só é necessário para refletir a mudança no Power BI antes da próxima execução.

---

## Uso no Power BI

### Fonte de dados

| Tabela | Arquivo | Como importar |
|---|---|---|
| `fCMED_Precos` | `dados/processed/fato/*.parquet` | **Obter Dados → Pasta → Combinar** (lê todos os meses) |
| `dMedicamento` | `dim_medicamento.csv` | Texto/CSV (UTF-8) |
| `dEstado` | `dim_estado.csv` | Texto/CSV (UTF-8) |
| `dCalendario` | `dim_calendario.csv` | Texto/CSV (UTF-8) |
| `dAliquota` | `dim_aliquota.csv` | Texto/CSV (UTF-8) |
| `pAliquotaEstado` | `ponte_aliquota_estado.csv` | Texto/CSV (UTF-8) |
| `dAliquotaHistorico` | `dim_aliquota_historico.csv` | Texto/CSV (UTF-8) |
| `fAliquotaMensal` | `fato_aliquota_mensal.csv` | Texto/CSV (UTF-8) |

Os arquivos podem ser lidos de uma cópia local do repositório ou direto do GitHub (`https://raw.githubusercontent.com/danielebitencourt/cmed-powerbi/main/dados/processed/...`).

### Relacionamentos

| De | Para | Cardinalidade | Filtro cruzado |
|---|---|---|---|
| fCMED_Precos[CHAVE_PRODUTO] | dMedicamento[CHAVE_PRODUTO] | Muitos:1 | Único |
| fCMED_Precos[CHAVE_ALIQUOTA] | dAliquota[CHAVE_ALIQUOTA] | Muitos:1 | Único |
| fCMED_Precos[DATA_REFERENCIA] | dCalendario[DATA] | Muitos:1 | Único |
| pAliquotaEstado[CHAVE_ALIQUOTA] | dAliquota[CHAVE_ALIQUOTA] | Muitos:1 | **Ambos** |
| pAliquotaEstado[ESTADO_UF] | dEstado[ESTADO_UF] | Muitos:1 | Único |
| fAliquotaMensal[ESTADO_UF] | dEstado[ESTADO_UF] | Muitos:1 | Único |
| fAliquotaMensal[COMPETENCIA_DATA] | dCalendario[DATA] | Muitos:1 | Único |
| dAliquotaHistorico[ESTADO_UF] | dEstado[ESTADO_UF] | Muitos:1 | Único |

**Como o filtro de estado chega à fato:** dEstado → pAliquotaEstado → dAliquota → fCMED_Precos. A relação ponte↔dAliquota precisa ser **bidirecional** (Ambos); todas as outras são unidirecionais.

**Por que a chave inclui a competência:** a alíquota de uma UF muda com o tempo. `2026-09|20%` liga os preços de 20% de setembro às UFs que usavam 20% *em setembro*; se em 2027 SP passar para 20%, os meses antigos continuam corretos.

**Comportamento esperado:**
- Filtrando uma UF, aparecem os preços da alíquota dela naquele mês.
- Filtrando várias UFs com a mesma alíquota, cada preço aparece **uma vez** (não é duplicado por estado).
- Alíquotas sem estado (`Sem Impostos`, `0%`, `12%`, `17,5%`, `21%`) aparecem sem filtro de UF e somem quando uma UF é selecionada. Use `dAliquota[QTD_UFS] = 0` para isolá-las.

---

## Modelo de dados

```
┌────────────────┐    ┌──────────────────┐      ┌──────────────┐
│  dMedicamento  │ 1 *│  fCMED_Precos    │ *  1 │ dCalendario  │
│ CHAVE_PRODUTO  │───<│  CHAVE_PRODUTO   │>─────│   DATA (PK)  │
│   (PK=GGREM|EAN)│   │  CODIGO_GGREM    │      └──────┬───────┘
│  GGREM, EAN,   │    │  EAN             │             │ 1
│  REGISTRO...   │    │  TIPO_PRECO      │             │ *
└────────────────┘    │  REGIME_PRECO    │    ┌────────┴─────────┐
                      │  ALIQUOTA_ICMS   │    │ fAliquotaMensal  │
                      │  CHAVE_ALIQUOTA  │    │ COMPETENCIA_DATA │
                      │  VALOR           │    │ ESTADO_UF        │
                      └────────┬─────────┘    └────────┬─────────┘
                               │ *                     │ *
                               │ 1                     │
                      ┌────────┴─────────┐             │ *
                      │    dAliquota     │             │
                      │ CHAVE_ALIQUOTA PK│             │
                      └────────┬─────────┘             │
                               │ 1  ⇅ (Ambos)          │
                               │ *                     │
                      ┌────────┴─────────┐             │
                      │ pAliquotaEstado  │             │
                      │ CHAVE_ALIQUOTA   │             │
                      │ ESTADO_UF        │             │
                      └────────┬─────────┘             │
                               │ *                     │
                               │ 1                     │
                      ┌────────┴─────────┐ 1           │
                      │     dEstado      │─────────────┘
                      │  ESTADO_UF (PK)  │──<  dAliquotaHistorico
                      └──────────────────┘
```

### Colunas principais da fato (`fato_precos_AAAA-MM.parquet`)

| Coluna | Descrição |
|---|---|
| `CHAVE_PRODUTO` | `GGREM\|EAN` — liga à `dim_medicamento` (relação do Power BI) |
| `CODIGO_GGREM` | Código GGREM da apresentação (chave única na CMED) |
| `EAN` | EAN-13 tratado (código de barras; 1 GGREM pode ter até 3) |
| `TIPO_PRECO` | `PMC` ou `PF` |
| `REGIME_PRECO` | `Regulado` ou `Liberado`, conforme a lista (PMC/PF) daquela competência |
| `ALIQUOTA_ICMS` / `ALIQUOTA_ICMS_PCT` | Alíquota em texto (`20,5%`) e em número (`0,205`) |
| `CHAVE_ALIQUOTA` | `AAAA-MM\|alíquota` — liga à dAliquota (e, pela ponte, aos estados) |
| `VALOR` | Preço em R$ |
| `COMPETENCIA` / `COMPETENCIA_DATA` / `DATA_REFERENCIA` | Mês de referência (`AAAA-MM` e data do 1º dia) |
| `FLAG_ASTERISCO` | Valor veio com `*` na planilha da CMED |
| `EAN_INVALIDO` | Dígito verificador do EAN não confere |
| `DATA_CARGA` | Data e hora do processamento |

As descrições (`PRODUTO`, `APRESENTACAO`, `LABORATORIO`, `SUBSTANCIA`, `REGISTRO`, `CLASSE_TERAPEUTICA`, `TARJA`, `TIPO_PRODUTO`, `CNPJ`...) ficam na `dim_medicamento`, uma linha por `CHAVE_PRODUTO`.

---

## Tratamentos de qualidade aplicados

| Problema | Tratamento |
|---|---|
| EAN com zeros faltando | `zfill(13)` — preenchimento com zeros à esquerda |
| EAN inválido (dígito verificador) | Sinalizado com `EAN_INVALIDO = True` |
| Valor com ponto como decimal | Detecta e corrige separadores BR/US |
| Asterisco (*) no valor | Remove e sinaliza `FLAG_ASTERISCO = True` |
| Cabeçalho em posição variável | Detecta a linha com "SUBSTÂNCIA" automaticamente |
| Colunas renomeadas pela ANVISA | Mapeamento de nomes alternativos |
| Novas colunas de alíquota | Colunas PF/PMC detectadas dinamicamente |
| Site fora do ar | Retry 3× com intervalo de 30 s + alerta por e-mail (se ativo) |

---

## Notas técnicas

- A fato é gravada em **Parquet particionado por mês**, sem UF (uma linha por EAN × tipo × alíquota): comprime muito (centenas de MB em CSV viram poucos MB), fica abaixo do limite de 100 MB do GitHub e evita um arquivo único que cresce sem parar. Reprocessar um mês sobrescreve apenas o arquivo daquele mês.
- `dim_medicamento.csv` é **acumulada**: novos EANs são adicionados e os existentes são atualizados com a versão mais recente.
- Os CSVs usam encoding `utf-8-sig`, compatível com Excel e Power BI sem perda de acentos.
- Os arquivos de `dados/processed/` são gerados pelos scripts. Edições manuais neles serão sobrescritas na próxima execução — altere o script, o `config.yaml` ou o `aliquotas_icms.csv`.

---

## Referências normativas

- Resolução CM-CMED nº 2/2024 — novos fatores de conversão PF/PMC
- Resolução CTE-CMED nº 6/2021 — medicamentos com PMVG
- Resolução CMED nº 02/2004 — medicamentos regulados
- Resolução CMED nº 02/2019 — medicamentos liberados
- CONFAZ — Convênios ICMS para isenção de medicamentos
