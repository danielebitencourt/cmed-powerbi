# Automação CMED → Power BI

Solução de importação automática mensal dos dados oficiais de preços de medicamentos da CMED/ANVISA para o Power BI, com relacionamento EAN × Estado.

---

## Visão geral

O sistema opera em duas camadas complementares:

**Camada ETL (Python)** — baixa os arquivos PMC e PF/PMVG diretamente do site da ANVISA, trata qualidade de dados (EAN, separadores decimais, asteriscos) e faz o unpivot para gerar uma tabela fato com granularidade EAN × UF × Tipo de Preço × Competência.

**Camada analítica (Power BI)** — consome os CSVs tratados via Power Query, mantém modelo estrela com dimensões Medicamento, Estado e Calendário, e disponibiliza medidas DAX pré-configuradas para análise de preços.

---

## Estrutura de pastas

```
CMED_PowerBI/
├── etl_cmed.py              ← Script principal ETL
├── config.yaml              ← Configurações (URLs, caminhos, alertas)
├── requirements.txt         ← Dependências Python
├── README.md                ← Este arquivo
│
├── dados/
│   ├── raw/                 ← Arquivos originais baixados da ANVISA
│   │   └── 2026-07/
│   ├── processed/           ← CSVs tratados para o Power BI
│   │   ├── fato_precos.csv
│   │   ├── dim_medicamento.csv
│   │   ├── dim_estado.csv
│   │   └── dim_calendario.csv
│   └── historico/           ← Backup mensal
│
├── logs/
│   └── cmed_etl_YYYYMM.log
│
├── powerbi/
│   ├── PowerQuery_CMED.pq   ← Código M para o Power BI
│   └── medidas_dax.dax       ← Medidas DAX prontas
│
└── scripts/
    └── agendar_windows.bat   ← Agendamento no Windows
```

---

## Instalação

### 1. Pré-requisitos

- Python 3.10 ou superior
- Power BI Desktop (versão atual)
- Acesso à internet (site gov.br/anvisa)

### 2. Instalar dependências

```bash
cd C:\CMED_PowerBI
pip install -r requirements.txt
```

### 3. Configurar

Edite `config.yaml` com os caminhos do seu ambiente. Os campos mínimos a ajustar são os diretórios de saída e, opcionalmente, os dados de e-mail para alertas.

### 4. Primeira execução

```bash
python etl_cmed.py
```

O script vai acessar o site da ANVISA, baixar os dois arquivos (PMC e PF), processar e exportar os CSVs para `dados/processed/`.

---

## Uso no Power BI

### Importar as queries

1. Abra o Power BI Desktop
2. Vá em **Página Inicial → Transformar Dados → Editor do Power Query**
3. Crie uma nova Query em branco para cada seção do arquivo `powerbi/PowerQuery_CMED.pq`:
   - `pCaminhoDados` — parâmetro com o caminho dos CSVs
   - `fCMED_Precos` — tabela fato
   - `dMedicamento` — dimensão medicamento
   - `dEstado` — dimensão estado
   - `dCalendario` — dimensão calendário
4. Ajuste o parâmetro `pCaminhoDados` para apontar para sua pasta `dados/processed/`
5. Clique em **Fechar e Aplicar**

### Configurar relacionamentos

No Model View do Power BI, confirme os relacionamentos (normalmente criados automaticamente):

| De | Para | Cardinalidade |
|---|---|---|
| fCMED_Precos[EAN] | dMedicamento[EAN] | Muitos:1 |
| fCMED_Precos[ESTADO_UF] | dEstado[ESTADO_UF] | Muitos:1 |
| fCMED_Precos[DATA_REFERENCIA] | dCalendario[DATA] | Muitos:1 |

Todos com filtro cruzado **unidirecional** (da dimensão para a fato).

### Adicionar medidas DAX

As medidas estão documentadas em `powerbi/medidas_dax.dax`. Crie cada uma na tabela `fCMED_Precos` via **Nova Medida** no Power BI.

---

## Agendamento mensal

### Windows Task Scheduler (recomendado)

Execute `scripts/agendar_windows.bat` como Administrador. Edite os caminhos dentro do .bat antes de executar. A tarefa fica configurada para rodar no dia 2 de cada mês às 08:00.

### Power BI Service + Gateway

1. Instale o **Gateway de Dados Local** no servidor onde os CSVs ficam
2. Publique o relatório no Power BI Service
3. Em **Configurações do Dataset → Atualização Agendada**, configure atualização mensal no dia 3 (um dia após o ETL)
4. Aponte a fonte de dados para o caminho dos CSVs via Gateway

### Fluxo combinado (recomendado para empresas)

Python ETL roda no dia 2 → gera CSVs → Gateway monitora a pasta → Power BI Service atualiza o dataset no dia 3.

---

## Tratamentos de qualidade aplicados

| Problema | Tratamento |
|---|---|
| EAN com zeros faltando | `zfill(13)` — preenchimento com zeros à esquerda |
| EAN inválido (dígito verificador) | Sinalizado com `EAN_INVALIDO = True` |
| Valor com ponto como decimal | Detecta e corrige separadores BR/US |
| Asterisco (*) no valor | Remove e sinaliza `FLAG_ASTERISCO = True` |
| Cabeçalho em posição variável | Detecta linha com "SUBSTÂNCIA" automaticamente |
| Colunas renomeadas pela ANVISA | Mapeamento de nomes alternativos |
| Site fora do ar | Retry 3× com intervalo de 30s + e-mail de alerta |

---

## Modelo de dados

```
            ┌──────────────┐
            │ dCalendario  │
            │   DATA (PK)  │
            └──────┬───────┘
                   │ 1
                   │
                   │ *
┌──────────────┐   ┌──────────────────┐   ┌──────────────┐
│ dMedicamento │   │  fCMED_Precos    │   │   dEstado    │
│   EAN (PK)   │──<│  EAN             │>──│ ESTADO_UF    │
│              │ 1 │  ESTADO_UF       │ * │   (PK)       │
│              │   │  DATA_REFERENCIA │   │              │
└──────────────┘   │  TIPO_PRECO      │   └──────────────┘
                   │  ALIQUOTA_ICMS   │
                   │  VALOR           │
                   │  COMPETENCIA     │
                   └──────────────────┘
```

---

## Mapeamento ICMS → UF

Conforme Resolução CM-CMED nº 2/2024:

| Alíquota | Estados |
|---|---|
| 0% (isento CONFAZ) | AC, AM, AP, PA, RO, RR, TO, MT, MS, GO, DF |
| 12% | ES, RS |
| 17% | AL, BA, CE, MA, PB, PE, PI, RN, SE, PR, SC, SP |
| 17,5% | RJ |
| 18% | MG |
| 19,5% a 22% | Verificar legislação vigente antes de cada carga |

Atualize o mapeamento em `etl_cmed.py` (constante `MAPEAMENTO_ICMS_UF`) sempre que houver mudança legislativa.

---

## Notas de segurança

- Nenhuma credencial é armazenada em texto plano nos CSVs — dados são exclusivamente preços públicos da ANVISA.
- O `config.yaml` pode conter senha de e-mail — proteja com permissões de arquivo restritas ou use variável de ambiente.
- Os arquivos gerados seguem encoding `utf-8-sig` para compatibilidade com Excel e Power BI sem perda de caracteres acentuados.

---

## Referências normativas

- Resolução CM-CMED nº 2/2024 — novos fatores de conversão PF/PMC
- Resolução CTE-CMED nº 6/2021 — medicamentos com PMVG
- Resolução CMED nº 02/2004 — medicamentos regulados
- Resolução CMED nº 02/2019 — medicamentos liberados
- CONFAZ — Convênios ICMS para isenção de medicamentos
