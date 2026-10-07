# Automação Lista DCB → Power BI

Atualização automática da **Lista consolidada das Denominações Comuns Brasileiras (DCB)** publicada pela ANVISA na [Biblioteca Digital](https://bibliotecadigital.anvisa.gov.br/jspui/handle/anvisa/11933), no mesmo padrão da automação CMED deste repositório.

---

## Como funciona

A cada nova Instrução Normativa a ANVISA publica a lista em um **item novo**, com outro endereço (ex.: `.../19200/1/4__Lista_DCB_consolidada_out_2025.xlsx` → `.../21318/1/2-Lista DCB consolidada jul 2026.xlsx`). Por isso o link nunca fica fixo no código. O `etl_dcb.py`:

1. Abre a coleção *Farmacopeia: Denominações Comuns Brasileiras* e escolhe o item marcado como **VIGENTE**. Se nenhum estiver marcado, usa o mais recente. Se a página falhar, usa o RSS da coleção.
2. Compara com o `estado.json`. **Se a versão for a mesma, termina sem alterar nada.**
3. Baixa o `.xlsx`, valida que é uma planilha real e que tem pelo menos 5.000 DCBs (trava em `config.yaml`).
4. Padroniza as colunas e gera as tabelas para o Power BI, registrando o que mudou em relação à versão anterior.

Se qualquer etapa falhar, os arquivos atuais **não** são substituídos.

---

## Estrutura

```
dcb/
├── etl_dcb.py                  ← ETL (descoberta, download, tratamento, exportação)
├── config.yaml                 ← URLs, pastas, retry, trava de linhas mínimas, alertas
├── estado.json                 ← Última versão processada (handle, link, SHA-256)
└── dados/
    ├── original/
    │   └── Lista_DCB_consolidada_vigente.xlsx   ← planilha oficial vigente (sobrescrita)
    └── processed/
        ├── dim_dcb.csv          ← lista vigente tratada
        ├── dcb_alteracoes.csv   ← incluídas / excluídas / alteradas por versão (acumulado)
        └── dcb_versoes.csv      ← log das versões processadas
```

O workflow fica em `.github/workflows/dcb.yml`, na raiz do repositório, porque o GitHub só lê workflows de lá. As dependências são as mesmas do `requirements.txt` da raiz.

---

## Execução automática (GitHub Actions)

O workflow **ETL Lista DCB** roda **toda segunda-feira às 11:00 UTC (08:00 de Brasília)**. A ANVISA não tem calendário fixo de publicação; nas semanas sem versão nova, o workflow termina sem fazer commit.

Para rodar manualmente: **Actions → ETL Lista DCB → Run workflow**. Marque *Reprocessar mesmo sem versão nova* para forçar.

Ele usa o mesmo grupo de concorrência do ETL CMED, então os dois nunca fazem push ao mesmo tempo. Os alertas por e-mail usam os **mesmos Secrets** do CMED (`EMAIL_ALERTA_ATIVO`, `SMTP_REMETENTE`, `SMTP_SENHA`, `SMTP_DESTINATARIOS`).

---

## Execução local

```bash
cd dcb
pip install -r ../requirements.txt
python etl_dcb.py                       # baixa a versão vigente (se houver nova)
python etl_dcb.py --forcar              # reprocessa mesmo sem versão nova
python etl_dcb.py --arquivo lista.xlsx --versao "IN nº 462, de 23 de julho de 2026"
```

---

## Uso no Power BI

| Tabela | Arquivo | Como importar |
|---|---|---|
| `dDCB` | `dcb/dados/processed/dim_dcb.csv` | Texto/CSV (UTF-8) |
| `fDCB_Alteracoes` | `dcb/dados/processed/dcb_alteracoes.csv` | Texto/CSV (UTF-8) |
| `dDCB_Versoes` | `dcb/dados/processed/dcb_versoes.csv` | Texto/CSV (UTF-8) |

Direto do GitHub: `https://raw.githubusercontent.com/danielebitencourt/cmed-powerbi/main/dcb/dados/processed/dim_dcb.csv`

Relacionamento sugerido: `fDCB_Alteracoes[NUM_DCB]` → `dDCB[NUM_DCB]` (Muitos:1). DCBs excluídas não existem mais em `dDCB`; para elas use as colunas `VALOR_ANTERIOR` da própria tabela de alterações.

### Colunas de `dim_dcb.csv`

| Coluna | Descrição |
|---|---|
| `NUM_DCB` | Nº DCB (chave) |
| `DENOMINACAO` | Denominação Comum Brasileira |
| `NUM_CAS` | Nº CAS |
| `CLASSIFICACAO` | Sigla (BIO, EXA, HOM, IFA, PM, RAD, INF, OUTRO) |
| `CLASSIFICACAO_DESC` | Descrição da sigla, conforme a legenda da ANVISA |
| `HISTORICO` | Ato que incluiu/alterou a DCB (texto da ANVISA) |
| `VERSAO` | Versão da lista (ex.: `IN nº 462, de 23 de julho de 2026`) |
| `DATA_CARGA` | Data e hora do processamento |

### Colunas de `dcb_alteracoes.csv`

`VERSAO_ANTERIOR`, `VERSAO_NOVA`, `NUM_DCB`, `TIPO_ALTERACAO` (`INCLUIDA` / `EXCLUIDA` / `ALTERADA`), `CAMPO` (para alteradas: `DENOMINACAO`, `NUM_CAS` ou `CLASSIFICACAO`), `VALOR_ANTERIOR`, `VALOR_NOVO`, `DATA_CARGA`.

---

## Observações

- A classificação `OUTRO` apareceu na IN nº 462/2026 (óxido nítrico, monóxido de carbono) e não consta da legenda oficial. Se surgir outra sigla nova, o ETL registra um aviso no log e deixa `CLASSIFICACAO_DESC` vazio. Basta adicioná-la ao dicionário `CLASSIFICACOES` no `etl_dcb.py`.
- A classificação da Lista DCB não equivale a aprovação de finalidade de uso pela ANVISA (nota oficial da própria lista).
- Os arquivos de `dados/` são gerados pelo script. Edições manuais neles serão sobrescritas.
