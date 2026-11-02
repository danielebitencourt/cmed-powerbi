"""
Validação independente: preços PMC e PF publicados pela CMED/ANVISA
× fato_precos_AAAA-MM.parquet versionado neste repositório.

- Baixa as listas oficiais pelo MESMO link que o ETL consulta
  (reaproveita obter_links_cmed / baixar_arquivo_cmed de etl_cmed.py).
- Lê a planilha por um caminho INDEPENDENTE do ETL (openpyxl, célula a
  célula, valor nativo da célula), para que um erro de leitura do ETL não
  se repita aqui.
- Compara cada célula (GGREM × EAN × Tipo × Alíquota) e grava:
    validacao/resultado/resumo.json
    validacao/resultado/divergencias.csv   (até 20.000 linhas)

Uso:  python validacao/validar_contra_cmed.py --competencia 2026-10
"""
import argparse
import json
import re
import sys
import unicodedata
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import openpyxl
import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))
import etl_cmed  # noqa: E402

SAIDA = RAIZ / "validacao" / "resultado"
RE_PRECO = re.compile(r"^(PF|PMC)\s+(SEM IMPOSTOS|\d+(?:,\d+)?%)$")


def norm(txt) -> str:
    if txt is None:
        return ""
    t = str(txt).replace("\xa0", " ").strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s+%", "%", t).upper()
    return "".join(c for c in unicodedata.normalize("NFKD", t) if not unicodedata.combining(c))


def preco(v):
    """Valor oficial da célula -> (Decimal 2 casas | None, asterisco, tipo_bruto)."""
    if v is None:
        return None, False, "vazio"
    if isinstance(v, (int, float)):
        return Decimal(repr(float(v))).quantize(Decimal("0.01"), ROUND_HALF_UP), False, "numero"
    s = str(v).strip()
    ast = "*" in s
    s = s.replace("*", "").replace("R$", "").strip()
    if s in ("", "-"):
        return None, ast, "texto_vazio"
    if "," in s:                       # padrão BR: 1.234,56
        s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s).quantize(Decimal("0.01"), ROUND_HALF_UP), ast, "texto"
    except Exception:
        return None, ast, "texto_invalido"


def ean(v):
    if v is None:
        return None
    s = re.sub(r"\D", "", str(v).split(".")[0] if isinstance(v, float) else str(v))
    if not s or s == "0":
        return None
    return s.zfill(13)


def ggrem(v):
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") and s[:-2].isdigit() else (s or None)


def ler_oficial(caminho: Path, tipo: str):
    wb = openpyxl.load_workbook(caminho, read_only=True, data_only=True)
    for ws in wb.worksheets:
        linhas = ws.iter_rows(values_only=True)
        for i, row in enumerate(linhas):
            cab = [norm(c) for c in row]
            if "CODIGO GGREM" in cab and sum(bool(RE_PRECO.match(c)) for c in cab) >= 3:
                idx_g = cab.index("CODIGO GGREM")
                idx_e = [cab.index(e) for e in ("EAN 1", "EAN 2", "EAN 3") if e in cab]
                cols = [(j, c) for j, c in enumerate(cab) if RE_PRECO.match(c) and c.startswith(tipo)]
                regs, tipos_brutos = [], {}
                for r in linhas:
                    if r is None or all(x is None for x in r):
                        continue
                    g = ggrem(r[idx_g])
                    if not g:
                        continue
                    eans = [x for x in (ean(r[j]) for j in idx_e) if x] or [None]
                    for j, c in cols:
                        val, ast, tb = preco(r[j])
                        tipos_brutos[tb] = tipos_brutos.get(tb, 0) + 1
                        if val is None:
                            continue
                        aliq = "Sem Impostos" if "SEM IMPOSTOS" in c else c.split()[-1]
                        for e in eans:
                            regs.append((g, e or "", tipo, aliq, val))
                wb.close()
                return regs, {"aba": ws.title, "linha_cabecalho": i + 1,
                              "colunas_preco": [c for _, c in cols], "tipos_celula": tipos_brutos}
    wb.close()
    raise RuntimeError(f"Cabeçalho não encontrado em {caminho.name}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--competencia", required=True)
    args = ap.parse_args()
    comp = args.competencia

    config = etl_cmed.carregar_config(str(RAIZ / "config.yaml"))
    etl_cmed.configurar_log(config)
    config["_sessao"] = etl_cmed.criar_sessao_http(config)
    links = etl_cmed.obter_links_cmed(config)

    resumo = {"competencia_validada": comp, "links_oficiais": links, "arquivos": {}}
    oficial = []
    for tipo in ("PMC", "PF"):
        arq = etl_cmed.baixar_arquivo_cmed(links[tipo], tipo, config)
        regs, meta = ler_oficial(arq, tipo)
        meta.update({"arquivo": arq.name,
                     "competencia_no_nome": etl_cmed.extrair_competencia_do_arquivo(arq),
                     "sha256": etl_cmed.calcular_hash_arquivo(arq), "celulas": len(regs)})
        resumo["arquivos"][tipo] = meta
        oficial += regs

    of = pd.DataFrame(oficial, columns=["GGREM", "EAN", "TIPO", "ALIQ", "OFICIAL"])
    k = ["GGREM", "EAN", "TIPO", "ALIQ"]
    resumo["duplicidades_oficial"] = int(of.duplicated(k).sum())

    fp = RAIZ / "dados" / "processed" / "fato" / f"fato_precos_{comp}.parquet"
    rp = pd.read_parquet(fp)
    rp = pd.DataFrame({"GGREM": rp.CODIGO_GGREM.astype(str), "EAN": rp.EAN.fillna("").astype(str),
                       "TIPO": rp.TIPO_PRECO, "ALIQ": rp.ALIQUOTA_ICMS,
                       "REPO": rp.VALOR.map(lambda x: Decimal(repr(float(x))).quantize(Decimal("0.01"), ROUND_HALF_UP))})
    resumo["duplicidades_repo"] = int(rp.duplicated(k).sum())

    m = of.merge(rp, on=k, how="outer", indicator=True)
    so_of = m[m._merge == "left_only"]
    so_rp = m[m._merge == "right_only"]
    amb = m[m._merge == "both"]
    dif = amb[amb.OFICIAL != amb.REPO]

    def por(df):
        return {f"{t}|{a}": int(n) for (t, a), n in df.groupby(["TIPO", "ALIQ"]).size().items()}

    resumo.update({
        "celulas_oficial": int(len(of)), "celulas_repo": int(len(rp)),
        "celulas_comparadas": int(len(amb)),
        "celulas_iguais": int(len(amb) - len(dif)),
        "celulas_valor_divergente": int(len(dif)),
        "faltando_no_repo": int(len(so_of)),
        "sobrando_no_repo": int(len(so_rp)),
        "assertividade_pct": round(100 * (len(amb) - len(dif)) / max(len(of), 1), 4),
        "divergentes_por_tipo_aliq": por(dif),
        "faltando_por_tipo_aliq": por(so_of),
        "sobrando_por_tipo_aliq": por(so_rp),
    })

    SAIDA.mkdir(parents=True, exist_ok=True)
    out = pd.concat([
        dif.assign(PROBLEMA="valor_divergente"),
        so_of.assign(PROBLEMA="faltando_no_repo"),
        so_rp.assign(PROBLEMA="sobrando_no_repo"),
    ])[k + ["OFICIAL", "REPO", "PROBLEMA"]]
    out.head(20000).to_csv(SAIDA / "divergencias.csv", index=False)
    (SAIDA / "resumo.json").write_text(json.dumps(resumo, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(resumo, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
