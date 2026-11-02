"""
Valida os preços exibidos no Power BI contra a fato versionada
(fato_precos_AAAA-MM.parquet), que por sua vez é validada célula a célula
contra as listas oficiais CMED por validacao/validar_contra_cmed.py.

Entrada: CSV exportado do Power BI com as colunas
    EAN, TIPO_PRECO, ALIQUOTA_ICMS, VALOR
(ex.: "Exportar dados" do visual de matriz em formato tabular, ou a consulta
DAX abaixo no DAX Query View / DAX Studio):

    EVALUATE
    SUMMARIZECOLUMNS (
        'dm. PRODUTOS'[EAN], fato_precos[TIPO_PRECO], fato_precos[ALIQUOTA_ICMS],
        "VALOR", [Preço CMED Vigente]
    )

Uso:
    python validacao/validar_powerbi.py --csv export_pbi.csv --competencia 2026-10

Saída: validacao/resultado/powerbi_divergencias.csv e código de saída 1 se
houver qualquer célula divergente (tolerância de R$ 0,005).
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
FATO = RAIZ / "dados" / "processed" / "fato"
SAIDA = RAIZ / "validacao" / "resultado"
CHAVE = ["EAN", "TIPO_PRECO", "ALIQUOTA_ICMS"]
TOL = 0.005


def _num(s: pd.Series) -> pd.Series:
    """Aceita 1234.56, 1.234,56 e R$ 1.234,56."""
    t = s.astype(str).str.replace("R$", "", regex=False).str.strip()
    br = t.str.contains(",", regex=False)
    t = t.where(~br, t.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    return pd.to_numeric(t, errors="coerce")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--competencia", required=True, help="AAAA-MM exibida no Power BI")
    a = ap.parse_args()

    pbi = pd.read_csv(a.csv, dtype=str, sep=None, engine="python")
    pbi.columns = [c.split("[")[-1].strip("[]").upper() for c in pbi.columns]
    faltam = [c for c in CHAVE + ["VALOR"] if c not in pbi.columns]
    if faltam:
        print(f"CSV sem as colunas {faltam}. Colunas lidas: {list(pbi.columns)}")
        return 2
    pbi["VALOR"] = _num(pbi["VALOR"])
    pbi = pbi.dropna(subset=["VALOR"])

    ofi = pd.read_parquet(FATO / f"fato_precos_{a.competencia}.parquet")
    ofi = ofi[ofi["EAN"].isin(pbi["EAN"])][CHAVE + ["VALOR"]]
    # Um EAN com mais de um GGREM e preços diferentes não tem valor único.
    amb = ofi.groupby(CHAVE)["VALOR"].nunique()
    ofi = ofi.drop_duplicates(CHAVE)

    x = pbi.merge(ofi, on=CHAVE, how="outer", suffixes=("_PBI", "_CMED"), indicator=True)
    x["DIF"] = (x["VALOR_PBI"] - x["VALOR_CMED"]).round(2)
    x["PROBLEMA"] = ""
    x.loc[x["_merge"] == "left_only", "PROBLEMA"] = "no Power BI, fora da CMED"
    x.loc[x["_merge"] == "right_only", "PROBLEMA"] = "na CMED, faltando no Power BI"
    x.loc[(x["_merge"] == "both") & (x["DIF"].abs() >= TOL), "PROBLEMA"] = "valor divergente"
    div = x[x["PROBLEMA"] != ""].drop(columns="_merge")

    SAIDA.mkdir(parents=True, exist_ok=True)
    div.to_csv(SAIDA / "powerbi_divergencias.csv", index=False)
    comp = int((x["_merge"] == "both").sum())
    ok = comp - int((div["PROBLEMA"] == "valor divergente").sum())
    print(f"Competência {a.competencia}: {comp} células comparadas, {ok} corretas, "
          f"{len(div)} problemas ({100 * ok / max(comp, 1):.2f}% de acerto).")
    if (amb > 1).any():
        print(f"Atenção: {int((amb > 1).sum())} chaves com preços diferentes por GGREM.")
    if len(div):
        print(div["PROBLEMA"].value_counts().to_string())
    return 1 if len(div) else 0


if __name__ == "__main__":
    sys.exit(main())
