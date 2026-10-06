#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gerador de HISTÓRICO de alíquotas de ICMS para o Power BI.

Lê o arquivo-log 'aliquotas_icms.csv' (fonte da verdade, versionado no GitHub)
e produz dois artefatos prontos para o Power BI:

1) dim_aliquota_historico.csv
   Dimensão temporal (SCD Tipo 2). Uma linha por período de vigência de cada UF,
   com data de início, data de fim e flag de vigência atual. Serve para a
   "linha do tempo" de mudanças e para auditoria (qual lei, qual fonte).

2) fato_aliquota_mensal.csv
   Grade mês × UF. Uma linha por competência (mês) e UF, com a alíquota que
   estava vigente naquele mês. Serve para GRÁFICOS de evolução no Power BI,
   relacionando com dCalendario (por COMPETENCIA_DATA) e dEstado (por ESTADO_UF).
   A coluna MUDOU marca o mês exato em que a alíquota daquela UF mudou.

COMO REGISTRAR UMA MUDANÇA DE ALÍQUOTA (o motivo de tudo isto existir):
   NÃO edite a linha antiga. ABRA UMA NOVA LINHA em aliquotas_icms.csv com a
   nova alíquota e sua VIGENCIA_INICIO. Rode este script. Ele fecha sozinho a
   vigência anterior (VIGENCIA_FIM = dia anterior ao início da nova) e recalcula
   a grade mensal. Histórico preservado, com data e fonte, direto no Power BI.

Uso:
    python gerar_historico_aliquotas.py                 # grade até o mês atual
    python gerar_historico_aliquotas.py --ate 2027-06   # projeta a grade até jun/2027
    python gerar_historico_aliquotas.py --entrada caminho/aliquotas_icms.csv --saida dados/processed
"""
from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

import pandas as pd

ABERTO = date(9999, 12, 31)  # sentinela de "vigência em aberto" (sem data de fim)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")


def pct_para_numero(texto: str):
    """'20,5%' -> 0.205 ; '17%' -> 0.17 ; vazio/invalido -> None."""
    if texto is None:
        return None
    t = str(texto).strip()
    if not t or t.lower().startswith("sem"):
        return None
    t = t.replace("%", "").replace(",", ".").strip()
    try:
        return round(float(t) / 100, 6)
    except ValueError:
        return None


def primeiro_dia(d: date) -> date:
    return d.replace(day=1)


def soma_um_mes(d: date) -> date:
    return date(d.year + (d.month // 12), (d.month % 12) + 1, 1)


def carregar_log(caminho: Path) -> pd.DataFrame:
    """Lê o CSV-log e normaliza colunas/tipos."""
    df = pd.read_csv(caminho, dtype=str, encoding="utf-8-sig").fillna("")
    df.columns = [c.strip().lstrip("\ufeff").upper() for c in df.columns]

    obrigatorias = {"UF", "ALIQUOTA_ICMS", "VIGENCIA_INICIO"}
    faltando = obrigatorias - set(df.columns)
    if faltando:
        raise ValueError(f"{caminho.name} não tem as colunas {sorted(faltando)}")

    for opcional in ("FONTE", "OBS"):
        if opcional not in df.columns:
            df[opcional] = ""

    df["UF"] = df["UF"].str.strip().str.upper()
    df["ALIQUOTA_ICMS"] = df["ALIQUOTA_ICMS"].str.strip()
    df["VIGENCIA_INICIO"] = pd.to_datetime(
        df["VIGENCIA_INICIO"].str.strip(), format="%Y-%m-%d", errors="coerce"
    ).dt.date

    ruins = df[df["VIGENCIA_INICIO"].isna() | (df["UF"] == "") | (df["ALIQUOTA_ICMS"] == "")]
    if len(ruins):
        logging.warning(f"{len(ruins)} linha(s) ignorada(s) por UF/alíquota/data inválida.")
        df = df.drop(ruins.index)

    df["ALIQUOTA_ICMS_PCT"] = df["ALIQUOTA_ICMS"].map(pct_para_numero)
    return df.reset_index(drop=True)


def montar_historico(df: pd.DataFrame) -> pd.DataFrame:
    """SCD Tipo 2: calcula VIGENCIA_FIM e flag VIGENTE por UF."""
    linhas = []
    hoje = date.today()

    for uf, g in df.groupby("UF", sort=True):
        g = g.sort_values("VIGENCIA_INICIO").reset_index(drop=True)
        for i, r in g.iterrows():
            inicio = r["VIGENCIA_INICIO"]
            if i + 1 < len(g):
                prox_inicio = g.loc[i + 1, "VIGENCIA_INICIO"]
                fim = date.fromordinal(prox_inicio.toordinal() - 1)
                vigente = 0
            else:
                fim = ABERTO
                vigente = 1 if inicio <= hoje else 0
            linhas.append(
                {
                    "ALIQUOTA_HIST_ID": f"{uf}_{inicio.isoformat()}",
                    "ESTADO_UF": uf,
                    "ALIQUOTA_ICMS": r["ALIQUOTA_ICMS"],
                    "ALIQUOTA_ICMS_PCT": r["ALIQUOTA_ICMS_PCT"],
                    "VIGENCIA_INICIO": inicio.isoformat(),
                    "VIGENCIA_FIM": "" if fim == ABERTO else fim.isoformat(),
                    "EM_ABERTO": 1 if fim == ABERTO else 0,
                    "VIGENTE": vigente,
                    "ORDEM_VIGENCIA": i + 1,
                    "FONTE": r["FONTE"],
                    "OBS": r["OBS"],
                }
            )

    hist = pd.DataFrame(linhas)
    return hist.sort_values(["ESTADO_UF", "VIGENCIA_INICIO"]).reset_index(drop=True)


def montar_grade_mensal(df: pd.DataFrame, ate: date) -> pd.DataFrame:
    """Grade mês × UF com a alíquota vigente em cada mês, até 'ate' (inclusive)."""
    inicio_grade = primeiro_dia(min(df["VIGENCIA_INICIO"]))
    fim_grade = primeiro_dia(ate)

    meses = []
    m = inicio_grade
    while m <= fim_grade:
        meses.append(m)
        m = soma_um_mes(m)

    linhas = []
    for uf, g in df.groupby("UF", sort=True):
        g = g.sort_values("VIGENCIA_INICIO").reset_index(drop=True)
        aliq_mes_anterior = None
        for mes in meses:
            # última vigência iniciada até o 1º dia deste mês
            validas = g[g["VIGENCIA_INICIO"] <= mes]
            if validas.empty:
                continue  # UF ainda não tinha alíquota registrada neste mês
            r = validas.iloc[-1]
            aliq = r["ALIQUOTA_ICMS"]
            mudou = 1 if (aliq_mes_anterior is not None and aliq != aliq_mes_anterior) else 0
            linhas.append(
                {
                    "COMPETENCIA": mes.strftime("%Y-%m"),
                    "COMPETENCIA_DATA": mes.isoformat(),
                    "ESTADO_UF": uf,
                    "ALIQUOTA_ICMS": aliq,
                    "ALIQUOTA_ICMS_PCT": r["ALIQUOTA_ICMS_PCT"],
                    "VIGENCIA_INICIO": r["VIGENCIA_INICIO"].isoformat(),
                    "MUDOU": mudou,
                }
            )
            aliq_mes_anterior = aliq

    grade = pd.DataFrame(linhas)
    return grade.sort_values(["ESTADO_UF", "COMPETENCIA_DATA"]).reset_index(drop=True)


def descobrir_horizonte(saida_dir: Path) -> date:
    """Horizonte = o mais recente entre hoje e a última competência já existente na fato."""
    hoje = date.today()
    maior = hoje
    fato_dir = saida_dir / "fato"
    if fato_dir.exists():
        import re

        for f in fato_dir.glob("fato_precos_*.parquet"):
            mobj = re.search(r"(\d{4})-(\d{2})", f.name)
            if mobj:
                cand = date(int(mobj.group(1)), int(mobj.group(2)), 1)
                maior = max(maior, cand)
    return maior


def main():
    ap = argparse.ArgumentParser(description="Gera o histórico de alíquotas de ICMS para o Power BI.")
    ap.add_argument("--entrada", default="aliquotas_icms.csv", help="CSV-log de alíquotas (fonte da verdade).")
    ap.add_argument("--saida", default="dados/processed", help="Pasta de saída dos CSVs.")
    ap.add_argument("--ate", default=None, help="Projetar a grade mensal até AAAA-MM (padrão: mês atual / última competência).")
    args = ap.parse_args()

    base = Path(__file__).resolve().parent
    entrada = (base / args.entrada) if not Path(args.entrada).is_absolute() else Path(args.entrada)
    saida = (base / args.saida) if not Path(args.saida).is_absolute() else Path(args.saida)
    saida.mkdir(parents=True, exist_ok=True)

    logging.info(f"Lendo log de alíquotas: {entrada}")
    df = carregar_log(entrada)
    logging.info(f"{len(df)} linha(s) de vigência lida(s) para {df['UF'].nunique()} UF(s).")

    if args.ate:
        y, mth = args.ate.split("-")
        ate = date(int(y), int(mth), 1)
    else:
        ate = descobrir_horizonte(saida)
    logging.info(f"Grade mensal até: {ate.strftime('%Y-%m')}")

    hist = montar_historico(df)
    grade = montar_grade_mensal(df, ate)

    f_hist = saida / "dim_aliquota_historico.csv"
    f_grade = saida / "fato_aliquota_mensal.csv"
    hist.to_csv(f_hist, index=False, encoding="utf-8-sig")
    grade.to_csv(f_grade, index=False, encoding="utf-8-sig")

    n_mudancas = int(grade["MUDOU"].sum())
    logging.info(f"OK  →  {f_hist.name}: {len(hist)} períodos de vigência")
    logging.info(f"OK  →  {f_grade.name}: {len(grade)} linhas (UF × mês), {n_mudancas} mudança(s) registrada(s)")


if __name__ == "__main__":
    main()
