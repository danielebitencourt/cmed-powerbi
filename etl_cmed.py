#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL CMED — Extração, Transformação e Carga de dados ANVISA/CMED para Power BI.

Arquitetura preparada para GitHub Actions:
- Descobre automaticamente os arquivos PMC e PF/PMVG publicados pela CMED.
- Baixa os arquivos e calcula SHA-256.
- Mantém o estado de processamento em um arquivo separado do dado de negócio.
- Usa competência + hashes dos arquivos para detectar mudanças.
- Não depende de fato_precos.csv para decidir se deve processar.
- Mantém histórico de dados em CSV.
- Gera dimensões para consumo pelo Power BI.
"""

import argparse
import hashlib
import json
import logging
import re
import shutil
import smtplib
import sys
import time
from datetime import datetime
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup


CONFIG_PADRAO = {
    "url_cmed": "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos",
    "diretorio_saida": "dados/processed",
    "diretorio_raw": "dados/raw",
    "diretorio_historico": "dados/historico",
    "diretorio_log": "logs",
    "arquivo_estado": ".github/cmed/estado.json",
    "timeout_segundos": 90,
    "tentativas_max": 3,
    "intervalo_retry_segundos": 30,
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "email_alerta": {
        "ativo": False,
        "smtp_host": "smtp.gmail.com",
        "smtp_porta": 587,
        "remetente": "",
        "senha": "",
        "destinatarios": [],
    },
}


MAPEAMENTO_ICMS_UF = {
    "0%": ["AC", "AM", "AP", "PA", "RO", "RR", "TO", "MT", "MS", "GO", "DF"],
    "12%": ["ES", "RS"],
    "17%": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "PR", "SC", "SP"],
    "17,5%": ["RJ"],
    "18%": ["MG"],
    "19,5%": [],
    "20%": [],
    "20,5%": [],
    "21%": [],
    "22%": [],
}


COLUNAS_IDENTIFICACAO = [
    "SUBSTÂNCIA", "CNPJ", "LABORATÓRIO", "CÓDIGO GGREM", "REGISTRO",
    "EAN 1", "EAN 2", "EAN 3", "PRODUTO", "APRESENTAÇÃO",
    "F.FARMACÊUTICA", "CLASSE TERAPÊUTICA",
    "TIPO DE PRODUTO (STATUS DO PRODUTO)",
    "REGIME DE PREÇO", "TARJA", "RESTRIÇÃO HOSPITALAR",
    "CAP", "CONFAZ 87", "ICMS 0%", "ANÁLISE RECURSAL",
]


NOMES_ALTERNATIVOS = {
    "TIPO DE PRODUTO (REVOGADO/NOVO/IDÊNTICO)": "TIPO DE PRODUTO (STATUS DO PRODUTO)",
    "TIPO DE PRODUTO": "TIPO DE PRODUTO (STATUS DO PRODUTO)",
    "ANALISE RECURSAL": "ANÁLISE RECURSAL",
    "RESTRICAO HOSPITALAR": "RESTRIÇÃO HOSPITALAR",
    "RESTRIÇÃO HOSP.": "RESTRIÇÃO HOSPITALAR",
    "CLASSE TERAPEUTICA": "CLASSE TERAPÊUTICA",
    "FORMA FARMACEUTICA": "F.FARMACÊUTICA",
    "FORMA FARMACÊUTICA": "F.FARMACÊUTICA",
    "F. FARMACÊUTICA": "F.FARMACÊUTICA",
    "APRESENTACAO": "APRESENTAÇÃO",
    "CODIGO GGREM": "CÓDIGO GGREM",
    "SUBSTANCIA": "SUBSTÂNCIA",
    "LABORATORIO": "LABORATÓRIO",
}


ESTADOS_BRASIL = {
    "AC": ("Acre", "Norte"), "AL": ("Alagoas", "Nordeste"),
    "AM": ("Amazonas", "Norte"), "AP": ("Amapá", "Norte"),
    "BA": ("Bahia", "Nordeste"), "CE": ("Ceará", "Nordeste"),
    "DF": ("Distrito Federal", "Centro-Oeste"), "ES": ("Espírito Santo", "Sudeste"),
    "GO": ("Goiás", "Centro-Oeste"), "MA": ("Maranhão", "Nordeste"),
    "MG": ("Minas Gerais", "Sudeste"), "MS": ("Mato Grosso do Sul", "Centro-Oeste"),
    "MT": ("Mato Grosso", "Centro-Oeste"), "PA": ("Pará", "Norte"),
    "PB": ("Paraíba", "Nordeste"), "PE": ("Pernambuco", "Nordeste"),
    "PI": ("Piauí", "Nordeste"), "PR": ("Paraná", "Sul"),
    "RJ": ("Rio de Janeiro", "Sudeste"), "RN": ("Rio Grande do Norte", "Nordeste"),
    "RO": ("Rondônia", "Norte"), "RR": ("Roraima", "Norte"),
    "RS": ("Rio Grande do Sul", "Sul"), "SC": ("Santa Catarina", "Sul"),
    "SE": ("Sergipe", "Nordeste"), "SP": ("São Paulo", "Sudeste"),
    "TO": ("Tocantins", "Norte"),
}


def carregar_config(caminho_config: Optional[str] = None) -> dict:
    config = CONFIG_PADRAO.copy()
    if caminho_config and Path(caminho_config).exists():
        with open(caminho_config, "r", encoding="utf-8") as f:
            custom = yaml.safe_load(f) or {}
        config.update(custom)
        logging.info("Configuração carregada de: %s", caminho_config)
    return config


def configurar_log(config: dict) -> None:
    dir_log = Path(config["diretorio_log"])
    dir_log.mkdir(parents=True, exist_ok=True)

    caminho_log = dir_log / f"cmed_etl_{datetime.now():%Y%m}.log"
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = RotatingFileHandler(
        caminho_log, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(fh)
    root.addHandler(ch)


def enviar_alerta_email(config: dict, assunto: str, corpo: str) -> None:
    cfg = config.get("email_alerta", {})
    if not cfg.get("ativo"):
        return

    try:
        msg = MIMEText(corpo, "plain", "utf-8")
        msg["Subject"] = f"[CMED ETL] {assunto}"
        msg["From"] = cfg["remetente"]
        msg["To"] = ", ".join(cfg["destinatarios"])

        with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_porta"]) as server:
            server.starttls()
            server.login(cfg["remetente"], cfg["senha"])
            server.send_message(msg)

        logging.info("Alerta enviado por e-mail.")
    except Exception as exc:
        logging.error("Falha ao enviar e-mail de alerta: %s", exc)


def calcular_hash_arquivo(caminho: Path) -> str:
    sha = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(8192), b""):
            sha.update(bloco)
    return sha.hexdigest()


# ---------------------------------------------------------------------------
# ESTADO DO GITHUB
# ---------------------------------------------------------------------------

def carregar_estado(caminho: str) -> dict:
    """
    Lê o estado persistido fora das tabelas de negócio.

    O arquivo pode ser versionado no próprio repositório pelo GitHub Actions.
    """
    path = Path(caminho)

    if not path.exists():
        logging.info("Estado CMED ainda não existe: %s", path)
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            estado = json.load(f)
        logging.info(
            "Estado carregado: competência=%s",
            estado.get("competencia"),
        )
        return estado
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Estado inválido ou ilegível: %s", exc)
        return {}


def salvar_estado(caminho: str, estado: dict) -> None:
    path = Path(caminho)
    path.parent.mkdir(parents=True, exist_ok=True)

    temporario = path.with_suffix(path.suffix + ".tmp")
    with open(temporario, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)
        f.write("\n")

    temporario.replace(path)
    logging.info("Estado atualizado: %s", path)


def estado_ja_processado(estado: dict, competencia: str, hashes: dict) -> bool:
    """
    Retorna True somente quando competência E hashes são iguais.

    Assim, uma atualização do arquivo da mesma competência volta a ser
    processada.
    """
    if not estado:
        return False

    mesma_competencia = estado.get("competencia") == competencia
    hashes_atuais = {
        "PMC": hashes.get("PMC"),
        "PF": hashes.get("PF"),
    }
    hashes_salvos = {
        "PMC": estado.get("hashes", {}).get("PMC"),
        "PF": estado.get("hashes", {}).get("PF"),
    }

    if mesma_competencia and hashes_atuais == hashes_salvos:
        logging.info(
            "Competência %s já processada com os mesmos arquivos. "
            "Nenhum processamento necessário.",
            competencia,
        )
        return True

    if mesma_competencia:
        logging.info(
            "Competência %s já existe, mas o hash dos arquivos mudou. "
            "Novo processamento será realizado.",
            competencia,
        )

    return False


# ---------------------------------------------------------------------------
# EXTRAÇÃO
# ---------------------------------------------------------------------------

def obter_links_cmed(config: dict) -> dict:
    url = config["url_cmed"]
    headers = {"User-Agent": config["user_agent"]}

    logging.info("Acessando página CMED: %s", url)
    resp = requests.get(url, headers=headers, timeout=config["timeout_segundos"])
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    links = {"PMC": None, "PF": None}

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        texto = a.get_text(strip=True).lower()

        if (
            ("xls_conformidade_site" in href)
            or ("pmc" in texto and ".xls" in href)
        ) and href.endswith((".xlsx", ".xls")):
            links["PMC"] = href if href.startswith("http") else f"https://www.gov.br{href}"

        if (
            ("xls_conformidade_gov" in href)
            or ("pmvg" in texto and ".xls" in href)
        ) and href.endswith((".xlsx", ".xls")):
            links["PF"] = href if href.startswith("http") else f"https://www.gov.br{href}"

    if not links["PMC"] or not links["PF"]:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()

            if re.search(r"xls_conformidade_site_\d{8}", href):
                links["PMC"] = href if href.startswith("http") else f"https://www.gov.br{href}"

            if re.search(r"xls_conformidade_gov_\d{8}", href):
                links["PF"] = href if href.startswith("http") else f"https://www.gov.br{href}"

    for tipo, link in links.items():
        if link:
            logging.info("Link %s encontrado: %s", tipo, link)
        else:
            logging.warning("Link %s NÃO encontrado na página.", tipo)

    return links


def baixar_arquivo_cmed(
    url: str,
    tipo: str,
    config: dict,
    competencia: Optional[str] = None,
) -> Path:
    tentativas = config["tentativas_max"]
    intervalo = config["intervalo_retry_segundos"]
    headers = {"User-Agent": config["user_agent"]}

    if not competencia:
        match = re.search(r"(\d{8})", url)
        if match:
            data_str = match.group(1)
            competencia = f"{data_str[:4]}-{data_str[4:6]}"

    competencia = competencia or datetime.now().strftime("%Y-%m")

    dir_raw = Path(config["diretorio_raw"]) / competencia
    dir_raw.mkdir(parents=True, exist_ok=True)

    nome_arquivo = url.split("/")[-1].split("?")[0]
    if not nome_arquivo.endswith((".xlsx", ".xls")):
        nome_arquivo = f"cmed_{tipo.lower()}_{competencia}.xlsx"

    caminho_local = dir_raw / nome_arquivo

    for tentativa in range(1, tentativas + 1):
        try:
            logging.info(
                "Download %s — tentativa %s/%s: %s",
                tipo, tentativa, tentativas, url,
            )

            resp = requests.get(
                url,
                headers=headers,
                timeout=config["timeout_segundos"],
                stream=True,
            )
            resp.raise_for_status()

            with open(caminho_local, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            tamanho = caminho_local.stat().st_size
            if tamanho < 1024:
                raise ValueError(
                    f"Arquivo muito pequeno ({tamanho} bytes) — possível erro de download."
                )

            sha = calcular_hash_arquivo(caminho_local)
            logging.info(
                "Download %s concluído: %s (%s bytes, SHA-256: %s...)",
                tipo, caminho_local, f"{tamanho:,}", sha[:16],
            )
            return caminho_local

        except (requests.RequestException, ValueError) as exc:
            logging.error(
                "Erro no download %s (tentativa %s): %s",
                tipo, tentativa, exc,
            )
            if tentativa < tentativas:
                time.sleep(intervalo)
            else:
                msg = (
                    f"Falha permanente no download {tipo} após "
                    f"{tentativas} tentativas: {exc}"
                )
                logging.critical(msg)
                enviar_alerta_email(config, f"Falha download {tipo}", msg)
                raise RuntimeError(msg) from exc

    raise RuntimeError("Fluxo inesperado na função de download.")


# ---------------------------------------------------------------------------
# TRANSFORMAÇÃO
# ---------------------------------------------------------------------------

def validar_ean13(ean: str) -> bool:
    if not ean or not isinstance(ean, str):
        return False

    ean = re.sub(r"\D", "", ean)
    if len(ean) != 13:
        return False

    try:
        soma = sum(
            int(digito) * (1 if i % 2 == 0 else 3)
            for i, digito in enumerate(ean[:12])
        )
        verificador = (10 - (soma % 10)) % 10
        return verificador == int(ean[12])
    except (ValueError, IndexError):
        return False


def tratar_ean(valor) -> Optional[str]:
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return None

    texto = str(valor).strip()
    if not texto or texto.lower() in ("nan", "none", ""):
        return None

    numeros = re.sub(r"\D", "", texto)
    if not numeros or numeros == "0":
        return None

    return numeros.zfill(13)


def tratar_valor_preco(valor) -> tuple:
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return None, False

    texto = str(valor).strip()
    if not texto or texto.lower() in ("nan", "none", "-", ""):
        return None, False

    flag_asterisco = "*" in texto
    texto = texto.replace("*", "").strip()

    if not texto:
        return None, flag_asterisco

    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")

    try:
        return float(texto), flag_asterisco
    except ValueError:
        logging.warning("Valor de preço não numérico ignorado: '%s'", valor)
        return None, flag_asterisco


def _normalizar_texto_cabecalho(valor) -> str:
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ""

    texto = str(valor).replace("\xa0", " ").strip()
    texto = re.sub(r"\s+", " ", texto)
    return re.sub(r"\s+%", "%", texto).upper()


def detectar_linha_cabecalho(df: pd.DataFrame) -> int:
    identificadores = {
        "SUBSTÂNCIA", "CNPJ", "LABORATÓRIO", "CÓDIGO GGREM", "REGISTRO",
        "EAN 1", "EAN 2", "EAN 3", "PRODUTO", "APRESENTAÇÃO",
        "CLASSE TERAPÊUTICA", "TIPO DE PRODUTO (STATUS DO PRODUTO)",
        "REGIME DE PREÇO", "RESTRIÇÃO HOSPITALAR", "CAP", "CONFAZ 87",
        "ICMS 0%", "ANÁLISE RECURSAL", "TARJA",
    }

    def eh_coluna_preco(nome: str) -> bool:
        n = _normalizar_texto_cabecalho(nome)
        return bool(
            re.match(
                r"^(PF|PMC|PMVG)\s+(SEM IMPOSTOS|\d+(?:,\d+)?%)$",
                n,
            )
        )

    melhor_idx = None
    melhor_score = -1

    for idx in range(min(len(df), 200)):
        valores = {
            _normalizar_texto_cabecalho(v)
            for v in df.iloc[idx].tolist()
        }
        valores.discard("")

        acertos_id = len(valores & identificadores)
        acertos_preco = sum(eh_coluna_preco(v) for v in valores)
        score = acertos_id * 10 + acertos_preco * 3

        if acertos_id >= 8 and acertos_preco >= 3 and score > melhor_score:
            melhor_idx = idx
            melhor_score = score

    if melhor_idx is None:
        raise ValueError(
            "Não foi possível identificar automaticamente o cabeçalho da CMED."
        )

    logging.info(
        "Cabeçalho CMED detectado na linha Excel %s "
        "(índice pandas %s; score=%s).",
        melhor_idx + 1, melhor_idx, melhor_score,
    )
    return melhor_idx


def normalizar_nomes_colunas(colunas: list) -> list:
    resultado = []

    for col in colunas:
        col_limpo = str(col).replace("\xa0", " ").strip()
        col_limpo = re.sub(r"\s+", " ", col_limpo)
        col_limpo = re.sub(r"\s+%", "%", col_limpo)

        col_upper = col_limpo.upper()

        for alt, padrao in NOMES_ALTERNATIVOS.items():
            alt_norm = re.sub(
                r"\s+",
                " ",
                str(alt).replace("\xa0", " ").strip(),
            ).upper()

            if col_upper == alt_norm:
                col_limpo = padrao
                break

        resultado.append(col_limpo)

    return resultado


def ler_arquivo_cmed(caminho: Path) -> pd.DataFrame:
    logging.info("Lendo arquivo: %s", caminho)

    df_raw = pd.read_excel(
        caminho,
        header=None,
        dtype=str,
        engine="openpyxl",
    )

    idx_header = detectar_linha_cabecalho(df_raw)

    df = pd.read_excel(
        caminho,
        header=idx_header,
        dtype=str,
        engine="openpyxl",
    )

    df.columns = normalizar_nomes_colunas(list(df.columns))
    df.dropna(how="all", inplace=True)

    obrigatorias = {
        "SUBSTÂNCIA", "CNPJ", "LABORATÓRIO",
        "CÓDIGO GGREM", "EAN 1", "PRODUTO", "APRESENTAÇÃO",
    }

    ausentes = sorted(obrigatorias - set(df.columns))
    if ausentes:
        raise ValueError(
            f"Colunas obrigatórias ausentes: {ausentes}"
        )

    logging.info(
        "Arquivo lido: %s linhas × %s colunas.",
        len(df), len(df.columns),
    )
    return df


def extrair_competencia_do_arquivo(caminho: Path) -> str:
    nome = caminho.name

    match = re.search(r"(20\d{2})-(0[1-9]|1[0-2])", nome)
    if match:
        return f"{match.group(1)}-{match.group(2)}"

    match = re.search(r"(20\d{2})(0[1-9]|1[0-2])\d{2}", nome)
    if match:
        return f"{match.group(1)}-{match.group(2)}"

    logging.warning(
        "Não foi possível extrair competência de '%s'. Usando mês atual.",
        nome,
    )
    return datetime.now().strftime("%Y-%m")


def detectar_colunas_preco(df: pd.DataFrame, tipo_preco: str) -> list:
    encontradas = []

    for col in df.columns:
        n = _normalizar_texto_cabecalho(col)

        if not n.startswith(tipo_preco):
            continue

        if " ALC" in n:
            continue

        if re.match(
            r"^(PF|PMC)\s+(SEM IMPOSTOS|\d+(?:,\d+)?%)$",
            n,
        ):
            encontradas.append(col)

    return encontradas


def processar_tabela_precos(
    df: pd.DataFrame,
    caminho: Path,
    tipo_preco: str,
) -> pd.DataFrame:
    competencia = extrair_competencia_do_arquivo(caminho)
    data_carga = datetime.now()

    logging.info(
        "Processando %s — competência: %s",
        tipo_preco, competencia,
    )

    for col_ean in ["EAN 1", "EAN 2", "EAN 3"]:
        if col_ean in df.columns:
            df[col_ean] = df[col_ean].apply(tratar_ean)

    colunas_preco = detectar_colunas_preco(df, tipo_preco)

    if not colunas_preco:
        logging.error(
            "Nenhuma coluna de preço %s encontrada.",
            tipo_preco,
        )
        return pd.DataFrame()

    for col in colunas_preco:
        resultados = df[col].apply(tratar_valor_preco)
        df[col] = resultados.apply(lambda x: x[0])
        df[f"_FLAG_{col}"] = resultados.apply(lambda x: x[1])

    cols_id = [c for c in COLUNAS_IDENTIFICACAO if c in df.columns]
    cols_ean = [c for c in ["EAN 1", "EAN 2", "EAN 3"] if c in df.columns]

    registros = []

    for _, row in df.iterrows():
        eans_validos = [
            row.get(c)
            for c in cols_ean
            if row.get(c) and pd.notna(row.get(c))
        ]

        if not eans_validos:
            eans_validos = [None]

        for ean in eans_validos:
            for col_preco in colunas_preco:
                valor = row[col_preco]

                if valor is None or (
                    isinstance(valor, float) and np.isnan(valor)
                ):
                    continue

                match = re.search(
                    r"(\d+(?:,\d+)?%|Sem Impostos)",
                    col_preco,
                )
                aliquota = match.group(1) if match else col_preco

                ufs = MAPEAMENTO_ICMS_UF.get(aliquota, [])
                if not ufs:
                    ufs = [None]

                registros.extend(
                    {
                        "EAN": ean,
                        "CODIGO_GGREM": row.get("CÓDIGO GGREM"),
                        "PRODUTO": row.get("PRODUTO"),
                        "APRESENTACAO": row.get("APRESENTAÇÃO"),
                        "LABORATORIO": row.get("LABORATÓRIO"),
                        "SUBSTANCIA": row.get("SUBSTÂNCIA"),
                        "TIPO_PRECO": tipo_preco,
                        "ALIQUOTA_ICMS": aliquota,
                        "ESTADO_UF": uf,
                        "VALOR": valor,
                        "FLAG_ASTERISCO": bool(
                            row.get(f"_FLAG_{col_preco}", False)
                        ),
                        "EAN_INVALIDO": (
                            not validar_ean13(ean) if ean else True
                        ),
                        "COMPETENCIA": competencia,
                        "DATA_REFERENCIA": f"{competencia}-01",
                        "DATA_CARGA": data_carga.isoformat(),
                    }
                    for uf in ufs
                )

    resultado = pd.DataFrame(registros)

    logging.info(
        "%s processado: %s registros (%s linhas originais).",
        tipo_preco, len(resultado), len(df),
    )
    return resultado


# ---------------------------------------------------------------------------
# MODELAGEM
# ---------------------------------------------------------------------------

def gerar_dimensao_medicamento(df_raw: pd.DataFrame) -> pd.DataFrame:
    cols_dim = {
        "EAN 1": "EAN",
        "CÓDIGO GGREM": "CODIGO_GGREM",
        "PRODUTO": "PRODUTO",
        "APRESENTAÇÃO": "APRESENTACAO",
        "SUBSTÂNCIA": "SUBSTANCIA",
        "LABORATÓRIO": "LABORATORIO",
        "CNPJ": "CNPJ",
        "CLASSE TERAPÊUTICA": "CLASSE_TERAPEUTICA",
        "F.FARMACÊUTICA": "F_FARMACEUTICA",
        "REGIME DE PREÇO": "REGIME_PRECO",
        "TARJA": "TARJA",
        "RESTRIÇÃO HOSPITALAR": "RESTRICAO_HOSPITALAR",
        "CAP": "CAP",
        "TIPO DE PRODUTO (STATUS DO PRODUTO)": "TIPO_PRODUTO",
    }

    presentes = {k: v for k, v in cols_dim.items() if k in df_raw.columns}
    df_dim = df_raw[list(presentes.keys())].copy()
    df_dim.rename(columns=presentes, inplace=True)

    if "EAN" in df_dim.columns:
        df_dim["EAN"] = df_dim["EAN"].apply(tratar_ean)

    df_dim.dropna(subset=["EAN"], inplace=True)
    df_dim.drop_duplicates(subset=["EAN"], keep="first", inplace=True)

    if "RESTRICAO_HOSPITALAR" in df_dim.columns:
        df_dim["RESTRICAO_HOSPITALAR"] = df_dim[
            "RESTRICAO_HOSPITALAR"
        ].apply(
            lambda x: str(x).strip().upper()
            in ("SIM", "S", "TRUE", "1", "X")
            if pd.notna(x)
            else False
        )

    df_dim.reset_index(drop=True, inplace=True)
    return df_dim


def gerar_dimensao_estado() -> pd.DataFrame:
    uf_aliquota = {}

    for aliq, ufs in MAPEAMENTO_ICMS_UF.items():
        for uf in ufs:
            uf_aliquota[uf] = aliq

    return pd.DataFrame(
        [
            {
                "ESTADO_UF": uf,
                "NOME_ESTADO": nome,
                "REGIAO": regiao,
                "ALIQUOTA_ICMS_VIGENTE": uf_aliquota.get(uf, ""),
            }
            for uf, (nome, regiao) in ESTADOS_BRASIL.items()
        ]
    )


def gerar_dimensao_calendario(
    data_inicio: str = "2024-01-01",
) -> pd.DataFrame:
    ano_atual = datetime.now().year
    data_fim = f"{ano_atual + 1}-12-31"

    datas = pd.date_range(
        start=data_inicio,
        end=data_fim,
        freq="D",
    )

    df = pd.DataFrame({"DATA": datas})
    df["ANO"] = df["DATA"].dt.year
    df["MES"] = df["DATA"].dt.month
    df["NOME_MES"] = df["DATA"].dt.strftime("%B").str.capitalize()
    df["TRIMESTRE"] = df["DATA"].dt.quarter
    df["COMPETENCIA"] = df["DATA"].dt.strftime("%Y-%m")
    df["DATA"] = df["DATA"].dt.strftime("%Y-%m-%d")

    return df


# ---------------------------------------------------------------------------
# EXPORTAÇÃO
# ---------------------------------------------------------------------------

def exportar_para_powerbi(
    df_fato: pd.DataFrame,
    df_medicamento: pd.DataFrame,
    df_estado: pd.DataFrame,
    df_calendario: pd.DataFrame,
    diretorio: str,
    modo_historico: bool = True,
) -> dict:
    dir_saida = Path(diretorio)
    dir_saida.mkdir(parents=True, exist_ok=True)

    encoding = "utf-8-sig"
    arquivos = {}

    caminho_fato = dir_saida / "fato_precos.csv"

    if modo_historico and caminho_fato.exists():
        df_existente = pd.read_csv(
            caminho_fato,
            dtype=str,
            encoding=encoding,
        )

        chave = [
            "EAN", "ESTADO_UF", "TIPO_PRECO",
            "COMPETENCIA", "ALIQUOTA_ICMS",
        ]

        chaves_existentes = set(
            df_existente[chave].apply(
                lambda r: "|".join(str(v) for v in r),
                axis=1,
            )
        )

        mask_novos = (
            df_fato[chave]
            .apply(
                lambda r: "|".join(str(v) for v in r),
                axis=1,
            )
            .apply(lambda x: x not in chaves_existentes)
        )

        df_novos = df_fato[mask_novos]

        if len(df_novos) > 0:
            df_final = pd.concat(
                [df_existente, df_novos],
                ignore_index=True,
            )
            logging.info(
                "Histórico: %s registros novos adicionados (total: %s).",
                len(df_novos), len(df_final),
            )
        else:
            df_final = df_existente
            logging.info("Nenhum registro novo.")
    else:
        df_final = df_fato

    df_final.to_csv(
        caminho_fato,
        index=False,
        encoding=encoding,
    )
    arquivos["fato_precos"] = caminho_fato

    caminho_med = dir_saida / "dim_medicamento.csv"

    if caminho_med.exists():
        df_med_existente = pd.read_csv(
            caminho_med,
            dtype=str,
            encoding=encoding,
        )
        df_medicamento = pd.concat(
            [df_med_existente, df_medicamento],
            ignore_index=True,
        )
        df_medicamento.drop_duplicates(
            subset=["EAN"],
            keep="last",
            inplace=True,
        )

    df_medicamento.to_csv(
        caminho_med,
        index=False,
        encoding=encoding,
    )
    arquivos["dim_medicamento"] = caminho_med

    caminho_est = dir_saida / "dim_estado.csv"
    df_estado.to_csv(
        caminho_est,
        index=False,
        encoding=encoding,
    )
    arquivos["dim_estado"] = caminho_est

    caminho_cal = dir_saida / "dim_calendario.csv"
    df_calendario.to_csv(
        caminho_cal,
        index=False,
        encoding=encoding,
    )
    arquivos["dim_calendario"] = caminho_cal

    return arquivos


# ---------------------------------------------------------------------------
# ORQUESTRADOR
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ETL CMED → Power BI"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Caminho do config.yaml",
    )
    parser.add_argument(
        "--competencia",
        default=None,
        help="Competência forçada (YYYY-MM)",
    )
    args = parser.parse_args()

    config = carregar_config(args.config)
    configurar_log(config)

    inicio = datetime.now()

    try:
        # 1. Descobrir os arquivos publicados pela CMED.
        links = obter_links_cmed(config)

        if not links.get("PMC"):
            raise RuntimeError(
                "Link do arquivo PMC não encontrado na página CMED."
            )

        if not links.get("PF"):
            raise RuntimeError(
                "Link do arquivo PF/PMVG não encontrado na página CMED."
            )

        # 2. Baixar PMC primeiro para descobrir a competência.
        arquivo_pmc = baixar_arquivo_cmed(
            links["PMC"],
            "PMC",
            config,
            args.competencia,
        )

        competencia_detectada = extrair_competencia_do_arquivo(
            arquivo_pmc
        )

        logging.info(
            "Competência detectada no arquivo PMC: %s",
            competencia_detectada,
        )

        # 3. Baixar PF da mesma execução.
        arquivo_pf = baixar_arquivo_cmed(
            links["PF"],
            "PF",
            config,
            args.competencia,
        )

        # 4. Calcular hashes ANTES de processar.
        hashes = {
            "PMC": calcular_hash_arquivo(arquivo_pmc),
            "PF": calcular_hash_arquivo(arquivo_pf),
        }

        logging.info("SHA-256 PMC: %s", hashes["PMC"])
        logging.info("SHA-256 PF : %s", hashes["PF"])

        # 5. Consultar o estado externo às tabelas de negócio.
        estado = carregar_estado(config["arquivo_estado"])

        if estado_ja_processado(
            estado,
            competencia_detectada,
            hashes,
        ):
            duracao = (datetime.now() - inicio).total_seconds()
            logging.info(
                "ETL encerrado sem alterações em %.1fs.",
                duracao,
            )
            return

        logging.info(
            "Nova versão CMED detectada para %s. "
            "Continuando processamento.",
            competencia_detectada,
        )

        # 6. Leitura.
        df_raw_pmc = ler_arquivo_cmed(arquivo_pmc)
        df_raw_pf = ler_arquivo_cmed(arquivo_pf)

        # 7. Transformação.
        df_fato_pmc = processar_tabela_precos(
            df_raw_pmc,
            arquivo_pmc,
            "PMC",
        )
        df_fato_pf = processar_tabela_precos(
            df_raw_pf,
            arquivo_pf,
            "PF",
        )

        df_fato = pd.concat(
            [df_fato_pmc, df_fato_pf],
            ignore_index=True,
        )

        # 8. Dimensões.
        df_medicamento = gerar_dimensao_medicamento(df_raw_pmc)
        df_estado = gerar_dimensao_estado()
        df_calendario = gerar_dimensao_calendario()

        # 9. Exportação dos dados de negócio.
        arquivos = exportar_para_powerbi(
            df_fato,
            df_medicamento,
            df_estado,
            df_calendario,
            config["diretorio_saida"],
        )

        # 10. Backup por competência.
        dir_hist = Path(config["diretorio_historico"])
        dir_hist.mkdir(parents=True, exist_ok=True)

        for nome, caminho in arquivos.items():
            backup = dir_hist / f"{nome}_{competencia_detectada}.csv"
            shutil.copy2(caminho, backup)
            logging.info("Backup: %s", backup)

        # 11. SOMENTE após o processamento ter terminado com sucesso,
        # atualizar o estado.
        novo_estado = {
            "competencia": competencia_detectada,
            "hashes": hashes,
            "arquivos": {
                "PMC": arquivo_pmc.name,
                "PF": arquivo_pf.name,
            },
            "urls": {
                "PMC": links["PMC"],
                "PF": links["PF"],
            },
            "processado_em": datetime.now().isoformat(),
            "registros_fato": int(len(df_fato)),
        }

        salvar_estado(
            config["arquivo_estado"],
            novo_estado,
        )

        duracao = (datetime.now() - inicio).total_seconds()

        logging.info(
            "ETL CMED CONCLUÍDO COM SUCESSO — "
            "competência=%s duração=%.1fs registros=%s",
            competencia_detectada,
            duracao,
            len(df_fato),
        )

        enviar_alerta_email(
            config,
            f"Sucesso — {competencia_detectada}",
            (
                "ETL CMED concluído com sucesso.\n"
                f"Competência: {competencia_detectada}\n"
                f"Registros fato: {len(df_fato)}\n"
                f"Duração: {duracao:.1f}s"
            ),
        )

    except Exception as exc:
        logging.critical(
            "FALHA NO ETL CMED: %s",
            exc,
            exc_info=True,
        )
        enviar_alerta_email(
            config,
            "FALHA NO ETL",
            str(exc),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
