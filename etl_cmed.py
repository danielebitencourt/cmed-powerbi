#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL CMED — Extração, Transformação e Carga de dados ANVISA/CMED para Power BI.

Automatiza o download mensal das tabelas oficiais de preços de medicamentos
(PMC e PF/PMVG), aplica tratamentos de qualidade e exporta em formato
pronto para consumo pelo Power BI via Gateway de Dados.

Uso:
    python etl_cmed.py                  # execução padrão
    python etl_cmed.py --config config.yaml   # configuração customizada
    python etl_cmed.py --competencia 2026-07  # forçar competência específica

Autor: Automação CMED × Power BI
Licença: Uso interno
"""

import argparse
import hashlib
import logging
import re
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

# ═══════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES PADRÃO
# ═══════════════════════════════════════════════════════════════════════

CONFIG_PADRAO = {
    "url_cmed": "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos",
    "diretorio_saida": "dados/processed",
    "diretorio_raw": "dados/raw",
    "diretorio_historico": "dados/historico",
    "diretorio_log": "logs",
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

# ═══════════════════════════════════════════════════════════════════════
# MAPEAMENTO ICMS → UF  (Resolução CM-CMED nº 2/2024 + atualizações)
# ═══════════════════════════════════════════════════════════════════════

MAPEAMENTO_ICMS_UF = {
    "0%":    ["AC", "AM", "AP", "PA", "RO", "RR", "TO", "MT", "MS", "GO", "DF"],
    "12%":   ["ES", "RS"],
    "17%":   ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE", "PR", "SC", "SP"],
    "17,5%": ["RJ"],
    "18%":   ["MG"],
    "19,5%": [],  # Preencher conforme legislação vigente no momento da carga
    "20%":   [],
    "20,5%": [],
    "21%":   [],
    "22%":   [],
}

# Colunas de preço esperadas no arquivo PMC/PF
# A CMED pode alterar a quantidade de alíquotas e inserir espaços antes do "%".
# Por isso, estas listas são mantidas apenas como referência; a seleção real
# das colunas é feita dinamicamente por detectar_colunas_preco().
COLUNAS_PF = ["PF Sem Impostos", "PF 0%", "PF 12%", "PF 17%", "PF 17,5%", "PF 18%", "PF 19%", "PF 19,5%", "PF 20%", "PF 20,5%", "PF 21%", "PF 22%", "PF 22,5%", "PF 23%"]
COLUNAS_PMC = ["PMC Sem Impostos", "PMC 0%", "PMC 12%", "PMC 17%", "PMC 17,5%", "PMC 18%", "PMC 19%", "PMC 19,5%", "PMC 20%", "PMC 20,5%", "PMC 21%", "PMC 22%", "PMC 22,5%", "PMC 23%"]

COLUNAS_IDENTIFICACAO = [
    "SUBSTÂNCIA", "CNPJ", "LABORATÓRIO", "CÓDIGO GGREM", "REGISTRO",
    "EAN 1", "EAN 2", "EAN 3", "PRODUTO", "APRESENTAÇÃO",
    "F.FARMACÊUTICA", "CLASSE TERAPÊUTICA",
    "TIPO DE PRODUTO (STATUS DO PRODUTO)",
    "REGIME DE PREÇO", "TARJA", "RESTRIÇÃO HOSPITALAR",
    "CAP", "CONFAZ 87", "ICMS 0%", "ANÁLISE RECURSAL",
]

# Nomes alternativos conhecidos (ANVISA renomeia colunas ocasionalmente)
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

# Tabela de estados para dimensão
ESTADOS_BRASIL = {
    "AC": ("Acre", "Norte"),
    "AL": ("Alagoas", "Nordeste"),
    "AM": ("Amazonas", "Norte"),
    "AP": ("Amapá", "Norte"),
    "BA": ("Bahia", "Nordeste"),
    "CE": ("Ceará", "Nordeste"),
    "DF": ("Distrito Federal", "Centro-Oeste"),
    "ES": ("Espírito Santo", "Sudeste"),
    "GO": ("Goiás", "Centro-Oeste"),
    "MA": ("Maranhão", "Nordeste"),
    "MG": ("Minas Gerais", "Sudeste"),
    "MS": ("Mato Grosso do Sul", "Centro-Oeste"),
    "MT": ("Mato Grosso", "Centro-Oeste"),
    "PA": ("Pará", "Norte"),
    "PB": ("Paraíba", "Nordeste"),
    "PE": ("Pernambuco", "Nordeste"),
    "PI": ("Piauí", "Nordeste"),
    "PR": ("Paraná", "Sul"),
    "RJ": ("Rio de Janeiro", "Sudeste"),
    "RN": ("Rio Grande do Norte", "Nordeste"),
    "RO": ("Rondônia", "Norte"),
    "RR": ("Roraima", "Norte"),
    "RS": ("Rio Grande do Sul", "Sul"),
    "SC": ("Santa Catarina", "Sul"),
    "SE": ("Sergipe", "Nordeste"),
    "SP": ("São Paulo", "Sudeste"),
    "TO": ("Tocantins", "Norte"),
}


# ═══════════════════════════════════════════════════════════════════════
# UTILITÁRIOS
# ═══════════════════════════════════════════════════════════════════════


def carregar_config(caminho_config: Optional[str] = None) -> dict:
    """Carrega configurações de config.yaml ou usa padrão."""
    config = CONFIG_PADRAO.copy()
    if caminho_config and Path(caminho_config).exists():
        with open(caminho_config, "r", encoding="utf-8") as f:
            custom = yaml.safe_load(f)
            if custom:
                config.update(custom)
        logging.info(f"Configuração carregada de: {caminho_config}")
    return config


def configurar_log(config: dict) -> None:
    """Configura logging com RotatingFileHandler + console."""
    dir_log = Path(config["diretorio_log"])
    dir_log.mkdir(parents=True, exist_ok=True)

    nome_log = f"cmed_etl_{datetime.now().strftime('%Y%m')}.log"
    caminho_log = dir_log / nome_log

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Arquivo (rotação a cada 10 MB, mantém 5 backups)
    fh = RotatingFileHandler(
        caminho_log, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(fh)
    root.addHandler(ch)


def enviar_alerta_email(config: dict, assunto: str, corpo: str) -> None:
    """Envia e-mail de alerta em caso de falha."""
    cfg_email = config.get("email_alerta", {})
    if not cfg_email.get("ativo"):
        return
    try:
        msg = MIMEText(corpo, "plain", "utf-8")
        msg["Subject"] = f"[CMED ETL] {assunto}"
        msg["From"] = cfg_email["remetente"]
        msg["To"] = ", ".join(cfg_email["destinatarios"])

        with smtplib.SMTP(cfg_email["smtp_host"], cfg_email["smtp_porta"]) as server:
            server.starttls()
            server.login(cfg_email["remetente"], cfg_email["senha"])
            server.send_message(msg)
        logging.info("Alerta enviado por e-mail.")
    except Exception as e:
        logging.error(f"Falha ao enviar e-mail de alerta: {e}")


def calcular_hash_arquivo(caminho: Path) -> str:
    """Calcula SHA-256 do arquivo para verificação de integridade."""
    sha = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(8192), b""):
            sha.update(bloco)
    return sha.hexdigest()


# ═══════════════════════════════════════════════════════════════════════
# 1. EXTRAÇÃO — Download dos arquivos CMED
# ═══════════════════════════════════════════════════════════════════════


def obter_links_cmed(config: dict) -> dict:
    """
    Acessa a página da CMED/ANVISA e extrai os links dos arquivos
    PMC (.xlsx site) e PF/PMVG (.xlsx gov) mais recentes.
    """
    url = config["url_cmed"]
    timeout = config["timeout_segundos"]
    headers = {"User-Agent": config["user_agent"]}

    logging.info(f"Acessando página CMED: {url}")
    resp = requests.get(url, headers=headers, timeout=timeout, verify=True)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    links_encontrados = {"PMC": None, "PF": None}

    # Procurar todos os links na página
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        texto = a.get_text(strip=True).lower()

        # PMC — arquivo "site" (xls_conformidade_site_*)
        if "xls_conformidade_site" in href or "pmc" in texto and ".xls" in href:
            if href.endswith((".xlsx", ".xls")):
                links_encontrados["PMC"] = href if href.startswith("http") else (
                    f"https://www.gov.br{href}"
                )

        # PF/PMVG — arquivo "gov" (xls_conformidade_gov_*)
        if "xls_conformidade_gov" in href or "pmvg" in texto and ".xls" in href:
            if href.endswith((".xlsx", ".xls")):
                links_encontrados["PF"] = href if href.startswith("http") else (
                    f"https://www.gov.br{href}"
                )

    # Fallback: procurar links que contenham padrão de data no nome
    if not links_encontrados["PMC"] or not links_encontrados["PF"]:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if re.search(r"xls_conformidade_site_\d{8}", href):
                links_encontrados["PMC"] = href if href.startswith("http") else (
                    f"https://www.gov.br{href}"
                )
            if re.search(r"xls_conformidade_gov_\d{8}", href):
                links_encontrados["PF"] = href if href.startswith("http") else (
                    f"https://www.gov.br{href}"
                )

    for tipo, link in links_encontrados.items():
        if link:
            logging.info(f"Link {tipo} encontrado: {link}")
        else:
            logging.warning(f"Link {tipo} NÃO encontrado na página.")

    return links_encontrados


def baixar_arquivo_cmed(
    url: str,
    tipo: str,
    config: dict,
    competencia: Optional[str] = None,
) -> Path:
    """
    Faz download de um arquivo CMED com retry automático.
    Retorna o Path do arquivo salvo.
    """
    tentativas = config["tentativas_max"]
    intervalo = config["intervalo_retry_segundos"]
    timeout = config["timeout_segundos"]
    headers = {"User-Agent": config["user_agent"]}

    # Extrair competência do nome do arquivo (YYYYMMDD)
    match_data = re.search(r"(\d{8})", url)
    if match_data and not competencia:
        data_str = match_data.group(1)
        competencia = f"{data_str[:4]}-{data_str[4:6]}"

    if not competencia:
        competencia = datetime.now().strftime("%Y-%m")

    # Diretório de destino
    dir_raw = Path(config["diretorio_raw"]) / competencia
    dir_raw.mkdir(parents=True, exist_ok=True)

    # Nome do arquivo local
    nome_arquivo = url.split("/")[-1]
    if not nome_arquivo.endswith((".xlsx", ".xls")):
        nome_arquivo = f"cmed_{tipo.lower()}_{competencia}.xlsx"
    caminho_local = dir_raw / nome_arquivo

    for tentativa in range(1, tentativas + 1):
        try:
            logging.info(
                f"Download {tipo} — tentativa {tentativa}/{tentativas}: {url}"
            )
            resp = requests.get(url, headers=headers, timeout=timeout, stream=True)
            resp.raise_for_status()

            with open(caminho_local, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            tamanho = caminho_local.stat().st_size
            if tamanho < 1024:
                raise ValueError(
                    f"Arquivo muito pequeno ({tamanho} bytes) — possível erro de download."
                )

            sha = calcular_hash_arquivo(caminho_local)
            logging.info(
                f"Download {tipo} concluído: {caminho_local} "
                f"({tamanho:,} bytes, SHA-256: {sha[:16]}...)"
            )
            return caminho_local

        except (requests.RequestException, ValueError) as e:
            logging.error(f"Erro no download {tipo} (tentativa {tentativa}): {e}")
            if tentativa < tentativas:
                logging.info(f"Aguardando {intervalo}s antes de nova tentativa...")
                time.sleep(intervalo)
            else:
                msg = f"Falha permanente no download {tipo} após {tentativas} tentativas: {e}"
                logging.critical(msg)
                enviar_alerta_email(config, f"Falha download {tipo}", msg)
                raise RuntimeError(msg) from e

    raise RuntimeError("Fluxo inesperado na função de download.")


# ═══════════════════════════════════════════════════════════════════════
# 2. TRANSFORMAÇÃO — Tratamento de qualidade
# ═══════════════════════════════════════════════════════════════════════


def validar_ean13(ean: str) -> bool:
    """
    Valida dígito verificador EAN-13.
    Retorna True se o EAN-13 é válido, False caso contrário.
    """
    if not ean or not isinstance(ean, str):
        return False
    ean = re.sub(r"\D", "", str(ean))
    if len(ean) != 13:
        return False
    try:
        soma = 0
        for i, digito in enumerate(ean[:12]):
            peso = 1 if i % 2 == 0 else 3
            soma += int(digito) * peso
        verificador = (10 - (soma % 10)) % 10
        return verificador == int(ean[12])
    except (ValueError, IndexError):
        return False


def tratar_ean(valor) -> Optional[str]:
    """
    Trata valor de EAN: remove não-numéricos, preenche zeros à esquerda.
    Retorna string de 13 dígitos ou None.
    """
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return None
    texto = str(valor).strip()
    if not texto or texto.lower() in ("nan", "none", ""):
        return None
    # Remover caracteres não numéricos
    numeros = re.sub(r"\D", "", texto)
    if not numeros or numeros == "0":
        return None
    # Preencher com zeros à esquerda até 13 dígitos
    return numeros.zfill(13)


def tratar_valor_preco(valor) -> tuple:
    """
    Trata valores de preço da CMED.
    Retorna (valor_float_ou_None, flag_asterisco_bool).
    """
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return (None, False)

    texto = str(valor).strip()
    if not texto or texto.lower() in ("nan", "none", "-", ""):
        return (None, False)

    # Detectar asterisco
    flag_asterisco = "*" in texto
    texto = texto.replace("*", "").strip()

    if not texto:
        return (None, flag_asterisco)

    # Tratar separadores decimais
    # Se tem vírgula E ponto: ponto é milhar, vírgula é decimal (padrão BR)
    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")

    try:
        valor_float = float(texto)
        return (valor_float, flag_asterisco)
    except ValueError:
        logging.warning(f"Valor de preço não numérico ignorado: '{valor}'")
        return (None, flag_asterisco)


def _normalizar_texto_cabecalho(valor) -> str:
    """Normaliza texto de cabeçalho sem depender de posição fixa na planilha."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ""
    texto = str(valor).replace("\xa0", " ").strip()
    texto = re.sub(r"\s+", " ", texto)
    # A CMED alterna entre "PF 12%" e "PF 12 %".
    texto = re.sub(r"\s+%", "%", texto)
    return texto.upper()


def detectar_linha_cabecalho(df: pd.DataFrame) -> int:
    """
    Detecta dinamicamente o cabeçalho real da CMED.

    Não usa linha fixa. A detecção exige um conjunto de nomes de coluna
    realmente presentes na tabela, evitando confundir as notas explicativas
    que aparecem antes do cabeçalho e que também contêm palavras como
    "produto".
    """
    identificadores = {
        "SUBSTÂNCIA", "CNPJ", "LABORATÓRIO", "CÓDIGO GGREM", "REGISTRO",
        "EAN 1", "EAN 2", "EAN 3", "PRODUTO", "APRESENTAÇÃO",
        "CLASSE TERAPÊUTICA", "TIPO DE PRODUTO (STATUS DO PRODUTO)",
        "REGIME DE PREÇO", "RESTRIÇÃO HOSPITALAR", "CAP", "CONFAZ 87",
        "ICMS 0%", "ANÁLISE RECURSAL", "TARJA"
    }

    def eh_coluna_preco(nome: str) -> bool:
        n = _normalizar_texto_cabecalho(nome)
        # Aceita PF/PMC/PMVG e qualquer alíquota que a CMED publicar,
        # mas ignora as colunas "ALC", que são valores derivados.
        return bool(re.match(r"^(PF|PMC|PMVG)\s+(SEM IMPOSTOS|\d+(?:,\d+)?%)$", n))

    melhor_idx = None
    melhor_score = -1

    # As notas ficam no topo; 150 linhas é suficiente para detectar o header
    # sem impor uma posição fixa. Se a CMED futuramente aumentar as notas,
    # ainda assim podemos localizar o cabeçalho em uma janela ampla.
    limite = min(len(df), 200)
    for idx in range(limite):
        valores = [_normalizar_texto_cabecalho(v) for v in df.iloc[idx].tolist()]
        valores = {v for v in valores if v}

        acertos_id = len(valores & identificadores)
        acertos_preco = sum(eh_coluna_preco(v) for v in valores)

        # O cabeçalho real tem vários identificadores + várias colunas de preço.
        score = acertos_id * 10 + acertos_preco * 3
        if acertos_id >= 8 and acertos_preco >= 3 and score > melhor_score:
            melhor_idx = idx
            melhor_score = score

    if melhor_idx is None:
        raise ValueError(
            "Não foi possível identificar automaticamente o cabeçalho da CMED. "
            "O arquivo não será processado para evitar gerar dados incorretos."
        )

    logging.info(
        f"Cabeçalho CMED detectado automaticamente na linha Excel {melhor_idx + 1}. "
        f"(índice pandas {melhor_idx}; score={melhor_score})."
    )
    return melhor_idx


def normalizar_nomes_colunas(colunas: list) -> list:
    """Normaliza nomes, inclusive espaços variáveis antes do símbolo %."""
    resultado = []
    for col in colunas:
        col_limpo = str(col).replace("\xa0", " ").strip()
        col_limpo = re.sub(r"\s+", " ", col_limpo)
        col_limpo = re.sub(r"\s+%", "%", col_limpo)

        col_upper = col_limpo.upper()
        for alt, padrao in NOMES_ALTERNATIVOS.items():
            alt_norm = re.sub(r"\s+", " ", str(alt).replace("\xa0", " ").strip()).upper()
            if col_upper == alt_norm:
                col_limpo = padrao
                break
        resultado.append(col_limpo)
    return resultado


def ler_arquivo_cmed(caminho: Path) -> pd.DataFrame:
    """Lê arquivo CMED, encontra o cabeçalho dinamicamente e valida a estrutura."""
    logging.info(f"Lendo arquivo: {caminho}")

    df_raw = pd.read_excel(caminho, header=None, dtype=str, engine="openpyxl")
    idx_header = detectar_linha_cabecalho(df_raw)

    df = pd.read_excel(caminho, header=idx_header, dtype=str, engine="openpyxl")
    df.columns = normalizar_nomes_colunas(list(df.columns))
    df.dropna(how="all", inplace=True)

    obrigatorias = {"SUBSTÂNCIA", "CNPJ", "LABORATÓRIO", "CÓDIGO GGREM", "EAN 1", "PRODUTO", "APRESENTAÇÃO"}
    ausentes = sorted(obrigatorias - set(df.columns))
    if ausentes:
        raise ValueError(f"Cabeçalho detectado, mas colunas obrigatórias ausentes: {ausentes}")

    logging.info(f"Arquivo lido: {len(df)} linhas × {len(df.columns)} colunas.")
    return df

def extrair_competencia_do_arquivo(caminho: Path) -> str:
    """Extrai YYYY-MM do nome do arquivo; só usa YYYYMMDD como fallback."""
    nome = caminho.name
    match = re.search(r"(20\d{2})-(0[1-9]|1[0-2])", nome)
    if match:
        return f"{match.group(1)}-{match.group(2)}"

    match = re.search(r"(20\d{2})(0[1-9]|1[0-2])\d{2}", nome)
    if match:
        return f"{match.group(1)}-{match.group(2)}"

    logging.warning(
        f"Não foi possível extrair competência de '{nome}'. Usando mês atual."
    )
    return datetime.now().strftime("%Y-%m")


def detectar_colunas_preco(df: pd.DataFrame, tipo_preco: str) -> list:
    """Encontra dinamicamente todas as colunas de preço publicadas pela CMED."""
    prefixos = {"PMC": ("PMC",), "PF": ("PF",)}[tipo_preco]
    encontradas = []
    for col in df.columns:
        n = _normalizar_texto_cabecalho(col)
        if not n.startswith(prefixos):
            continue
        # Não usar colunas ALC: são preços alternativos e não a alíquota-base.
        if " ALC" in n:
            continue
        if re.match(r"^(PF|PMC)\s+(SEM IMPOSTOS|\d+(?:,\d+)?%)$", n):
            encontradas.append(col)
    return encontradas

def processar_tabela_precos(
    df: pd.DataFrame,
    caminho: Path,
    tipo_preco: str,
) -> pd.DataFrame:
    """
    Processa tabela de preços (PMC ou PF): trata EANs, valores, faz unpivot.
    tipo_preco: 'PMC' ou 'PF'
    """
    competencia = extrair_competencia_do_arquivo(caminho)
    data_carga = datetime.now()
    logging.info(f"Processando {tipo_preco} — competência: {competencia}")

    # ── Tratar EANs ────────────────────────────────────────────────
    for col_ean in ["EAN 1", "EAN 2", "EAN 3"]:
        if col_ean in df.columns:
            df[col_ean] = df[col_ean].apply(tratar_ean)

    # ── Identificar colunas de preço disponíveis ───────────────────
    prefixo = tipo_preco
    colunas_preco_presentes = detectar_colunas_preco(df, tipo_preco)

    if not colunas_preco_presentes:
        logging.error(
            f"Nenhuma coluna de preço {prefixo} encontrada. "
            f"Colunas disponíveis: {list(df.columns)}"
        )
        return pd.DataFrame()

    # ── Tratar valores de preço ────────────────────────────────────
    for col in colunas_preco_presentes:
        resultados = df[col].apply(tratar_valor_preco)
        df[col] = resultados.apply(lambda x: x[0])
        # Flag de asterisco por coluna (consolidar depois)
        df[f"_FLAG_{col}"] = resultados.apply(lambda x: x[1])

    # ── Colunas de identificação presentes ─────────────────────────
    cols_id_presentes = [c for c in COLUNAS_IDENTIFICACAO if c in df.columns]
    cols_ean = [c for c in ["EAN 1", "EAN 2", "EAN 3"] if c in df.columns]

    # ── Explodir EANs (uma linha por EAN válido) ───────────────────
    registros = []
    for _, row in df.iterrows():
        eans_validos = []
        for col_ean in cols_ean:
            ean = row.get(col_ean)
            if ean and pd.notna(ean):
                eans_validos.append(ean)

        if not eans_validos:
            eans_validos = [None]

        for ean in eans_validos:
            for col_preco in colunas_preco_presentes:
                valor = row[col_preco]
                if valor is None or (isinstance(valor, float) and np.isnan(valor)):
                    continue

                # Extrair alíquota do nome da coluna (ex: "PMC 17%" → "17%")
                match_aliq = re.search(r"(\d+(?:,\d+)?%|Sem Impostos)", col_preco)
                aliquota = match_aliq.group(1) if match_aliq else col_preco

                flag_ast = row.get(f"_FLAG_{col_preco}", False)

                # Mapear alíquota → UFs
                ufs = MAPEAMENTO_ICMS_UF.get(aliquota, [])

                if not ufs:
                    # Se não há mapeamento (ex: "Sem Impostos" ou alíquota nova),
                    # gerar linha sem UF específica
                    ufs = [None]

                for uf in ufs:
                    registros.append({
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
                        "FLAG_ASTERISCO": bool(flag_ast),
                        "EAN_INVALIDO": not validar_ean13(ean) if ean else True,
                        "COMPETENCIA": competencia,
                        "DATA_REFERENCIA": competencia + "-01",
                        "DATA_CARGA": data_carga.isoformat(),
                    })

    df_resultado = pd.DataFrame(registros)
    logging.info(
        f"{tipo_preco} processado: {len(df_resultado)} registros "
        f"({len(df)} linhas originais)."
    )
    return df_resultado


# ═══════════════════════════════════════════════════════════════════════
# 3. MODELAGEM — Dimensões e Fato
# ═══════════════════════════════════════════════════════════════════════


def gerar_dimensao_medicamento(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Gera tabela dimensão dMedicamento a partir do DataFrame original."""

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

    # Selecionar colunas presentes
    cols_presentes = {k: v for k, v in cols_dim.items() if k in df_raw.columns}
    df_dim = df_raw[list(cols_presentes.keys())].copy()
    df_dim.rename(columns=cols_presentes, inplace=True)

    # Tratar EAN
    if "EAN" in df_dim.columns:
        df_dim["EAN"] = df_dim["EAN"].apply(tratar_ean)

    # Deduplica por EAN (mantém primeiro registro)
    df_dim.dropna(subset=["EAN"], inplace=True)
    df_dim.drop_duplicates(subset=["EAN"], keep="first", inplace=True)

    # Converter restrição hospitalar para booleano
    if "RESTRICAO_HOSPITALAR" in df_dim.columns:
        df_dim["RESTRICAO_HOSPITALAR"] = df_dim["RESTRICAO_HOSPITALAR"].apply(
            lambda x: str(x).strip().upper() in ("SIM", "S", "TRUE", "1", "X")
            if pd.notna(x) else False
        )

    df_dim.reset_index(drop=True, inplace=True)
    logging.info(f"Dimensão medicamento: {len(df_dim)} produtos únicos por EAN.")
    return df_dim


def gerar_dimensao_estado() -> pd.DataFrame:
    """Gera tabela dimensão dEstado."""
    registros = []
    # Montar mapa inverso: UF → alíquota
    uf_aliquota = {}
    for aliq, ufs in MAPEAMENTO_ICMS_UF.items():
        for uf in ufs:
            uf_aliquota[uf] = aliq

    for uf, (nome, regiao) in ESTADOS_BRASIL.items():
        registros.append({
            "ESTADO_UF": uf,
            "NOME_ESTADO": nome,
            "REGIAO": regiao,
            "ALIQUOTA_ICMS_VIGENTE": uf_aliquota.get(uf, ""),
        })

    return pd.DataFrame(registros)


def gerar_dimensao_calendario(
    data_inicio: str = "2024-01-01",
) -> pd.DataFrame:
    """Gera tabela dimensão dCalendario automaticamente até o fim do próximo ano."""

    ano_atual = datetime.now().year
    data_fim = f"{ano_atual + 1}-12-31"

    datas = pd.date_range(start=data_inicio, end=data_fim, freq="D")

    df = pd.DataFrame({"DATA": datas})
    df["ANO"] = df["DATA"].dt.year
    df["MES"] = df["DATA"].dt.month
    df["NOME_MES"] = df["DATA"].dt.strftime("%B").str.capitalize()
    df["TRIMESTRE"] = df["DATA"].dt.quarter
    df["COMPETENCIA"] = df["DATA"].dt.strftime("%Y-%m")
    df["DATA"] = df["DATA"].dt.strftime("%Y-%m-%d")

    return df

    df = pd.DataFrame({"DATA": datas})
    df["ANO"] = df["DATA"].dt.year
    df["MES"] = df["DATA"].dt.month
    df["NOME_MES"] = df["DATA"].dt.strftime("%B").str.capitalize()
    df["TRIMESTRE"] = df["DATA"].dt.quarter
    df["COMPETENCIA"] = df["DATA"].dt.strftime("%Y-%m")
    df["DATA"] = df["DATA"].dt.strftime("%Y-%m-%d")
    return df


# ═══════════════════════════════════════════════════════════════════════
# 4. EXPORTAÇÃO
# ═══════════════════════════════════════════════════════════════════════


def exportar_para_powerbi(
    df_fato: pd.DataFrame,
    df_medicamento: pd.DataFrame,
    df_estado: pd.DataFrame,
    df_calendario: pd.DataFrame,
    diretorio: str,
    modo_historico: bool = True,
) -> dict:
    """
    Exporta DataFrames para CSV (utf-8-sig) prontos para o Power BI.
    Se modo_historico=True, faz append à tabela fato existente.
    """
    dir_saida = Path(diretorio)
    dir_saida.mkdir(parents=True, exist_ok=True)

    encoding = "utf-8-sig"  # compatível com Excel e Power BI
    arquivos = {}

    # ── Fato: append (histórico) ───────────────────────────────────
    caminho_fato = dir_saida / "fato_precos.csv"
    if modo_historico and caminho_fato.exists():
        df_existente = pd.read_csv(caminho_fato, dtype=str, encoding=encoding)
        # Evitar duplicatas pela combinação EAN+UF+TIPO+COMPETENCIA+ALIQUOTA
        chave = ["EAN", "ESTADO_UF", "TIPO_PRECO", "COMPETENCIA", "ALIQUOTA_ICMS"]
        chaves_existentes = set(
            df_existente[chave].apply(lambda r: "|".join(str(v) for v in r), axis=1)
        )
        mask_novos = df_fato[chave].apply(
            lambda r: "|".join(str(v) for v in r), axis=1
        ).apply(lambda x: x not in chaves_existentes)
        df_novos = df_fato[mask_novos]
        if len(df_novos) > 0:
            df_final = pd.concat([df_existente, df_novos], ignore_index=True)
            logging.info(
                f"Histórico: {len(df_novos)} registros novos adicionados "
                f"(total: {len(df_final)})."
            )
        else:
            df_final = df_existente
            logging.info("Nenhum registro novo — dados já existem no histórico.")
    else:
        df_final = df_fato

    df_final.to_csv(caminho_fato, index=False, encoding=encoding)
    arquivos["fato_precos"] = caminho_fato

    # ── Dimensões (sobrescrever — são estáticas/acumulativas) ──────
    caminho_med = dir_saida / "dim_medicamento.csv"
    if caminho_med.exists():
        df_med_existente = pd.read_csv(caminho_med, dtype=str, encoding=encoding)
        df_medicamento = pd.concat(
            [df_med_existente, df_medicamento], ignore_index=True
        )
        df_medicamento.drop_duplicates(subset=["EAN"], keep="last", inplace=True)

    df_medicamento.to_csv(caminho_med, index=False, encoding=encoding)
    arquivos["dim_medicamento"] = caminho_med

    caminho_est = dir_saida / "dim_estado.csv"
    df_estado.to_csv(caminho_est, index=False, encoding=encoding)
    arquivos["dim_estado"] = caminho_est

    caminho_cal = dir_saida / "dim_calendario.csv"
    df_calendario.to_csv(caminho_cal, index=False, encoding=encoding)
    arquivos["dim_calendario"] = caminho_cal

    for nome, caminho in arquivos.items():
        tam = Path(caminho).stat().st_size
        logging.info(f"Exportado: {caminho} ({tam:,} bytes)")

    return arquivos


# ═══════════════════════════════════════════════════════════════════════
# 5. ORQUESTRADOR PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════


def main():
    """Orquestrador principal do ETL CMED."""
    parser = argparse.ArgumentParser(description="ETL CMED → Power BI")
    parser.add_argument("--config", default="config.yaml", help="Caminho do config.yaml")
    parser.add_argument("--competencia", default=None, help="Competência forçada (YYYY-MM)")
    args = parser.parse_args()

    config = carregar_config(args.config)
    configurar_log(config)

    inicio = datetime.now()
    logging.info("=" * 70)
    logging.info(f"INÍCIO ETL CMED — {inicio.isoformat()}")
    logging.info("=" * 70)

    try:
        # 1. Obter links
        links = obter_links_cmed(config)

        if not links.get("PMC"):
            raise RuntimeError("Link do arquivo PMC não encontrado na página CMED.")
        if not links.get("PF"):
            raise RuntimeError("Link do arquivo PF/PMVG não encontrado na página CMED.")

        # 2. Download
        arquivo_pmc = baixar_arquivo_cmed(
            links["PMC"], "PMC", config, args.competencia
        )
        arquivo_pf = baixar_arquivo_cmed(
            links["PF"], "PF", config, args.competencia
        )

        # 3. Leitura
        df_raw_pmc = ler_arquivo_cmed(arquivo_pmc)
        df_raw_pf = ler_arquivo_cmed(arquivo_pf)

        # 4. Processamento (unpivot + mapeamento UF)
        df_fato_pmc = processar_tabela_precos(df_raw_pmc, arquivo_pmc, "PMC")
        df_fato_pf = processar_tabela_precos(df_raw_pf, arquivo_pf, "PF")

        # 5. Combinar fato
        df_fato = pd.concat([df_fato_pmc, df_fato_pf], ignore_index=True)
        logging.info(f"Tabela fato combinada: {len(df_fato)} registros.")

        # 6. Dimensões
        df_medicamento = gerar_dimensao_medicamento(df_raw_pmc)
        df_estado = gerar_dimensao_estado()
        df_calendario = gerar_dimensao_calendario()

        # 7. Exportação
        arquivos = exportar_para_powerbi(
            df_fato, df_medicamento, df_estado, df_calendario,
            config["diretorio_saida"],
        )

        # 8. Backup histórico
        dir_hist = Path(config["diretorio_historico"])
        dir_hist.mkdir(parents=True, exist_ok=True)
        competencia = extrair_competencia_do_arquivo(arquivo_pmc)
        for nome, caminho in arquivos.items():
            backup = dir_hist / f"{nome}_{competencia}.csv"
            import shutil
            shutil.copy2(caminho, backup)
            logging.info(f"Backup: {backup}")

        fim = datetime.now()
        duracao = (fim - inicio).total_seconds()
        logging.info("=" * 70)
        logging.info(
            f"ETL CMED CONCLUÍDO COM SUCESSO — "
            f"duração: {duracao:.1f}s — {len(df_fato)} registros na fato."
        )
        logging.info("=" * 70)

        enviar_alerta_email(
            config,
            f"Sucesso — {competencia}",
            f"ETL CMED concluído com sucesso.\n"
            f"Competência: {competencia}\n"
            f"Registros fato: {len(df_fato)}\n"
            f"Duração: {duracao:.1f}s",
        )

    except Exception as e:
        logging.critical(f"FALHA NO ETL CMED: {e}", exc_info=True)
        enviar_alerta_email(config, "FALHA NO ETL", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
