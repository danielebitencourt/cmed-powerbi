#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL DCB — Lista consolidada das Denominações Comuns Brasileiras (ANVISA).

Descobre a versão VIGENTE da Lista DCB consolidada na Biblioteca Digital da
ANVISA, baixa o .xlsx, padroniza as colunas e exporta para o Power BI.
A cada nova versão registra o que foi incluído, excluído ou alterado.

O link do arquivo muda a cada nova Instrução Normativa (ex.: .../19200/1/
4__Lista_DCB_consolidada_out_2025.xlsx), por isso ele é sempre descoberto
pela página da coleção, nunca fixado no código.

Uso (a partir da pasta dcb/):
    python etl_dcb.py                     # execução padrão
    python etl_dcb.py --config config.yaml
    python etl_dcb.py --forcar            # reprocessa mesmo sem versão nova
    python etl_dcb.py --arquivo lista.xlsx --versao "IN nº 462, de 23/07/2026"
                                          # processa um arquivo local

Autor: Automação CMED × Power BI
Licença: Uso interno
"""

import argparse
import hashlib
import json
import logging
import os
import re
import smtplib
import socket
import sys
import unicodedata
from datetime import datetime
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES PADRÃO
# ═══════════════════════════════════════════════════════════════════════

CONFIG_PADRAO = {
    "url_base": "https://bibliotecadigital.anvisa.gov.br",
    # Coleção "Farmacopeia: Denominações Comuns Brasileiras".
    "url_colecao": (
        "https://bibliotecadigital.anvisa.gov.br/jspui/handle/anvisa/11933"
        "?sort_by=2&order=DESC&rpp=40"
    ),
    "url_rss": "https://bibliotecadigital.anvisa.gov.br/jspui/feed/rss_2.0/anvisa/11933",
    "diretorio_saida": "dados/processed",
    "diretorio_original": "dados/original",
    "diretorio_log": "logs",
    "diretorio_temp": "tmp",  # downloads em andamento (não versionado)
    "arquivo_estado": "estado.json",
    "timeout_segundos": 90,
    "tentativas_max": 3,
    "forcar_ipv4": True,
    "linhas_minimas": 5000,
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

# Legenda publicada pela ANVISA no resumo de cada versão da lista.
CLASSIFICACOES = {
    "BIO": "Produtos biológicos",
    "EXA": "Excipientes e adjuvantes",
    "HOM": "Homeopáticos",
    "IFA": "Insumos farmacêuticos ativos",
    "PM": "Espécies vegetais",
    "RAD": "Radiofármacos",
    "INF": "Insumos não classificados no processo de estabelecimento de DCB",
    # Surgiu na IN nº 462/2026 (óxido nítrico, monóxido de carbono) sem
    # descrição na legenda oficial.
    "OUTRO": "Outros (sem descrição na legenda da ANVISA)",
}

COLUNAS_SAIDA = [
    "NUM_DCB", "DENOMINACAO", "NUM_CAS", "CLASSIFICACAO",
    "CLASSIFICACAO_DESC", "HISTORICO", "VERSAO", "DATA_CARGA",
]

MESES = {
    "janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5,
    "junho": 6, "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}


# ═══════════════════════════════════════════════════════════════════════
# INFRAESTRUTURA (config, log, alerta, HTTP, estado)
# ═══════════════════════════════════════════════════════════════════════

def carregar_config(caminho_config: Optional[str] = None) -> dict:
    """Carrega configurações de config.yaml ou usa padrão."""
    config = CONFIG_PADRAO.copy()
    if caminho_config and Path(caminho_config).exists():
        with open(caminho_config, "r", encoding="utf-8") as f:
            custom = yaml.safe_load(f)
            if custom:
                config.update(custom)

    # Segurança: credenciais NUNCA devem ficar no repositório.
    # Elas são lidas de variáveis de ambiente (GitHub Secrets) quando existirem.
    cfg_email = config.setdefault("email_alerta", {})
    if os.getenv("SMTP_REMETENTE"):
        cfg_email["remetente"] = os.environ["SMTP_REMETENTE"]
    if os.getenv("SMTP_SENHA"):
        cfg_email["senha"] = os.environ["SMTP_SENHA"]
    if os.getenv("SMTP_DESTINATARIOS"):
        cfg_email["destinatarios"] = [
            e.strip() for e in os.environ["SMTP_DESTINATARIOS"].split(",") if e.strip()
        ]
    if os.getenv("EMAIL_ALERTA_ATIVO"):
        cfg_email["ativo"] = os.environ["EMAIL_ALERTA_ATIVO"].lower() in ("1", "true", "sim")

    return config


def configurar_log(config: dict) -> None:
    """Configura logging com RotatingFileHandler + console."""
    dir_log = Path(config["diretorio_log"])
    dir_log.mkdir(parents=True, exist_ok=True)
    caminho_log = dir_log / f"dcb_etl_{datetime.now().strftime('%Y%m')}.log"

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
    """Envia e-mail de alerta (somente se ativado via Secrets)."""
    cfg_email = config.get("email_alerta", {})
    if not cfg_email.get("ativo"):
        return
    try:
        msg = MIMEText(corpo, "plain", "utf-8")
        msg["Subject"] = f"[DCB ETL] {assunto}"
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


def forcar_ipv4() -> None:
    """
    Restringe as conexões a IPv4. Os runners do GitHub Actions normalmente
    não têm rota IPv6 de saída e o gov.br publica registro AAAA, o que causa
    '[Errno 101] Network is unreachable' (mesmo ajuste do ETL CMED).
    """
    try:
        import urllib3.util.connection as urllib3_cn
        urllib3_cn.allowed_gai_family = lambda: socket.AF_INET
    except Exception as e:  # pragma: no cover
        logging.debug(f"Não foi possível ajustar urllib3 para IPv4: {e}")

    _orig_getaddrinfo = socket.getaddrinfo

    def _getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
        resultados = _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
        return resultados or _orig_getaddrinfo(host, port, family, type, proto, flags)

    socket.getaddrinfo = _getaddrinfo_ipv4


def criar_sessao_http(config: dict) -> requests.Session:
    """Sessão HTTP com retry e backoff (o gov.br oscila bastante no CI)."""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    if config.get("forcar_ipv4", True):
        forcar_ipv4()

    sessao = requests.Session()
    retry = Retry(
        total=config.get("tentativas_max", 3),
        connect=config.get("tentativas_max", 3),
        read=config.get("tentativas_max", 3),
        backoff_factor=2,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sessao.mount("https://", adapter)
    sessao.mount("http://", adapter)
    sessao.headers.update({
        "User-Agent": config["user_agent"],
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*;q=0.8"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    })
    return sessao


def carregar_estado(config: dict) -> dict:
    """Lê o arquivo de estado (última versão processada)."""
    caminho = Path(config["arquivo_estado"])
    if caminho.exists():
        try:
            return json.loads(caminho.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            logging.warning(f"Estado ilegível ({e}). Iniciando estado vazio.")
    return {}


def salvar_estado(config: dict, estado: dict) -> None:
    caminho = Path(config["arquivo_estado"])
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(json.dumps(estado, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info(f"Estado salvo: {caminho}")


def _sem_acento(texto: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", str(texto))
        if not unicodedata.combining(c)
    )


# ═══════════════════════════════════════════════════════════════════════
# 1. EXTRAÇÃO — descobrir a versão vigente e baixar o .xlsx
# ═══════════════════════════════════════════════════════════════════════

def _get(sessao: requests.Session, url: str, config: dict) -> requests.Response:
    resp = sessao.get(url, timeout=config["timeout_segundos"])
    resp.raise_for_status()
    return resp


def _numero_handle(href: str) -> int:
    m = re.search(r"/handle/anvisa/(\d+)", href or "")
    return int(m.group(1)) if m else -1


def _versao_do_titulo(titulo: str) -> str:
    """'Lista consolidada das DCB: versão IN nº 462, de ... - VIGENTE' → 'IN nº 462, de ...'."""
    m = re.search(r"vers[aã]o\s+(.*)", titulo, flags=re.I)
    versao = m.group(1) if m else titulo
    versao = re.sub(r"\s*[-–—]\s*(n[aã]o\s+)?vigente\s*$", "", versao, flags=re.I)
    return versao.strip()


def _data_do_titulo(titulo: str) -> Optional[str]:
    """Extrai a data do ato ('23 de julho de 2026' ou '01/11/2024') em ISO."""
    t = _sem_acento(titulo).lower()
    m = re.search(r"(\d{1,2})\s+de\s+([a-z]+)\s+de\s+(\d{4})", t)
    if m and m.group(2) in MESES:
        return f"{int(m.group(3)):04d}-{MESES[m.group(2)]:02d}-{int(m.group(1)):02d}"
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", t)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def descobrir_versao_vigente(sessao: requests.Session, config: dict) -> dict:
    """
    Lista os itens "Lista consolidada das DCB" da coleção e escolhe o
    marcado como VIGENTE. Se nenhum estiver marcado, usa o de maior número
    de handle (o mais recente cadastrado).
    """
    itens = {}
    try:
        html = _get(sessao, config["url_colecao"], config).text
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            titulo = " ".join(a.get_text(" ", strip=True).split())
            if "lista consolidada das dcb" in _sem_acento(titulo).lower():
                url = urljoin(config["url_base"], a["href"])
                itens[_numero_handle(url)] = {"titulo": titulo, "url_item": url}
    except Exception as e:
        logging.warning(f"Página da coleção indisponível ({e}). Tentando o RSS.")

    if not itens:
        xml = _get(sessao, config["url_rss"], config).text
        soup = BeautifulSoup(xml, "html.parser")
        for item in soup.find_all("item"):
            titulo = (item.title.get_text(strip=True) if item.title else "")
            link = item.find("link")
            url = (link.get_text(strip=True) or link.next_sibling or "") if link else ""
            if "lista consolidada das dcb" in _sem_acento(titulo).lower():
                itens[_numero_handle(str(url))] = {"titulo": titulo, "url_item": str(url).strip()}

    itens.pop(-1, None)
    if not itens:
        raise RuntimeError("Nenhum item 'Lista consolidada das DCB' encontrado na coleção.")

    def _eh_vigente(titulo: str) -> bool:
        t = _sem_acento(titulo).lower()
        return "vigente" in t and "nao vigente" not in t

    vigentes = [h for h, i in itens.items() if _eh_vigente(i["titulo"])]
    if len(vigentes) > 1:
        logging.warning(f"Mais de um item marcado como VIGENTE: {sorted(vigentes)}. Usando o mais recente.")
    if not vigentes:
        logging.warning("Nenhum item marcado como VIGENTE. Usando o de maior handle.")
    handle = max(vigentes) if vigentes else max(itens)

    escolhido = itens[handle]
    escolhido["handle"] = handle
    escolhido["versao"] = _versao_do_titulo(escolhido["titulo"])
    escolhido["data_ato"] = _data_do_titulo(escolhido["titulo"])
    escolhido["url_arquivo"] = _link_xlsx_do_item(sessao, escolhido["url_item"], config)
    logging.info(f"Versão vigente: {escolhido['versao']} (handle {handle})")
    logging.info(f"Arquivo: {escolhido['url_arquivo']}")
    return escolhido


def _link_xlsx_do_item(sessao: requests.Session, url_item: str, config: dict) -> str:
    html = _get(sessao, url_item, config).text
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/bitstream/" in href and re.search(r"\.xlsx?($|\?)", href, flags=re.I):
            return urljoin(config["url_base"], href)
    raise RuntimeError(f"Nenhum .xlsx encontrado na página do item: {url_item}")


def baixar_arquivo(sessao: requests.Session, url: str, destino: Path, config: dict) -> Path:
    """Baixa para um arquivo temporário e só substitui o destino se for válido."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    temp = destino.with_suffix(destino.suffix + ".part")
    resp = sessao.get(url, timeout=config["timeout_segundos"], stream=True)
    resp.raise_for_status()
    with open(temp, "wb") as f:
        for bloco in resp.iter_content(65536):
            f.write(bloco)
    with open(temp, "rb") as f:
        if f.read(2) != b"PK":  # .xlsx é um ZIP
            temp.unlink(missing_ok=True)
            raise RuntimeError("O arquivo baixado não é um .xlsx válido (provável página de erro).")
    logging.info(f"Download concluído: {temp.stat().st_size / 1024:.0f} KB")
    return temp


# ═══════════════════════════════════════════════════════════════════════
# 2. TRANSFORMAÇÃO
# ═══════════════════════════════════════════════════════════════════════

def _identificar_coluna(nome: str) -> Optional[str]:
    n = _sem_acento(nome).upper()
    if "DENOMINA" in n:
        return "DENOMINACAO"
    if "CAS" in n.split() or n.replace(" ", "").endswith("CAS"):
        return "NUM_CAS"
    if "DCB" in n:
        return "NUM_DCB"
    if "CLASSIFICA" in n:
        return "CLASSIFICACAO"
    if "HISTOR" in n:
        return "HISTORICO"
    return None


def ler_lista_dcb(caminho: Path, versao: str, linhas_minimas: int) -> pd.DataFrame:
    """Lê o .xlsx detectando a linha de cabeçalho (a planilha tem um título acima)."""
    bruto = pd.read_excel(caminho, header=None, dtype=str, engine="openpyxl")

    linha_cab = None
    for i in range(min(30, len(bruto))):
        valores = " ".join(_sem_acento(v).upper() for v in bruto.iloc[i].dropna())
        if "DENOMINA" in valores and "DCB" in valores:
            linha_cab = i
            break
    if linha_cab is None:
        raise RuntimeError("Cabeçalho da Lista DCB não encontrado (coluna 'DENOMINAÇÃO').")

    df = bruto.iloc[linha_cab + 1:].copy()
    nomes = {}
    for idx, valor in bruto.iloc[linha_cab].items():
        destino = _identificar_coluna(valor) if pd.notna(valor) else None
        if destino and destino not in nomes.values():
            nomes[idx] = destino
    faltando = {"NUM_DCB", "DENOMINACAO"} - set(nomes.values())
    if faltando:
        raise RuntimeError(f"Colunas obrigatórias ausentes na planilha: {sorted(faltando)}")
    df = df[list(nomes)].rename(columns=nomes)
    for col in COLUNAS_SAIDA:
        if col not in df.columns:
            df[col] = None

    for col in ("DENOMINACAO", "NUM_CAS", "CLASSIFICACAO", "HISTORICO"):
        df[col] = df[col].astype("string").str.replace(r"\s+", " ", regex=True).str.strip()
    df["CLASSIFICACAO"] = df["CLASSIFICACAO"].str.upper()
    df["NUM_DCB"] = pd.to_numeric(df["NUM_DCB"], errors="coerce").astype("Int64")

    df = df[df["NUM_DCB"].notna() & df["DENOMINACAO"].notna() & (df["DENOMINACAO"] != "")]
    duplicados = df["NUM_DCB"].duplicated(keep="first")
    if duplicados.any():
        logging.warning(f"{int(duplicados.sum())} Nº DCB duplicado(s) — mantida a 1ª ocorrência.")
        df = df[~duplicados]

    df["CLASSIFICACAO_DESC"] = df["CLASSIFICACAO"].map(CLASSIFICACOES)
    df["VERSAO"] = versao
    df["DATA_CARGA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df[COLUNAS_SAIDA].sort_values("NUM_DCB").reset_index(drop=True)

    if len(df) < linhas_minimas:
        raise RuntimeError(
            f"Somente {len(df)} DCBs lidas (mínimo esperado: {linhas_minimas}). "
            "Arquivo provavelmente incompleto — nada foi substituído."
        )
    desconhecidas = sorted(set(df["CLASSIFICACAO"].dropna()) - set(CLASSIFICACOES))
    if desconhecidas:
        logging.warning(f"Classificações sem descrição na legenda: {desconhecidas}")
    logging.info(f"Lista DCB lida: {len(df)} denominações.")
    return df


def comparar_versoes(anterior: pd.DataFrame, atual: pd.DataFrame, versao: str) -> pd.DataFrame:
    """Gera as alterações entre a versão anterior e a atual, chave = Nº DCB."""
    campos = ["DENOMINACAO", "NUM_CAS", "CLASSIFICACAO"]
    a = anterior.set_index("NUM_DCB")
    b = atual.set_index("NUM_DCB")
    linhas = []

    for num in b.index.difference(a.index):
        linhas.append({"NUM_DCB": num, "TIPO_ALTERACAO": "INCLUIDA", "CAMPO": None,
                       "VALOR_ANTERIOR": None, "VALOR_NOVO": b.at[num, "DENOMINACAO"]})
    for num in a.index.difference(b.index):
        linhas.append({"NUM_DCB": num, "TIPO_ALTERACAO": "EXCLUIDA", "CAMPO": None,
                       "VALOR_ANTERIOR": a.at[num, "DENOMINACAO"], "VALOR_NOVO": None})
    for num in a.index.intersection(b.index):
        for campo in campos:
            va = a.at[num, campo] if campo in a.columns else None
            vb = b.at[num, campo]
            va = None if pd.isna(va) else str(va)
            vb = None if pd.isna(vb) else str(vb)
            if va != vb:
                linhas.append({"NUM_DCB": num, "TIPO_ALTERACAO": "ALTERADA", "CAMPO": campo,
                               "VALOR_ANTERIOR": va, "VALOR_NOVO": vb})

    df = pd.DataFrame(linhas, columns=["NUM_DCB", "TIPO_ALTERACAO", "CAMPO",
                                       "VALOR_ANTERIOR", "VALOR_NOVO"])
    df.insert(0, "VERSAO_ANTERIOR", anterior["VERSAO"].iloc[0] if len(anterior) else None)
    df.insert(1, "VERSAO_NOVA", versao)
    df["DATA_CARGA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df.sort_values(["TIPO_ALTERACAO", "NUM_DCB"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════
# 3. CARGA
# ═══════════════════════════════════════════════════════════════════════

def exportar(df: pd.DataFrame, info: dict, temp_xlsx: Path, config: dict) -> dict:
    dir_saida = Path(config["diretorio_saida"])
    dir_saida.mkdir(parents=True, exist_ok=True)
    encoding = "utf-8-sig"  # Excel e Power BI leem acentos corretamente
    caminho_dim = dir_saida / "dim_dcb.csv"
    caminho_alt = dir_saida / "dcb_alteracoes.csv"
    caminho_ver = dir_saida / "dcb_versoes.csv"

    # Alterações em relação à versão anterior (acumulado).
    resumo = {"INCLUIDA": 0, "EXCLUIDA": 0, "ALTERADA": 0}
    if caminho_dim.exists():
        anterior = pd.read_csv(caminho_dim, dtype=str, encoding=encoding)
        anterior["NUM_DCB"] = pd.to_numeric(anterior["NUM_DCB"], errors="coerce").astype("Int64")
        if len(anterior) and anterior["VERSAO"].iloc[0] != info["versao"]:
            alteracoes = comparar_versoes(anterior, df, info["versao"])
            resumo.update(alteracoes["TIPO_ALTERACAO"].value_counts().to_dict())
            if caminho_alt.exists():
                historico = pd.read_csv(caminho_alt, dtype=str, encoding=encoding)
                historico = historico[historico["VERSAO_NOVA"] != info["versao"]]
                alteracoes = pd.concat([historico, alteracoes.astype("string")], ignore_index=True)
            alteracoes.to_csv(caminho_alt, index=False, encoding=encoding)
            logging.info(f"Alterações vs. versão anterior: {resumo}")

    df.to_csv(caminho_dim, index=False, encoding=encoding)

    # Log de versões processadas.
    nova_versao = pd.DataFrame([{
        "VERSAO": info["versao"],
        "DATA_ATO": info.get("data_ato"),
        "TITULO": info["titulo"],
        "URL_ITEM": info["url_item"],
        "URL_ARQUIVO": info["url_arquivo"],
        "QTD_DCB": len(df),
        "INCLUIDAS": resumo["INCLUIDA"],
        "EXCLUIDAS": resumo["EXCLUIDA"],
        "ALTERADAS": resumo["ALTERADA"],
        "DATA_CARGA": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }])
    if caminho_ver.exists():
        versoes = pd.read_csv(caminho_ver, dtype=str, encoding=encoding)
        versoes = versoes[versoes["VERSAO"] != info["versao"]]
        nova_versao = pd.concat([versoes, nova_versao.astype("string")], ignore_index=True)
    nova_versao.to_csv(caminho_ver, index=False, encoding=encoding)

    # Planilha original vigente (sobrescrita a cada nova versão).
    dir_orig = Path(config["diretorio_original"])
    dir_orig.mkdir(parents=True, exist_ok=True)
    caminho_orig = dir_orig / "Lista_DCB_consolidada_vigente.xlsx"
    temp_xlsx.replace(caminho_orig)

    logging.info(f"Exportado: {caminho_dim}, {caminho_ver}, {caminho_orig}")
    return resumo


# ═══════════════════════════════════════════════════════════════════════
# ORQUESTRAÇÃO
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="ETL Lista DCB → Power BI")
    parser.add_argument("--config", default="config.yaml", help="Caminho do config.yaml")
    parser.add_argument("--forcar", action="store_true", help="Reprocessa mesmo sem versão nova")
    parser.add_argument("--arquivo", default=None, help="Processa um .xlsx local em vez de baixar")
    parser.add_argument("--versao", default=None, help="Rótulo da versão (com --arquivo)")
    args = parser.parse_args()

    config = carregar_config(args.config)
    configurar_log(config)
    estado = carregar_estado(config)

    inicio = datetime.now()
    logging.info("=" * 70)
    logging.info(f"INÍCIO ETL DCB — {inicio.isoformat()}")
    logging.info("=" * 70)

    try:
        if args.arquivo:
            origem = Path(args.arquivo)
            versao = args.versao or origem.stem
            info = {"versao": versao, "titulo": versao, "handle": None,
                    "data_ato": _data_do_titulo(versao),
                    "url_item": None, "url_arquivo": str(origem)}
            temp = Path(config["diretorio_temp"]) / "manual.xlsx.part"
            temp.parent.mkdir(parents=True, exist_ok=True)
            temp.write_bytes(origem.read_bytes())
        else:
            sessao = criar_sessao_http(config)
            info = descobrir_versao_vigente(sessao, config)
            if (not args.forcar and estado.get("handle") == info["handle"]
                    and estado.get("url_arquivo") == info["url_arquivo"]):
                logging.info(f"Sem atualização: {info['versao']} já foi processada.")
                return
            temp = baixar_arquivo(
                sessao, info["url_arquivo"],
                Path(config["diretorio_temp"]) / "download.xlsx", config,
            )

        sha = calcular_hash_arquivo(temp)
        if not args.forcar and estado.get("sha256") == sha:
            logging.info("Sem atualização: arquivo idêntico ao já processado.")
            temp.unlink(missing_ok=True)
            estado.update({k: info.get(k) for k in ("handle", "url_item", "url_arquivo")})
            salvar_estado(config, estado)
            return

        df = ler_lista_dcb(temp, info["versao"], config["linhas_minimas"])
        resumo = exportar(df, info, temp, config)

        estado.update({
            "versao": info["versao"],
            "data_ato": info.get("data_ato"),
            "handle": info.get("handle"),
            "url_item": info.get("url_item"),
            "url_arquivo": info.get("url_arquivo"),
            "sha256": sha,
            "qtd_dcb": int(len(df)),
            "ultima_execucao": datetime.now().isoformat(),
        })
        salvar_estado(config, estado)

        duracao = (datetime.now() - inicio).total_seconds()
        logging.info("=" * 70)
        logging.info(f"ETL DCB CONCLUÍDO — {info['versao']} — {len(df)} DCBs — {duracao:.1f}s")
        logging.info("=" * 70)

        # Resumo na página da execução do GitHub Actions.
        resumo_gh = os.getenv("GITHUB_STEP_SUMMARY")
        if resumo_gh:
            with open(resumo_gh, "a", encoding="utf-8") as f:
                f.write(
                    f"### Lista DCB atualizada\n\n"
                    f"- Versão: **{info['versao']}**\n"
                    f"- DCBs: {len(df)}\n"
                    f"- Incluídas: {resumo['INCLUIDA']} · Excluídas: {resumo['EXCLUIDA']}"
                    f" · Alteradas: {resumo['ALTERADA']}\n"
                    f"- Fonte: {info['url_arquivo']}\n"
                )

        enviar_alerta_email(
            config,
            f"Nova versão — {info['versao']}",
            f"Lista DCB atualizada.\nVersão: {info['versao']}\nDCBs: {len(df)}\n"
            f"Incluídas: {resumo['INCLUIDA']} | Excluídas: {resumo['EXCLUIDA']} | "
            f"Alteradas: {resumo['ALTERADA']}\nFonte: {info['url_arquivo']}",
        )

    except Exception as e:
        logging.critical(f"FALHA NO ETL DCB: {e}", exc_info=True)
        enviar_alerta_email(config, "FALHA NO ETL", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
