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
import json
import logging
import os
import re
import smtplib
import socket
import sys
import time
import unicodedata
from datetime import datetime
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

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
    "arquivo_estado": ".github/cmed/estado.json",
    "timeout_segundos": 90,
    "tentativas_max": 3,
    "intervalo_retry_segundos": 30,
    "forcar_ipv4": True,
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


def forcar_ipv4() -> None:
    """
    Força todas as conexões HTTP a usarem IPv4.

    No GitHub Actions o runner normalmente NÃO tem rota IPv6 de saída. Como o
    gov.br publica endereço IPv6 (registro AAAA), o requests pode tentar IPv6
    e falhar com '[Errno 101] Network is unreachable'. Localmente isso não
    ocorre. Restringir a resolução de nomes a IPv4 resolve o problema.
    """
    # Caminho 1 (preferido): urllib3 respeita allowed_gai_family.
    try:
        import urllib3.util.connection as urllib3_cn

        def _somente_ipv4():
            return socket.AF_INET

        urllib3_cn.allowed_gai_family = _somente_ipv4
    except Exception as e:  # pragma: no cover
        logging.debug(f"Não foi possível ajustar urllib3 para IPv4: {e}")

    # Caminho 2 (reforço): filtra getaddrinfo para AF_INET no processo todo.
    _orig_getaddrinfo = socket.getaddrinfo

    def _getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
        resultados = _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
        return resultados or _orig_getaddrinfo(host, port, family, type, proto, flags)

    socket.getaddrinfo = _getaddrinfo_ipv4
    logging.info("Rede: conexões restritas a IPv4 (evita ENETUNREACH no CI).")


def criar_sessao_http(config: dict) -> requests.Session:
    """
    Cria uma sessão HTTP com retry automático a nível de conexão.

    Isto é essencial no GitHub Actions: os runners rodam em datacenter
    (Azure) e o gov.br frequentemente responde com 403/429/503 ou derruba
    a conexão. O retry com backoff recupera falhas transitórias que não
    acontecem na sua máquina local.
    """
    from requests.adapters import HTTPAdapter

    if config.get("forcar_ipv4", True):
        forcar_ipv4()

    try:
        from urllib3.util.retry import Retry
    except ImportError:  # urllib3 < 1.26
        from requests.packages.urllib3.util.retry import Retry  # type: ignore

    sessao = requests.Session()
    retry = Retry(
        total=config.get("tentativas_max", 3),
        connect=config.get("tentativas_max", 3),
        read=config.get("tentativas_max", 3),
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sessao.mount("https://", adapter)
    sessao.mount("http://", adapter)

    # Cabeçalhos "de navegador" reduzem bloqueios do WAF do gov.br.
    # NÃO forçamos Accept-Encoding: deixamos o requests anunciar apenas os
    # formatos que sabe descomprimir (gzip/deflate). Forçar "br" (brotli)
    # sem a lib instalada faz o corpo chegar ilegível e nenhum link é achado.
    sessao.headers.update({
        "User-Agent": config["user_agent"],
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
            "application/vnd.ms-excel,*/*;q=0.8"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    return sessao


def carregar_estado(config: dict) -> dict:
    """Lê o arquivo de estado (última competência processada)."""
    caminho = Path(config.get("arquivo_estado", ".github/cmed/estado.json"))
    if caminho.exists():
        try:
            return json.loads(caminho.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            logging.warning(f"Estado ilegível ({e}). Iniciando estado vazio.")
    return {}


def salvar_estado(config: dict, estado: dict) -> None:
    """
    Grava o arquivo de estado. Isto também garante que o caminho exista
    para o `git add .github/cmed/estado.json` do workflow não falhar.
    """
    caminho = Path(config.get("arquivo_estado", ".github/cmed/estado.json"))
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_text(
        json.dumps(estado, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logging.info(f"Estado salvo: {caminho}")


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
    sessao = config.get("_sessao") or criar_sessao_http(config)

    logging.info(f"Acessando página CMED: {url}")
    resp = sessao.get(url, timeout=(20, timeout), verify=True)
    if resp.status_code != 200:
        # Diagnóstico: no CI, gov.br costuma devolver 403 (bloqueio de WAF/IP
        # de datacenter). O trecho abaixo ajuda a distinguir bloqueio de bug.
        amostra = resp.text[:500].replace("\n", " ")
        logging.error(
            f"Página CMED respondeu HTTP {resp.status_code}. "
            f"Início da resposta: {amostra!r}"
        )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    def desescapar(s: str) -> str:
        # Links vindos de blocos JSON/JS trazem barras escapadas: \u002F, \/ etc.
        s = s.replace("\\/", "/")
        s = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), s)
        return s.strip()

    def absolutizar(href: str) -> str:
        href = desescapar(href)
        if href.startswith("http"):
            return href
        if href.startswith("//"):
            return "https:" + href
        if href.startswith("/"):
            return "https://www.gov.br" + href
        return urljoin(url.rstrip("/") + "/", href)

    links_encontrados = {"PMC": None, "PF": None}

    def texto_link(a) -> str:
        t = a.get_text(" ", strip=True).lower()
        return re.sub(r"\s+", " ", t)

    # ── Estratégia 1: identificar pelo TEXTO do link (mais robusta) ──
    # A página lista âncoras que começam com "PMC - xls ..." e "PMVG - xls ...".
    # Isso independe do formato da URL, que pode vir como:
    #   .../xls_conformidade_site_AAAAMMDD_xxxx.xlsx/@@download/file
    #   .../resolveuid/<uuid>            (o Plone às vezes entrega assim)
    #   .../xls_conformidade_site_...xlsx  (URL direta — ambiente local)
    for a in soup.find_all("a", href=True):
        inicio = texto_link(a)[:30]
        href = a["href"]
        eh_xls = ("xls" in inicio) or href.lower().endswith((".xlsx", ".xls"))
        if not eh_xls:
            continue
        if links_encontrados["PMC"] is None and re.search(r"\bpmc\b", inicio):
            links_encontrados["PMC"] = absolutizar(href)
        elif links_encontrados["PF"] is None and re.search(r"\bpmvg\b", inicio):
            links_encontrados["PF"] = absolutizar(href)

    # ── Estratégia 2 (fallback): padrão do nome do arquivo no href ──
    if not links_encontrados["PMC"] or not links_encontrados["PF"]:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Nomes atuais da ANVISA: lista_pmc_ / lista_pmvg_
            # Nomes antigos (compat.): xls_conformidade_site_ / _gov_
            if links_encontrados["PMC"] is None and re.search(r"lista_pmc|xls_conformidade_site", href, re.I):
                links_encontrados["PMC"] = absolutizar(href)
            if links_encontrados["PF"] is None and re.search(r"lista_pmvg|xls_conformidade_gov", href, re.I):
                links_encontrados["PF"] = absolutizar(href)

    # ── Estratégia 3 (fallback amplo): qualquer .xls* com "conformidade" ──
    if not links_encontrados["PMC"] or not links_encontrados["PF"]:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            h = href.lower()
            if ".xls" not in h:
                continue
            if links_encontrados["PMC"] is None and ("lista_pmc" in h or ("conformidade" in h and "site" in h)):
                links_encontrados["PMC"] = absolutizar(href)
            if links_encontrados["PF"] is None and ("lista_pmvg" in h or ("conformidade" in h and "gov" in h)):
                links_encontrados["PF"] = absolutizar(href)

    # ── Estratégia 4 (à prova de falhas): regex no HTML BRUTO ──────
    # O html.parser do Python às vezes descarta o trecho com esses links
    # (HTML malformado do gov.br). Aqui procuramos direto no texto cru,
    # sem depender do parser. Os arquivos ficam sempre em .../arquivos/
    # e começam com xls_conformidade_site_ (PMC) ou xls_conformidade_gov_ (PF).
    if not links_encontrados["PMC"] or not links_encontrados["PF"]:
        padrao = re.compile(
            r'([^\s"\'<>()]*(?:lista_(pmc|pmvg)|xls_conformidade_(site|gov))_\d{8}[^\s"\'<>()]*)',
            re.IGNORECASE,
        )
        for m in padrao.finditer(resp.text):
            token = m.group(1)
            nome = (m.group(2) or m.group(3) or "").lower()
            if nome in ("pmc", "site") and not links_encontrados["PMC"]:
                links_encontrados["PMC"] = absolutizar(token)
            elif nome in ("pmvg", "gov") and not links_encontrados["PF"]:
                links_encontrados["PF"] = absolutizar(token)

    for tipo, link in links_encontrados.items():
        if link:
            logging.info(f"Link {tipo} encontrado: {link}")
        else:
            logging.warning(f"Link {tipo} NÃO encontrado na página.")

    # Diagnóstico quando algo falta.
    if not links_encontrados["PMC"] or not links_encontrados["PF"]:
        total_links = len(soup.find_all("a", href=True))
        tem_conformidade = any(k in resp.text.lower() for k in ("conformidade", "lista_pmc", "lista_pmvg"))
        logging.error(
            f"Diagnóstico: {total_links} links na página | "
            f"'conformidade' presente no HTML: {tem_conformidade} | "
            f"tamanho do HTML: {len(resp.text)} bytes. "
            "Se 'conformidade' for False, provavelmente é página de bloqueio/anti-bot."
        )
        # Amostra dos links que mencionam xls/pmc/pmvg/conformidade — mostra o
        # formato REAL de href recebido, para ajustar a regra se necessário.
        amostras = []
        for a in soup.find_all("a", href=True):
            t = texto_link(a)[:40]
            h = a["href"]
            if any(k in (t + " " + h.lower()) for k in ("xls", "pmc", "pmvg", "conformidade")):
                amostras.append(f"  texto={t!r} href={h[:160]!r}")
            if len(amostras) >= 15:
                break
        if amostras:
            logging.error("Amostra de links candidatos:\n" + "\n".join(amostras))
        try:
            dir_log = Path(config.get("diretorio_log", "logs"))
            dir_log.mkdir(parents=True, exist_ok=True)
            debug_path = dir_log / "cmed_pagina_debug.html"
            debug_path.write_text(resp.text, encoding="utf-8")
            logging.error(f"HTML da resposta salvo em: {debug_path}")
        except OSError as e:
            logging.warning(f"Não foi possível salvar HTML de depuração: {e}")

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
    sessao = config.get("_sessao") or criar_sessao_http(config)
    headers = {"Referer": config["url_cmed"]}

    competencia_forcada = competencia

    def _competencia_de(texto: Optional[str]) -> Optional[str]:
        if not texto:
            return None
        m = re.search(r"(20\d{2})(0[1-9]|1[0-2])\d{2}", texto)  # AAAAMMDD
        if m:
            return f"{m.group(1)}-{m.group(2)}"
        m = re.search(r"(20\d{2})-(0[1-9]|1[0-2])", texto)      # AAAA-MM
        if m:
            return f"{m.group(1)}-{m.group(2)}"
        return None

    for tentativa in range(1, tentativas + 1):
        try:
            logging.info(
                f"Download {tipo} — tentativa {tentativa}/{tentativas}: {url}"
            )
            resp = sessao.get(url, headers=headers, timeout=(20, timeout), stream=True)
            resp.raise_for_status()

            # A URL de origem pode ser "resolveuid/<uuid>" (sem data). Descobrimos
            # o nome/competência reais pelo Content-Disposition ou pela URL final
            # após os redirecionamentos que o Plone faz.
            content_disp = resp.headers.get("Content-Disposition", "")
            nome_cd = None
            m_cd = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^\";]+)"?', content_disp, re.I)
            if m_cd:
                nome_cd = m_cd.group(1)

            competencia = (
                competencia_forcada
                or _competencia_de(nome_cd)
                or _competencia_de(str(resp.url))
                or _competencia_de(url)
                or datetime.now().strftime("%Y-%m")
            )

            # Nome do arquivo local: prioriza Content-Disposition, depois URL final.
            nome_arquivo = nome_cd or str(resp.url).split("/")[-1].split("?")[0]
            if not nome_arquivo.lower().endswith((".xlsx", ".xls")):
                nome_arquivo = f"cmed_{tipo.lower()}_{competencia}.xlsx"

            dir_raw = Path(config["diretorio_raw"]) / competencia
            dir_raw.mkdir(parents=True, exist_ok=True)
            caminho_local = dir_raw / nome_arquivo

            with open(caminho_local, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            tamanho = caminho_local.stat().st_size
            if tamanho < 1024:
                raise ValueError(
                    f"Arquivo muito pequeno ({tamanho} bytes) — possível erro de download."
                )

            # Validação leve: XLSX é um ZIP (assinatura "PK"); XLS antigo começa
            # com D0 CF 11 E0. Se vier HTML, é página de erro disfarçada.
            with open(caminho_local, "rb") as fh:
                assinatura = fh.read(4)
            if assinatura[:2] not in (b"PK", b"\xd0\xcf"):
                raise ValueError(
                    "Conteúdo baixado não é um Excel válido (possível página de "
                    f"erro). Primeiros bytes: {assinatura!r}"
                )

            sha = calcular_hash_arquivo(caminho_local)
            logging.info(
                f"Download {tipo} concluído: {caminho_local} "
                f"({tamanho:,} bytes, competência {competencia}, "
                f"SHA-256: {sha[:16]}...)"
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


def _sem_acento(texto: str) -> str:
    """Remove acentos para comparação robusta de cabeçalhos."""
    return "".join(
        c for c in unicodedata.normalize("NFKD", str(texto))
        if not unicodedata.combining(c)
    )


def detectar_linha_cabecalho(df: pd.DataFrame, limite_linhas: int = 200):
    """
    Detecta dinamicamente a linha de cabeçalho da CMED.

    Retorna (idx, score, candidatos). idx é None se nenhuma linha atingir o
    limiar. A comparação é INSENSÍVEL A ACENTO e aplica NOMES_ALTERNATIVOS,
    então tolera variações como 'CODIGO GGREM'/'CÓDIGO GGREM',
    'SUBSTANCIA'/'SUBSTÂNCIA', 'FORMA FARMACÊUTICA' etc. 'candidatos' traz as
    melhores linhas (para diagnóstico quando a detecção falha).
    """
    identificadores = {
        "SUBSTÂNCIA", "CNPJ", "LABORATÓRIO", "CÓDIGO GGREM", "REGISTRO",
        "EAN 1", "EAN 2", "EAN 3", "PRODUTO", "APRESENTAÇÃO",
        "CLASSE TERAPÊUTICA", "TIPO DE PRODUTO (STATUS DO PRODUTO)",
        "REGIME DE PREÇO", "RESTRIÇÃO HOSPITALAR", "CAP", "CONFAZ 87",
        "ICMS 0%", "ANÁLISE RECURSAL", "TARJA"
    }
    ident_norm = {_sem_acento(x) for x in identificadores}
    alt_norm = {
        _sem_acento(re.sub(r"\s+", " ", str(k).replace("\xa0", " ").strip()).upper()): v
        for k, v in NOMES_ALTERNATIVOS.items()
    }

    def eh_coluna_preco(nome: str) -> bool:
        n = _normalizar_texto_cabecalho(nome)
        return bool(re.match(r"^(PF|PMC|PMVG)\s+(SEM IMPOSTOS|\d+(?:,\d+)?%)$", n))

    def norm(v):
        t = _normalizar_texto_cabecalho(v)      # upper + espaços + %
        t_na = _sem_acento(t)
        if t_na in alt_norm:                    # mapeia alternativo -> canônico
            t_na = _sem_acento(alt_norm[t_na])
        return t, t_na

    melhor_idx, melhor_score = None, -1
    candidatos = []
    limite = min(len(df), limite_linhas)
    for idx in range(limite):
        pares = [norm(v) for v in df.iloc[idx].tolist()]
        orig = {t for t, _ in pares if t}
        sem_ac = {tna for _, tna in pares if tna}

        acertos_id = len(sem_ac & ident_norm)
        acertos_preco = sum(eh_coluna_preco(v) for v in orig)
        score = acertos_id * 10 + acertos_preco * 3

        if acertos_id or acertos_preco:
            candidatos.append((score, idx, acertos_id, acertos_preco, sorted(orig)[:12]))
        if acertos_id >= 8 and acertos_preco >= 3 and score > melhor_score:
            melhor_idx, melhor_score = idx, score

    candidatos.sort(reverse=True)
    return melhor_idx, melhor_score, candidatos[:3]


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


def _selecionar_engine(caminho: Path) -> str:
    """Escolhe o engine do pandas conforme a extensão do arquivo."""
    sufixo = caminho.suffix.lower()
    if sufixo == ".xls":
        return "xlrd"       # formato antigo (BIFF)
    return "openpyxl"       # .xlsx / .xlsm


def ler_arquivo_cmed(caminho: Path) -> pd.DataFrame:
    """Lê arquivo CMED, encontra o cabeçalho dinamicamente e valida a estrutura.

    Varre TODAS as abas (a CMED pode adicionar uma aba de instruções antes da
    tabela) e escolhe a aba/linha com melhor pontuação de cabeçalho. Se nada
    for encontrado, registra no log os melhores candidatos de cada aba — isso
    revela o cabeçalho real quando o layout da ANVISA muda.
    """
    logging.info(f"Lendo arquivo: {caminho}")
    engine = _selecionar_engine(caminho)
    xls = pd.ExcelFile(caminho, engine=engine)

    melhor = None  # (score, aba, idx_header)
    diagnostico = {}
    for aba in xls.sheet_names:
        df_raw = xls.parse(sheet_name=aba, header=None, dtype=str)
        idx, score, candidatos = detectar_linha_cabecalho(df_raw)
        diagnostico[aba] = candidatos
        if idx is not None and (melhor is None or score > melhor[0]):
            melhor = (score, aba, idx)

    if melhor is None:
        linhas = ["Não foi possível identificar o cabeçalho. Melhores candidatos por aba:"]
        for aba, cands in diagnostico.items():
            linhas.append(f"  [aba {aba!r}]")
            if not cands:
                linhas.append("    (nenhuma linha com colunas reconhecíveis)")
            for score, idx, nid, npreco, amostra in cands:
                linhas.append(
                    f"    linha {idx + 1}: ids={nid} precos={npreco} amostra={amostra}"
                )
        logging.error("\n".join(linhas))
        raise ValueError(
            "Não foi possível identificar automaticamente o cabeçalho da CMED. "
            "O arquivo não será processado para evitar gerar dados incorretos."
        )

    _, aba, idx_header = melhor
    logging.info(
        f"Cabeçalho CMED detectado na aba {aba!r}, linha Excel {idx_header + 1} "
        f"(índice pandas {idx_header}; score={melhor[0]})."
    )

    df = xls.parse(sheet_name=aba, header=idx_header, dtype=str)
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

    # Colunas de atributo (todas menos as de EAN) presentes no arquivo
    attr_map = {k: v for k, v in cols_dim.items()
                if k != "EAN 1" and k in df_raw.columns}

    # Empilhar EAN 1/2/3 numa única coluna EAN. O fato explode os três
    # códigos de barras; se a dimensão usar só "EAN 1", ~4,5% das linhas do
    # fato ficam sem produto correspondente. Empilhando, todo EAN do fato
    # tem match na dimensão.
    frames = []
    for col_ean in ("EAN 1", "EAN 2", "EAN 3"):
        if col_ean in df_raw.columns:
            tmp = df_raw[[col_ean] + list(attr_map.keys())].copy()
            tmp.rename(columns={col_ean: "EAN", **attr_map}, inplace=True)
            frames.append(tmp)
    df_dim = pd.concat(frames, ignore_index=True) if frames else df_raw.iloc[0:0].copy()

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

    df_est = pd.DataFrame(registros)
    _t = df_est["ALIQUOTA_ICMS_VIGENTE"].astype(str)
    df_est["ALIQUOTA_ICMS_VIGENTE_PCT"] = pd.to_numeric(
        _t.str.replace("%", "", regex=False)
          .str.replace(",", ".", regex=False)
          .where(~_t.str.lower().str.startswith("sem")),
        errors="coerce",
    ) / 100
    return df_est


def gerar_dimensao_calendario(
    data_inicio: str = "2024-01-01",
    data_fim: str = "2027-12-31",
) -> pd.DataFrame:
    """Gera tabela dimensão dCalendario."""
    meses_pt = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
    }
    datas = pd.date_range(start=data_inicio, end=data_fim, freq="D")
    df = pd.DataFrame({"DATA": datas})
    df["ANO"] = df["DATA"].dt.year
    df["MES"] = df["DATA"].dt.month
    # Não usar strftime("%B"): depende do locale do SO (sai em inglês no CI).
    df["NOME_MES"] = df["MES"].map(meses_pt)
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
    Exporta dados prontos para o Power BI.

    A tabela FATO é gravada em Parquet PARTICIONADO POR COMPETÊNCIA
    (um arquivo por mês em dados/processed/fato/). Motivos:
      • Parquet comprime muito dados repetitivos (377 MB de CSV ≈ 20-40 MB),
        ficando bem abaixo do limite de 100 MB por arquivo do GitHub.
      • Particionar por mês evita um arquivo único que cresce sem parar.
      • O Power BI lê uma pasta de Parquet nativamente ("Pasta" → Combinar).
    As DIMENSÕES continuam em CSV (são pequenas) e são acumuladas/dedupadas.
    """
    dir_saida = Path(diretorio)
    dir_saida.mkdir(parents=True, exist_ok=True)
    encoding = "utf-8-sig"  # compatível com Excel e Power BI
    arquivos = {}

    # ── FATO: Parquet, um arquivo por competência ──────────────────
    dir_fato = dir_saida / "fato"
    dir_fato.mkdir(parents=True, exist_ok=True)

    if "COMPETENCIA" in df_fato.columns and len(df_fato) > 0:
        competencias = sorted(df_fato["COMPETENCIA"].dropna().unique())
    else:
        competencias = []

    for comp in competencias:
        parte = df_fato[df_fato["COMPETENCIA"] == comp].copy()

        # Tipos corretos para o Power BI. Sem isto, o Parquet grava datas e
        # alíquota como Texto e o Power BI respeita o tipo embutido, quebrando
        # eixo de tempo e cálculos.
        parte["DATA_REFERENCIA"] = pd.to_datetime(parte["DATA_REFERENCIA"]).dt.date
        parte["DATA_CARGA"] = pd.to_datetime(parte["DATA_CARGA"])
        parte["COMPETENCIA_DATA"] = pd.to_datetime(
            parte["COMPETENCIA"].astype(str) + "-01"
        ).dt.date
        _aliq = parte["ALIQUOTA_ICMS"].astype(str)
        parte["ALIQUOTA_ICMS_PCT"] = pd.to_numeric(
            _aliq.str.replace("%", "", regex=False)
                 .str.replace(",", ".", regex=False)
                 .where(~_aliq.str.lower().str.startswith("sem")),
            errors="coerce",
        ) / 100

        caminho_parte = dir_fato / f"fato_precos_{comp}.parquet"
        # Sobrescreve o mês (idempotente entre as execuções do mesmo mês).
        parte.to_parquet(caminho_parte, index=False, engine="pyarrow", compression="snappy")
        arquivos[f"fato_precos_{comp}"] = caminho_parte

    if not competencias:
        logging.warning("Tabela fato vazia — nenhum Parquet gerado.")

    # ── Dimensões (CSV, acumuladas/dedupadas) ──────────────────────
    caminho_med = dir_saida / "dim_medicamento.csv"
    if caminho_med.exists():
        df_med_existente = pd.read_csv(caminho_med, dtype=str, encoding=encoding)
        df_medicamento = pd.concat([df_med_existente, df_medicamento], ignore_index=True)
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
        aviso = "  ⚠ ACIMA DE 100MB!" if tam > 100 * 1024 * 1024 else ""
        logging.info(f"Exportado: {caminho} ({tam:,} bytes){aviso}")

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

    # Sessão HTTP única e resiliente, reaproveitada em todos os downloads.
    config["_sessao"] = criar_sessao_http(config)
    estado = carregar_estado(config)

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

        competencia = extrair_competencia_do_arquivo(arquivo_pmc)

        # 8. Registrar estado (última execução bem-sucedida).
        estado.update({
            "ultima_competencia": competencia,
            "ultima_execucao": datetime.now().isoformat(),
            "registros_fato": int(len(df_fato)),
        })
        salvar_estado(config, estado)

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
