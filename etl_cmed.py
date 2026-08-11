
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL CMED — Extração, Transformação e Carga de dados ANVISA/CMED para Power BI.

Arquitetura preparada para GitHub Actions:

- Descobre automaticamente os arquivos PMC e PMVG publicados pela CMED.
- Usa a página oficial de arquivos da CMED.
- Não depende de nomes fixos dos arquivos.
- Baixa os arquivos e calcula SHA-256.
- Mantém o estado de processamento separado dos dados de negócio.
- Usa competência + hashes para detectar mudanças.
- Não depende de fato_precos.csv para decidir se deve processar.
- Mantém histórico dos dados em CSV.
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
from urllib.parse import urljoin, urlparse

import numpy as np
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup


# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

CONFIG_PADRAO = {
    "url_cmed": (
        "https://www.gov.br/anvisa/pt-br/assuntos/"
        "medicamentos/cmed/precos/arquivos"
    ),
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


# ============================================================================
# ICMS
# ============================================================================

MAPEAMENTO_ICMS_UF = {
    "0%": [
        "AC", "AM", "AP", "PA", "RO", "RR", "TO",
        "MT", "MS", "GO", "DF"
    ],
    "12%": ["ES", "RS"],
    "17%": [
        "AL", "BA", "CE", "MA", "PB", "PE", "PI",
        "RN", "SE", "PR", "SC", "SP"
    ],
    "17,5%": ["RJ"],
    "18%": ["MG"],
    "19,5%": [],
    "20%": [],
    "20,5%": [],
    "21%": [],
    "22%": [],
}


# ============================================================================
# COLUNAS
# ============================================================================

COLUNAS_IDENTIFICACAO = [
    "SUBSTÂNCIA",
    "CNPJ",
    "LABORATÓRIO",
    "CÓDIGO GGREM",
    "REGISTRO",
    "EAN 1",
    "EAN 2",
    "EAN 3",
    "PRODUTO",
    "APRESENTAÇÃO",
    "F.FARMACÊUTICA",
    "CLASSE TERAPÊUTICA",
    "TIPO DE PRODUTO (STATUS DO PRODUTO)",
    "REGIME DE PREÇO",
    "TARJA",
    "RESTRIÇÃO HOSPITALAR",
    "CAP",
    "CONFAZ 87",
    "ICMS 0%",
    "ANÁLISE RECURSAL",
]


NOMES_ALTERNATIVOS = {
    "TIPO DE PRODUTO (REVOGADO/NOVO/IDÊNTICO)":
        "TIPO DE PRODUTO (STATUS DO PRODUTO)",
    "TIPO DE PRODUTO":
        "TIPO DE PRODUTO (STATUS DO PRODUTO)",
    "ANALISE RECURSAL":
        "ANÁLISE RECURSAL",
    "RESTRICAO HOSPITALAR":
        "RESTRIÇÃO HOSPITALAR",
    "RESTRIÇÃO HOSP.":
        "RESTRIÇÃO HOSPITALAR",
    "CLASSE TERAPEUTICA":
        "CLASSE TERAPÊUTICA",
    "FORMA FARMACEUTICA":
        "F.FARMACÊUTICA",
    "FORMA FARMACÊUTICA":
        "F.FARMACÊUTICA",
    "F. FARMACÊUTICA":
        "F.FARMACÊUTICA",
    "APRESENTACAO":
        "APRESENTAÇÃO",
    "CODIGO GGREM":
        "CÓDIGO GGREM",
    "SUBSTANCIA":
        "SUBSTÂNCIA",
    "LABORATORIO":
        "LABORATÓRIO",
}


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


# ============================================================================
# CONFIGURAÇÃO E LOG
# ============================================================================

def carregar_config(caminho_config: Optional[str] = None) -> dict:
    config = CONFIG_PADRAO.copy()

    if caminho_config and Path(caminho_config).exists():
        with open(caminho_config, "r", encoding="utf-8") as f:
            custom = yaml.safe_load(f) or {}

        config.update(custom)

        if "email_alerta" in custom:
            config["email_alerta"] = {
                **CONFIG_PADRAO["email_alerta"],
                **custom["email_alerta"],
            }

        logging.info(
            "Configuração carregada de: %s",
            caminho_config,
        )

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
        caminho_log,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
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


def enviar_alerta_email(
    config: dict,
    assunto: str,
    corpo: str,
) -> None:
    cfg = config.get("email_alerta", {})

    if not cfg.get("ativo"):
        return

    try:
        msg = MIMEText(corpo, "plain", "utf-8")
        msg["Subject"] = f"[CMED ETL] {assunto}"
        msg["From"] = cfg["remetente"]
        msg["To"] = ", ".join(cfg["destinatarios"])

        with smtplib.SMTP(
            cfg["smtp_host"],
            cfg["smtp_porta"],
        ) as server:
            server.starttls()
            server.login(
                cfg["remetente"],
                cfg["senha"],
            )
            server.send_message(msg)

        logging.info("Alerta enviado por e-mail.")

    except Exception as exc:
        logging.error(
            "Falha ao enviar e-mail de alerta: %s",
            exc,
        )


# ============================================================================
# HASH
# ============================================================================

def calcular_hash_arquivo(caminho: Path) -> str:
    sha = hashlib.sha256()

    with open(caminho, "rb") as f:
        for bloco in iter(
            lambda: f.read(8192),
            b"",
        ):
            sha.update(bloco)

    return sha.hexdigest()


# ============================================================================
# ESTADO
# ============================================================================

def carregar_estado(caminho: str) -> dict:
    path = Path(caminho)

    if not path.exists():
        logging.info(
            "Estado CMED ainda não existe: %s",
            path,
        )
        return {}

    try:
        with open(
            path,
            "r",
            encoding="utf-8",
        ) as f:
            estado = json.load(f)

        logging.info(
            "Estado carregado: competência=%s",
            estado.get("competencia"),
        )

        return estado

    except (
        OSError,
        json.JSONDecodeError,
    ) as exc:
        logging.warning(
            "Estado inválido ou ilegível: %s",
            exc,
        )
        return {}


def salvar_estado(
    caminho: str,
    estado: dict,
) -> None:
    path = Path(caminho)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporario = path.with_suffix(
        path.suffix + ".tmp"
    )

    with open(
        temporario,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            estado,
            f,
            ensure_ascii=False,
            indent=2,
        )
        f.write("\n")

    temporario.replace(path)

    logging.info(
        "Estado atualizado: %s",
        path,
    )


def estado_ja_processado(
    estado: dict,
    competencia: str,
    hashes: dict,
) -> bool:

    if not estado:
        return False

    mesma_competencia = (
        estado.get("competencia") == competencia
    )

    hashes_atuais = {
        "PMC": hashes.get("PMC"),
        "PMVG": hashes.get("PMVG"),
    }

    hashes_salvos = {
        "PMC": estado.get("hashes", {}).get("PMC"),
        "PMVG": estado.get("hashes", {}).get("PMVG"),
    }

    if (
        mesma_competencia
        and hashes_atuais == hashes_salvos
    ):
        logging.info(
            "Competência %s já processada com os mesmos "
            "arquivos. Nenhum processamento necessário.",
            competencia,
        )
        return True

    if mesma_competencia:
        logging.info(
            "Competência %s já existe, mas o hash dos "
            "arquivos mudou. Novo processamento será realizado.",
            competencia,
        )

    return False


# ============================================================================
# EXTRAÇÃO — DESCOBERTA DOS LINKS CMED
# ============================================================================

def obter_links_cmed(config: dict) -> dict:
    """
    Descobre automaticamente os links atuais da CMED.

    A página oficial atualmente apresenta os arquivos como:

        PMC - xls
        PMVG - xls

    O retorno mantém as chaves:

        {
            "PMC": "...",
            "PMVG": "..."
        }

    Compatibilidade:
    - links absolutos;
    - links relativos;
    - texto do link;
    - URL do link;
    - pequenas alterações de nomenclatura;
    - páginas antigas da CMED;
    - parâmetros na URL;
    - links sem extensão explícita no href.
    """

    url = config["url_cmed"]

    headers = {
        "User-Agent": config["user_agent"],
        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    logging.info(
        "Acessando página oficial de arquivos CMED: %s",
        url,
    )

    resp = requests.get(
        url,
        headers=headers,
        timeout=config["timeout_segundos"],
        verify=True,
    )

    resp.raise_for_status()

    logging.info(
        "Página CMED acessada com sucesso — "
        "HTTP %s (%s bytes).",
        resp.status_code,
        f"{len(resp.content):,}",
    )

    soup = BeautifulSoup(
        resp.text,
        "html.parser",
    )

    candidatos = []

    for a in soup.find_all("a", href=True):

        href_original = str(
            a.get("href", "")
        ).strip()

        texto = a.get_text(
            " ",
            strip=True,
        )

        title = str(
            a.get("title", "")
        ).strip()

        aria_label = str(
            a.get("aria-label", "")
        ).strip()

        if not href_original:
            continue

        href = urljoin(
            url,
            href_original,
        )

        texto_completo = " ".join(
            [
                texto,
                title,
                aria_label,
                href,
            ]
        )

        texto_lower = texto_completo.lower()

        # Só consideramos candidatos relacionados
        # a arquivos/planilhas CMED.
        palavras_excel = (
            ".xls",
            ".xlsx",
            "xls",
            "planilha",
            "arquivo",
        )

        palavras_cmed = (
            "pmc",
            "pmvg",
            "preço",
            "precos",
            "preços",
            "medicamento",
        )

        parece_excel = any(
            palavra in texto_lower
            for palavra in palavras_excel
        )

        parece_cmed = any(
            palavra in texto_lower
            for palavra in palavras_cmed
        )

        if not (parece_excel and parece_cmed):
            continue

        candidatos.append(
            {
                "texto": texto,
                "title": title,
                "aria_label": aria_label,
                "href": href,
                "texto_lower": texto_lower,
            }
        )

    logging.info(
        "Candidatos de arquivos CMED encontrados: %s",
        len(candidatos),
    )

    for candidato in candidatos:
        logging.debug(
            "Candidato CMED: texto='%s' | title='%s' | url='%s'",
            candidato["texto"],
            candidato["title"],
            candidato["href"],
        )

    links = {
        "PMC": None,
        "PMVG": None,
    }

    # ------------------------------------------------------------------
    # PMC — prioridade por texto
    # ------------------------------------------------------------------

    regras_pmc = [
        lambda x: (
            "pmc" in x["texto_lower"]
            and "xls" in x["texto_lower"]
        ),
        lambda x: (
            "pmc" in x["texto_lower"]
            and "planilha" in x["texto_lower"]
        ),
        lambda x: (
            "pmc" in x["href"].lower()
        ),
    ]

    for regra in regras_pmc:
        for candidato in candidatos:
            if regra(candidato):
                links["PMC"] = candidato["href"]

                logging.info(
                    "Link PMC encontrado: %s",
                    links["PMC"],
                )
                break

        if links["PMC"]:
            break

    # ------------------------------------------------------------------
    # PMVG — prioridade por texto
    # ------------------------------------------------------------------

    regras_pmvg = [
        lambda x: (
            "pmvg" in x["texto_lower"]
            and "xls" in x["texto_lower"]
        ),
        lambda x: (
            "pmvg" in x["texto_lower"]
            and "planilha" in x["texto_lower"]
        ),
        lambda x: (
            "pmvg" in x["href"].lower()
        ),
    ]

    for regra in regras_pmvg:
        for candidato in candidatos:
            if regra(candidato):
                links["PMVG"] = candidato["href"]

                logging.info(
                    "Link PMVG encontrado: %s",
                    links["PMVG"],
                )
                break

        if links["PMVG"]:
            break

    # ------------------------------------------------------------------
    # Compatibilidade com estrutura antiga
    # ------------------------------------------------------------------

    if not links["PMC"]:
        for candidato in candidatos:
            href = candidato["href"].lower()

            if (
                "xls_conformidade_site" in href
                or "conformidade" in href
            ):
                links["PMC"] = candidato["href"]

                logging.info(
                    "Link PMC encontrado pelo padrão "
                    "de compatibilidade antiga: %s",
                    links["PMC"],
                )
                break

    if not links["PMVG"]:
        for candidato in candidatos:
            href = candidato["href"].lower()

            if (
                "xls_conformidade_gov" in href
                or "conformidade_gov" in href
            ):
                links["PMVG"] = candidato["href"]

                logging.info(
                    "Link PMVG encontrado pelo padrão "
                    "de compatibilidade antiga: %s",
                    links["PMVG"],
                )
                break

    # ------------------------------------------------------------------
    # Validação final
    # ------------------------------------------------------------------

    if not links["PMC"]:
        logging.error(
            "Não foi possível localizar o arquivo PMC "
            "na página oficial da CMED."
        )

    if not links["PMVG"]:
        logging.error(
            "Não foi possível localizar o arquivo PMVG "
            "na página oficial da CMED."
        )

    if not links["PMC"] or not links["PMVG"]:
        logging.error(
            "Links encontrados: %s",
            links,
        )

        raise RuntimeError(
            "A página CMED foi acessada, mas não foi possível "
            "identificar simultaneamente os arquivos PMC e PMVG."
        )

    logging.info(
        "Arquivos CMED identificados com sucesso."
    )

    return links


# ============================================================================
# DOWNLOAD
# ============================================================================

def obter_extensao_url(url: str) -> str:
    caminho = urlparse(url).path.lower()

    if caminho.endswith(".xlsx"):
        return ".xlsx"

    if caminho.endswith(".xls"):
        return ".xls"

    return ".xlsx"


def baixar_arquivo_cmed(
    url: str,
    tipo: str,
    config: dict,
    competencia: Optional[str] = None,
) -> Path:

    tentativas = config["tentativas_max"]
    intervalo = config["intervalo_retry_segundos"]

    headers = {
        "User-Agent": config["user_agent"],
        "Accept": (
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet,"
            "application/vnd.ms-excel,"
            "*/*"
        ),
    }

    if not competencia:
        match = re.search(
            r"(20\d{2})[-_]?([01]\d)",
            url,
        )

        if match:
            competencia = (
                f"{match.group(1)}-{match.group(2)}"
            )

    competencia = (
        competencia
        or datetime.now().strftime("%Y-%m")
    )

    dir_raw = (
        Path(config["diretorio_raw"])
        / competencia
    )

    dir_raw.mkdir(
        parents=True,
        exist_ok=True,
    )

    nome_url = Path(
        urlparse(url).path
    ).name

    extensao = obter_extensao_url(url)

    if nome_url and "." in nome_url:
        nome_arquivo = nome_url
    else:
        nome_arquivo = (
            f"cmed_{tipo.lower()}_"
            f"{competencia}{extensao}"
        )

    caminho_local = (
        dir_raw / nome_arquivo
    )

    for tentativa in range(
        1,
        tentativas + 1,
    ):

        try:
            logging.info(
                "Download %s — tentativa %s/%s: %s",
                tipo,
                tentativa,
                tentativas,
                url,
            )

            resp = requests.get(
                url,
                headers=headers,
                timeout=config["timeout_segundos"],
                stream=True,
                allow_redirects=True,
            )

            resp.raise_for_status()

            with open(
                caminho_local,
                "wb",
            ) as f:

                for chunk in resp.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        f.write(chunk)

            tamanho = caminho_local.stat().st_size

            if tamanho < 1024:
                raise ValueError(
                    f"Arquivo muito pequeno "
                    f"({tamanho} bytes)."
                )

            sha = calcular_hash_arquivo(
                caminho_local
            )

            logging.info(
                "Download %s concluído: %s "
                "(%s bytes, SHA-256: %s...)",
                tipo,
                caminho_local,
                f"{tamanho:,}",
                sha[:16],
            )

            return caminho_local

        except (
            requests.RequestException,
            ValueError,
            OSError,
        ) as exc:

            logging.error(
                "Erro no download %s "
                "(tentativa %s): %s",
                tipo,
                tentativa,
                exc,
            )

            if tentativa < tentativas:
                time.sleep(intervalo)

            else:
                msg = (
                    f"Falha permanente no download "
                    f"{tipo} após {tentativas} tentativas: "
                    f"{exc}"
                )

                logging.critical(msg)

                enviar_alerta_email(
                    config,
                    f"Falha download {tipo}",
                    msg,
                )

                raise RuntimeError(
                    msg
                ) from exc

    raise RuntimeError(
        "Fluxo inesperado no download."
    )


# ============================================================================
# TRANSFORMAÇÃO
# ============================================================================

def validar_ean13(ean: str) -> bool:

    if not ean or not isinstance(ean, str):
        return False

    ean = re.sub(
        r"\D",
        "",
        ean,
    )

    if len(ean) != 13:
        return False

    try:
        soma = sum(
            int(digito)
            * (1 if i % 2 == 0 else 3)
            for i, digito in enumerate(ean[:12])
        )

        verificador = (
            10 - (soma % 10)
        ) % 10

        return (
            verificador
            == int(ean[12])
        )

    except (
        ValueError,
        IndexError,
    ):
        return False


def tratar_ean(valor) -> Optional[str]:

    if valor is None:
        return None

    if isinstance(valor, float) and np.isnan(valor):
        return None

    texto = str(valor).strip()

    if texto.lower() in (
        "nan",
        "none",
        "",
    ):
        return None

    numeros = re.sub(
        r"\D",
        "",
        texto,
    )

    if not numeros or numeros == "0":
        return None

    return numeros.zfill(13)


def tratar_valor_preco(valor) -> tuple:

    if valor is None:
        return None, False

    if (
        isinstance(valor, float)
        and np.isnan(valor)
    ):
        return None, False

    texto = str(valor).strip()

    if texto.lower() in (
        "nan",
        "none",
        "-",
        "",
    ):
        return None, False

    flag_asterisco = "*" in texto

    texto = texto.replace(
        "*",
        "",
    ).strip()

    if not texto:
        return None, flag_asterisco

    if "," in texto and "." in texto:
        texto = (
            texto
            .replace(".", "")
            .replace(",", ".")
        )

    elif "," in texto:
        texto = texto.replace(
            ",",
            ".",
        )

    try:
        return (
            float(texto),
            flag_asterisco,
        )

    except ValueError:
        logging.warning(
            "Valor de preço não numérico ignorado: '%s'",
            valor,
        )

        return (
            None,
            flag_asterisco,
        )


def _normalizar_texto_cabecalho(
    valor,
) -> str:

    if valor is None:
        return ""

    if (
        isinstance(valor, float)
        and pd.isna(valor)
    ):
        return ""

    texto = str(valor)
    texto = texto.replace(
        "\xa0",
        " ",
    ).strip()

    texto = re.sub(
        r"\s+",
        " ",
        texto,
    )

    texto = re.sub(
        r"\s+%",
        "%",
        texto,
    )

    return texto.upper()


def detectar_linha_cabecalho(
    df: pd.DataFrame,
) -> int:

    identificadores = {
        _normalizar_texto_cabecalho(x)
        for x in COLUNAS_IDENTIFICACAO
    }

    def eh_coluna_preco(nome: str) -> bool:
        n = _normalizar_texto_cabecalho(nome)

        return bool(
            re.match(
                r"^(PF|PMC|PMVG)\s+"
                r"(SEM IMPOSTOS|\d+(?:,\d+)?%)$",
                n,
            )
        )

    melhor_idx = None
    melhor_score = -1

    limite = min(
        len(df),
        250,
    )

    for idx in range(limite):

        valores = {
            _normalizar_texto_cabecalho(v)
            for v in df.iloc[idx].tolist()
        }

        valores.discard("")

        acertos_id = len(
            valores & identificadores
        )

        acertos_preco = sum(
            eh_coluna_preco(v)
            for v in valores
        )

        score = (
            acertos_id * 10
            + acertos_preco * 3
        )

        if (
            acertos_id >= 8
            and acertos_preco >= 3
            and score > melhor_score
        ):
            melhor_idx = idx
            melhor_score = score

    if melhor_idx is None:
        raise ValueError(
            "Não foi possível identificar "
            "automaticamente o cabeçalho da CMED."
        )

    logging.info(
        "Cabeçalho CMED detectado na linha Excel %s "
        "(índice pandas %s; score=%s).",
        melhor_idx + 1,
        melhor_idx,
        melhor_score,
    )

    return melhor_idx


def normalizar_nomes_colunas(
    colunas: list,
) -> list:

    resultado = []

    alternativas = {
        re.sub(
            r"\s+",
            " ",
            str(k)
            .replace("\xa0", " ")
            .strip(),
        ).upper(): v
        for k, v in NOMES_ALTERNATIVOS.items()
    }

    for col in colunas:

        col_limpo = (
            str(col)
            .replace("\xa0", " ")
            .strip()
        )

        col_limpo = re.sub(
            r"\s+",
            " ",
            col_limpo,
        )

        col_limpo = re.sub(
            r"\s+%",
            "%",
            col_limpo,
        )

        col_upper = col_limpo.upper()

        if col_upper in alternativas:
            col_limpo = alternativas[
                col_upper
            ]

        resultado.append(
            col_limpo
        )

    return resultado


# ============================================================================
# LEITURA
# ============================================================================

def ler_arquivo_cmed(
    caminho: Path,
) -> pd.DataFrame:

    logging.info(
        "Lendo arquivo: %s",
        caminho,
    )

    suffix = caminho.suffix.lower()

    # Arquivos XLSX
    if suffix == ".xlsx":

        df_raw = pd.read_excel(
            caminho,
            header=None,
            dtype=str,
            engine="openpyxl",
        )

    # Arquivos XLS antigos.
    # Caso realmente apareça XLS binário, o ambiente
    # precisa ter xlrd instalado.
    elif suffix == ".xls":

        try:
            df_raw = pd.read_excel(
                caminho,
                header=None,
                dtype=str,
                engine="xlrd",
            )

        except ImportError as exc:
            raise RuntimeError(
                "O arquivo CMED foi publicado em formato .xls "
                "e o pacote 'xlrd' não está instalado. "
                "Adicione 'xlrd' ao requirements.txt."
            ) from exc

    else:
        raise ValueError(
            f"Formato de arquivo não suportado: {suffix}"
        )

    idx_header = detectar_linha_cabecalho(
        df_raw
    )

    if suffix == ".xlsx":

        df = pd.read_excel(
            caminho,
            header=idx_header,
            dtype=str,
            engine="openpyxl",
        )

    else:

        df = pd.read_excel(
            caminho,
            header=idx_header,
            dtype=str,
            engine="xlrd",
        )

    df.columns = normalizar_nomes_colunas(
        list(df.columns)
    )

    df.dropna(
        how="all",
        inplace=True,
    )

    obrigatorias = {
        "SUBSTÂNCIA",
        "CNPJ",
        "LABORATÓRIO",
        "CÓDIGO GGREM",
        "EAN 1",
        "PRODUTO",
        "APRESENTAÇÃO",
    }

    ausentes = sorted(
        obrigatorias - set(df.columns)
    )

    if ausentes:
        raise ValueError(
            f"Colunas obrigatórias ausentes: {ausentes}"
        )

    logging.info(
        "Arquivo lido: %s linhas × %s colunas.",
        len(df),
        len(df.columns),
    )

    return df


# ============================================================================
# COMPETÊNCIA
# ============================================================================

def extrair_competencia_do_arquivo(
    caminho: Path,
) -> str:

    nome = caminho.name

    padroes = [
        r"(20\d{2})[-_](0[1-9]|1[0-2])",
        r"(20\d{2})(0[1-9]|1[0-2])\d{2}",
        r"(0[1-9]|1[0-2])[-_](20\d{2})",
    ]

    for i, padrao in enumerate(padroes):

        match = re.search(
            padrao,
            nome,
        )

        if not match:
            continue

        if i == 2:
            return (
                f"{match.group(2)}-"
                f"{match.group(1)}"
            )

        return (
            f"{match.group(1)}-"
            f"{match.group(2)}"
        )

    logging.warning(
        "Não foi possível extrair competência "
        "do nome '%s'. Usando mês atual.",
        nome,
    )

    return datetime.now().strftime(
        "%Y-%m"
    )


# ============================================================================
# COLUNAS DE PREÇO
# ============================================================================

def detectar_colunas_preco(
    df: pd.DataFrame,
    tipo_preco: str,
) -> list:

    encontradas = []

    tipo_preco = tipo_preco.upper()

    for col in df.columns:

        n = _normalizar_texto_cabecalho(
            col
        )

        if not n.startswith(
            tipo_preco
        ):
            continue

        if " ALC" in n:
            continue

        if re.match(
            r"^(PF|PMC|PMVG)\s+"
            r"(SEM IMPOSTOS|\d+(?:,\d+)?%)$",
            n,
        ):
            encontradas.append(col)

    return encontradas


# ============================================================================
# PROCESSAMENTO
# ============================================================================

def processar_tabela_precos(
    df: pd.DataFrame,
    caminho: Path,
    tipo_preco: str,
    competencia: Optional[str] = None,
) -> pd.DataFrame:

    competencia = (
        competencia
        or extrair_competencia_do_arquivo(
            caminho
        )
    )

    data_carga = datetime.now()

    logging.info(
        "Processando %s — competência: %s",
        tipo_preco,
        competencia,
    )

    for col_ean in (
        "EAN 1",
        "EAN 2",
        "EAN 3",
    ):
        if col_ean in df.columns:
            df[col_ean] = df[
                col_ean
            ].apply(tratar_ean)

    colunas_preco = detectar_colunas_preco(
        df,
        tipo_preco,
    )

    if not colunas_preco:
        logging.error(
            "Nenhuma coluna de preço %s encontrada.",
            tipo_preco,
        )

        return pd.DataFrame()

    logging.info(
        "Colunas de preço %s encontradas: %s",
        tipo_preco,
        colunas_preco,
    )

    for col in colunas_preco:

        resultados = df[col].apply(
            tratar_valor_preco
        )

        df[col] = resultados.apply(
            lambda x: x[0]
        )

        df[
            f"_FLAG_{col}"
        ] = resultados.apply(
            lambda x: x[1]
        )

    cols_ean = [
        c
        for c in (
            "EAN 1",
            "EAN 2",
            "EAN 3",
        )
        if c in df.columns
    ]

    registros = []

    for _, row in df.iterrows():

        eans_validos = [
            row.get(c)
            for c in cols_ean
            if row.get(c)
            and pd.notna(row.get(c))
        ]

        if not eans_validos:
            eans_validos = [None]

        for ean in eans_validos:

            for col_preco in colunas_preco:

                valor = row[col_preco]

                if valor is None:
                    continue

                if (
                    isinstance(valor, float)
                    and np.isnan(valor)
                ):
                    continue

                match = re.search(
                    r"(Sem Impostos|"
                    r"\d+(?:,\d+)?%)",
                    str(col_preco),
                    re.IGNORECASE,
                )

                aliquota = (
                    match.group(1)
                    if match
                    else str(col_preco)
                )

                aliquota = aliquota.strip()

                ufs = MAPEAMENTO_ICMS_UF.get(
                    aliquota,
                    [],
                )

                if not ufs:
                    ufs = [None]

                for uf in ufs:

                    registros.append(
                        {
                            "EAN": ean,
                            "CODIGO_GGREM": row.get(
                                "CÓDIGO GGREM"
                            ),
                            "PRODUTO": row.get(
                                "PRODUTO"
                            ),
                            "APRESENTACAO": row.get(
                                "APRESENTAÇÃO"
                            ),
                            "LABORATORIO": row.get(
                                "LABORATÓRIO"
                            ),
                            "SUBSTANCIA": row.get(
                                "SUBSTÂNCIA"
                            ),
                            "TIPO_PRECO": tipo_preco,
                            "ALIQUOTA_ICMS": aliquota,
                            "ESTADO_UF": uf,
                            "VALOR": valor,
                            "FLAG_ASTERISCO": bool(
                                row.get(
                                    f"_FLAG_{col_preco}",
                                    False,
                                )
                            ),
                            "EAN_INVALIDO": (
                                not validar_ean13(ean)
                                if ean
                                else True
                            ),
                            "COMPETENCIA": competencia,
                            "DATA_REFERENCIA": (
                                f"{competencia}-01"
                            ),
                            "DATA_CARGA": (
                                data_carga.isoformat()
                            ),
                        }
                    )

    resultado = pd.DataFrame(
        registros
    )

    logging.info(
        "%s processado: %s registros "
        "(%s linhas originais).",
        tipo_preco,
        len(resultado),
        len(df),
    )

    return resultado


# ============================================================================
# DIMENSÃO MEDICAMENTO
# ============================================================================

def gerar_dimensao_medicamento(
    df_raw: pd.DataFrame,
) -> pd.DataFrame:

    cols_dim = {
        "EAN 1": "EAN",
        "CÓDIGO GGREM": "CODIGO_GGREM",
        "PRODUTO": "PRODUTO",
        "APRESENTAÇÃO": "APRESENTACAO",
        "SUBSTÂNCIA": "SUBSTANCIA",
        "LABORATÓRIO": "LABORATORIO",
        "CNPJ": "CNPJ",
        "CLASSE TERAPÊUTICA":
            "CLASSE_TERAPEUTICA",
        "F.FARMACÊUTICA":
            "F_FARMACEUTICA",
        "REGIME DE PREÇO":
            "REGIME_PRECO",
        "TARJA": "TARJA",
        "RESTRIÇÃO HOSPITALAR":
            "RESTRICAO_HOSPITALAR",
        "CAP": "CAP",
        "TIPO DE PRODUTO (STATUS DO PRODUTO)":
            "TIPO_PRODUTO",
    }

    presentes = {
        k: v
        for k, v in cols_dim.items()
        if k in df_raw.columns
    }

    df_dim = df_raw[
        list(presentes.keys())
    ].copy()

    df_dim.rename(
        columns=presentes,
        inplace=True,
    )

    if "EAN" in df_dim.columns:
        df_dim["EAN"] = df_dim[
            "EAN"
        ].apply(tratar_ean)

    df_dim.dropna(
        subset=["EAN"],
        inplace=True,
    )

    df_dim.drop_duplicates(
        subset=["EAN"],
        keep="first",
        inplace=True,
    )

    if "RESTRICAO_HOSPITALAR" in df_dim.columns:

        df_dim[
            "RESTRICAO_HOSPITALAR"
        ] = df_dim[
            "RESTRICAO_HOSPITALAR"
        ].apply(
            lambda x:
                str(x).strip().upper()
                in (
                    "SIM",
                    "S",
                    "TRUE",
                    "1",
                    "X",
                )
                if pd.notna(x)
                else False
        )

    df_dim.reset_index(
        drop=True,
        inplace=True,
    )

    return df_dim


# ============================================================================
# DIMENSÃO ESTADO
# ============================================================================

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
                "ALIQUOTA_ICMS_VIGENTE":
                    uf_aliquota.get(
                        uf,
                        "",
                    ),
            }
            for uf, (
                nome,
                regiao,
            ) in ESTADOS_BRASIL.items()
        ]
    )


# ============================================================================
# DIMENSÃO CALENDÁRIO
# ============================================================================

def gerar_dimensao_calendario(
    data_inicio: str = "2024-01-01",
) -> pd.DataFrame:

    ano_atual = datetime.now().year

    data_fim = (
        f"{ano_atual + 1}-12-31"
    )

    datas = pd.date_range(
        start=data_inicio,
        end=data_fim,
        freq="D",
    )

    df = pd.DataFrame(
        {
            "DATA": datas
        }
    )

    df["ANO"] = (
        df["DATA"].dt.year
    )

    df["MES"] = (
        df["DATA"].dt.month
    )

    df["NOME_MES"] = (
        df["DATA"]
        .dt.strftime("%B")
        .str.capitalize()
    )

    df["TRIMESTRE"] = (
        df["DATA"].dt.quarter
    )

    df["COMPETENCIA"] = (
        df["DATA"]
        .dt.strftime("%Y-%m")
    )

    df["DATA"] = (
        df["DATA"]
        .dt.strftime("%Y-%m-%d")
    )

    return df


# ============================================================================
# EXPORTAÇÃO
# ============================================================================

def exportar_para_powerbi(
    df_fato: pd.DataFrame,
    df_medicamento: pd.DataFrame,
    df_estado: pd.DataFrame,
    df_calendario: pd.DataFrame,
    diretorio: str,
    modo_historico: bool = True,
) -> dict:

    dir_saida = Path(
        diretorio
    )

    dir_saida.mkdir(
        parents=True,
        exist_ok=True,
    )

    encoding = "utf-8-sig"

    arquivos = {}

    # ------------------------------------------------------------------
    # FATO
    # ------------------------------------------------------------------

    caminho_fato = (
        dir_saida / "fato_precos.csv"
    )

    if (
        modo_historico
        and caminho_fato.exists()
    ):

        df_existente = pd.read_csv(
            caminho_fato,
            dtype=str,
            encoding=encoding,
        )

        chave = [
            "EAN",
            "ESTADO_UF",
            "TIPO_PRECO",
            "COMPETENCIA",
            "ALIQUOTA_ICMS",
        ]

        chaves_existentes = set(
            df_existente[chave].apply(
                lambda r:
                    "|".join(
                        str(v)
                        for v in r
                    ),
                axis=1,
            )
        )

        mask_novos = (
            df_fato[chave]
            .apply(
                lambda r:
                    "|".join(
                        str(v)
                        for v in r
                    ),
                axis=1,
            )
            .apply(
                lambda x:
                    x not in chaves_existentes
            )
        )

        df_novos = df_fato[
            mask_novos
        ]

        if len(df_novos) > 0:

            df_final = pd.concat(
                [
                    df_existente,
                    df_novos,
                ],
                ignore_index=True,
            )

            logging.info(
                "Histórico: %s registros "
                "novos adicionados "
                "(total: %s).",
                len(df_novos),
                len(df_final),
            )

        else:

            df_final = df_existente

            logging.info(
                "Nenhum registro novo."
            )

    else:
        df_final = df_fato

    df_final.to_csv(
        caminho_fato,
        index=False,
        encoding=encoding,
    )

    arquivos[
        "fato_precos"
    ] = caminho_fato

    # ------------------------------------------------------------------
    # MEDICAMENTO
    # ------------------------------------------------------------------

    caminho_med = (
        dir_saida
        / "dim_medicamento.csv"
    )

    if caminho_med.exists():

        df_med_existente = pd.read_csv(
            caminho_med,
            dtype=str,
            encoding=encoding,
        )

        df_medicamento = pd.concat(
            [
                df_med_existente,
                df_medicamento,
            ],
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

    arquivos[
        "dim_medicamento"
    ] = caminho_med

    # ------------------------------------------------------------------
    # ESTADO
    # ------------------------------------------------------------------

    caminho_est = (
        dir_saida / "dim_estado.csv"
    )

    df_estado.to_csv(
        caminho_est,
        index=False,
        encoding=encoding,
    )

    arquivos[
        "dim_estado"
    ] = caminho_est

    # ------------------------------------------------------------------
    # CALENDÁRIO
    # ------------------------------------------------------------------

    caminho_cal = (
        dir_saida
        / "dim_calendario.csv"
    )

    df_calendario.to_csv(
        caminho_cal,
        index=False,
        encoding=encoding,
    )

    arquivos[
        "dim_calendario"
    ] = caminho_cal

    return arquivos


# ============================================================================
# ORQUESTRADOR
# ============================================================================

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

    config = carregar_config(
        args.config
    )

    configurar_log(
        config
    )

    inicio = datetime.now()

    try:

        # ==============================================================
        # 1. DESCOBRIR LINKS
        # ==============================================================

        links = obter_links_cmed(
            config
        )

        if not links.get("PMC"):
            raise RuntimeError(
                "Link do arquivo PMC não encontrado."
            )

        if not links.get("PMVG"):
            raise RuntimeError(
                "Link do arquivo PMVG não encontrado."
            )

        # ==============================================================
        # 2. COMPETÊNCIA
        # ==============================================================

        if args.competencia:

            if not re.fullmatch(
                r"20\d{2}-(0[1-9]|1[0-2])",
                args.competencia,
            ):
                raise ValueError(
                    "Competência inválida. "
                    "Use YYYY-MM, por exemplo 2026-07."
                )

            competencia_execucao = (
                args.competencia
            )

            logging.info(
                "Modo manual: competência forçada: %s",
                competencia_execucao,
            )

            arquivo_pmc = baixar_arquivo_cmed(
                links["PMC"],
                "PMC",
                config,
                competencia_execucao,
            )

        else:

            arquivo_pmc = baixar_arquivo_cmed(
                links["PMC"],
                "PMC",
                config,
            )

            competencia_execucao = (
                extrair_competencia_do_arquivo(
                    arquivo_pmc
                )
            )

            logging.info(
                "Modo automático: competência "
                "descoberta no arquivo PMC: %s",
                competencia_execucao,
            )

        # ==============================================================
        # 3. BAIXAR PMVG
        # ==============================================================

        arquivo_pmvg = baixar_arquivo_cmed(
            links["PMVG"],
            "PMVG",
            config,
            competencia_execucao,
        )

        # ==============================================================
        # 4. HASH
        # ==============================================================

        hashes = {
            "PMC": calcular_hash_arquivo(
                arquivo_pmc
            ),
            "PMVG": calcular_hash_arquivo(
                arquivo_pmvg
            ),
        }

        logging.info(
            "SHA-256 PMC : %s",
            hashes["PMC"],
        )

        logging.info(
            "SHA-256 PMVG: %s",
            hashes["PMVG"],
        )

        # ==============================================================
        # 5. ESTADO
        # ==============================================================

        estado = carregar_estado(
            config["arquivo_estado"]
        )

        if estado_ja_processado(
            estado,
            competencia_execucao,
            hashes,
        ):

            duracao = (
                datetime.now() - inicio
            ).total_seconds()

            logging.info(
                "ETL encerrado sem alterações "
                "em %.1fs.",
                duracao,
            )

            return

        logging.info(
            "Nova versão CMED detectada para %s. "
            "Continuando processamento.",
            competencia_execucao,
        )

        # ==============================================================
        # 6. LEITURA
        # ==============================================================

        df_raw_pmc = ler_arquivo_cmed(
            arquivo_pmc
        )

        df_raw_pmvg = ler_arquivo_cmed(
            arquivo_pmvg
        )

        # ==============================================================
        # 7. TRANSFORMAÇÃO
        # ==============================================================

        df_fato_pmc = (
            processar_tabela_precos(
                df_raw_pmc,
                arquivo_pmc,
                "PMC",
                competencia_execucao,
            )
        )

        # IMPORTANTE:
        # O arquivo PMVG contém informações de PMVG/PF.
        # Primeiro tentamos PMVG.
        df_fato_pmvg = (
            processar_tabela_precos(
                df_raw_pmvg,
                arquivo_pmvg,
                "PMVG",
                competencia_execucao,
            )
        )

        # Se a versão atual da planilha não possuir
        # colunas PMVG, tentamos PF.
        if df_fato_pmvg.empty:

            logging.warning(
                "Nenhuma coluna PMVG encontrada. "
                "Tentando extrair PF do arquivo PMVG."
            )

            df_fato_pmvg = (
                processar_tabela_precos(
                    df_raw_pmvg,
                    arquivo_pmvg,
                    "PF",
                    competencia_execucao,
                )
            )

        df_fato = pd.concat(
            [
                df_fato_pmc,
                df_fato_pmvg,
            ],
            ignore_index=True,
        )

        if df_fato.empty:
            raise RuntimeError(
                "Nenhum registro de preço foi produzido "
                "pelos arquivos CMED."
            )

        # ==============================================================
        # 8. DIMENSÕES
        # ==============================================================

        # PMC normalmente possui a identificação principal
        # dos medicamentos. Caso necessário, usamos PMVG como fallback.
        if len(df_raw_pmc) >= len(df_raw_pmvg):
            df_base_medicamento = df_raw_pmc
        else:
            df_base_medicamento = df_raw_pmvg

        df_medicamento = (
            gerar_dimensao_medicamento(
                df_base_medicamento
            )
        )

        df_estado = (
            gerar_dimensao_estado()
        )

        df_calendario = (
            gerar_dimensao_calendario()
        )

        # ==============================================================
        # 9. EXPORTAÇÃO
        # ==============================================================

        arquivos = exportar_para_powerbi(
            df_fato,
            df_medicamento,
            df_estado,
            df_calendario,
            config["diretorio_saida"],
        )

        # ==============================================================
        # 10. BACKUP
        # ==============================================================

        dir_hist = Path(
            config["diretorio_historico"]
        )

        dir_hist.mkdir(
            parents=True,
            exist_ok=True,
        )

        for nome, caminho in arquivos.items():

            backup = (
                dir_hist
                / f"{nome}_{competencia_execucao}.csv"
            )

            shutil.copy2(
                caminho,
                backup,
            )

            logging.info(
                "Backup: %s",
                backup,
            )

        # ==============================================================
        # 11. ATUALIZAR ESTADO
        # ==============================================================

        novo_estado = {
            "competencia": competencia_execucao,
            "hashes": hashes,
            "arquivos": {
                "PMC": arquivo_pmc.name,
                "PMVG": arquivo_pmvg.name,
            },
            "urls": {
                "PMC": links["PMC"],
                "PMVG": links["PMVG"],
            },
            "processado_em": datetime.now().isoformat(),
            "registros_fato": int(
                len(df_fato)
            ),
        }

        salvar_estado(
            config["arquivo_estado"],
            novo_estado,
        )

        # ==============================================================
        # 12. FINALIZAÇÃO
        # ==============================================================

        duracao = (
            datetime.now() - inicio
        ).total_seconds()

        logging.info(
            "ETL CMED CONCLUÍDO COM SUCESSO — "
            "competência=%s duração=%.1fs registros=%s",
            competencia_execucao,
            duracao,
            len(df_fato),
        )

        enviar_alerta_email(
            config,
            f"Sucesso — {competencia_execucao}",
            (
                "ETL CMED concluído com sucesso.\n"
                f"Competência: {competencia_execucao}\n"
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
