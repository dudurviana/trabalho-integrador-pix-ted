# Trabalho Integrador – Pix e TED

Etapa 0 – Configuração e funções auxiliares

import json, re, hashlib, unicodedata, zipfile, gzip, time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from IPython.display import display

pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 180)
pd.set_option("display.max_colwidth", 80)
PIX_MES_PARADA = "202611"   # o download para ao chegar neste mês, sem baixá-lo (AAAAMM)

# Pastas de trabalho (no Colab, /content)
BASE_DIR = Path("/content") if Path("/content").exists() else Path(".")
RAW_DIR = BASE_DIR / "raw"        # dados brutos exatamente como recebidos
OUT_DIR = BASE_DIR / "saidas"     # tabela final, gráfico, evidências
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Data/hora da coleta (fuso de Brasília) – ajustada para limitar até Novembro de 2026
TZ = ZoneInfo("America/Sao_Paulo")
AGORA = datetime(2026, 11, 13, tzinfo=TZ)
DATA_COLETA = AGORA.strftime("%Y-%m-%d %H:%M:%S %Z")
CARIMBO = AGORA.strftime("%Y%m%d_%H%M%S")

# Endpoints da API Olinda do Banco Central
URL_PIX = "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/EstatisticasTransacoesPix"
URL_TED = "https://olinda.bcb.gov.br/olinda/servico/MPV_DadosAbertos/versao/v1/odata/MeiosdePagamentosMensalDA"

# ⚠️ Só altere se a detecção automática (Etapa 3) escolher a coluna errada. None = detectar sozinho.
CFG = {
    "pix": {"mes": None, "qtd": None, "valor": None},
    "ted": {"mes": None, "qtd": None, "valor": None,
            "col_instrumento": None, "valores_instrumento": None},
}

PIX_SO_COLUNAS_ESSENCIAIS = False
PIX_MES_INICIAL = "2020-11"      # lançamento do Pix

# ⚠️ Multiplicadores para levar cada variável da unidade ORIGINAL até a unidade-base
MULT_ORIGEM = {
    "pix": {"qtd": 1.0, "valor": 1.0},
    "ted": {"qtd": 1.0, "valor": 1.0},
}
     

# ----------------------------- funções auxiliares (usadas em todas as etapas)

def _get(url, timeout=90, tentativas=3):
    # A URL é montada À MÃO: o requests codifica "$" como "%24" quando se usa params=, e o Olinda rejeita (HTTP 400).
    ultimo = None
    for t in range(tentativas):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            ultimo = f"HTTP {r.status_code} em {url}\n{r.text[:600]}"
            if r.status_code < 500:      # erro do cliente (400/404): repetir não adianta
                break
        except requests.RequestException as e:
            ultimo = f"{type(e).__name__}: {e}"
        time.sleep(2 * (t + 1))
    raise RuntimeError(ultimo)


def baixar_odata(url_base, extra="", page=10000, max_paginas=500):
    # Baixa todos os registros de um recurso OData, paginando com 
skip.
    # url_base: endereço sem query. extra: parâmetros adicionais já formatados (ex.: "&@Database='202011'").
    registros, ultimo_hash, contexto = [], None, None
    for p in range(max_paginas):
        corpo = _get(f"{url_base}?format=json&
top={page}&$skip={p * page}{extra}")
        contexto = corpo.get("@odata.context", contexto)
        lote = corpo.get("value", [])
        if not lote:
            break
        h = hashlib.md5(json.dumps(lote, sort_keys=True).encode()).hexdigest()
        if h == ultimo_hash:      # o servidor ignorou $skip e repetiu a página
            break
        ultimo_hash = h
        registros.extend(lote)
        if len(lote) < page:
            break
    return registros, contexto


def baixar_pix(select_cols=None):
    # EstatisticasTransacoesPix é uma FUNÇÃO OData: exige Database=@Database (AAAAMM). Um request por mês.
    meses = pd.period_range(PIX_MES_INICIAL, AGORA.strftime("%Y-%m"), freq="M")
    base = f"{URL_PIX}(Database=@Database)"
    tudo, ctx, falhas, select = [], None, [], ""
    for m in meses:
        ym = f"{m.year}{m.month:02d}"
        try:
            reg, c = baixar_odata(base, f"&@Database='{ym}'" + select)
        except RuntimeError as e:
            falhas.append((ym, str(e).splitlines()[0])); print(f"  Pix {ym}: FALHOU -> {falhas[-1][1]}"); continue
        ctx = ctx or c
        if reg and PIX_SO_COLUNAS_ESSENCIAIS and not select:
            cols = resolver_colunas(pd.json_normalize(reg[:200]), CFG["pix"], "PIX")
            chaves = [cols["mes"], cols["qtd"], cols["valor"]]
            select = "&$select=" + ",".join(chaves)
            reg = [{k: r.get(k) for k in chaves} for r in reg]     # primeiro mês já veio completo: reduz igual aos demais
        tudo.extend(reg)
        print(f"  Pix {ym}: {len(reg):>8,} linhas (acumulado: {len(tudo):,})")
    if not tudo:
        raise RuntimeError(f"Nenhum mês do Pix retornou dados. Primeiras falhas: {falhas[:3]}")
    if falhas:
        print(f"⚠️ {len(falhas)} mês(es) falharam: {[f[0] for f in falhas]} (aparecerão como lacunas no diagnóstico)")
    return tudo, ctx


def salvar_bruto(registros, contexto, chave):
    # Guarda o dado bruto (evidência do estado inicial), comprimido, com carimbo de data/hora.
    caminho = RAW_DIR / f"{chave}_bruto_{CARIMBO}.json.gz"
    with gzip.open(caminho, "wt", encoding="utf-8") as f:
        json.dump({"@odata.context": contexto, "value": registros}, f, ensure_ascii=False)
    return caminho


def carregar_bruto(caminho):
    with gzip.open(caminho, "rt", encoding="utf-8") as f:
        return json.load(f)["value"]


def ler_inicio_bruto(caminho, n=700):
    with gzip.open(caminho, "rt", encoding="utf-8") as f:
        return f.read(n)


def norm_nome(s):
    # "Quantidade de Transações" -> "quantidade_de_transacoes"
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()


def parse_mes(s):
    # Converte AAAAMM, 'AAAA-MM', 'AAAA-MM-DD' ou datetime para o 1º dia do mês. Inválidos -> NaT.
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.to_period("M").dt.to_timestamp()
    txt = s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    m6 = txt.str.fullmatch(r"\d{6}").fillna(False).astype(bool)
    if m6.any():
        out[m6] = pd.to_datetime(txt[m6], format="%Y%m", errors="coerce")
    resto = (~m6 & txt.notna()).to_numpy()
    if resto.any():
        try:
            conv = pd.to_datetime(txt[resto], errors="coerce", format="mixed")
        except (ValueError, TypeError):
            conv = pd.to_datetime(txt[resto], errors="coerce")
        out[resto] = conv
    return out.dt.to_period("M").dt.to_timestamp()


def para_numero(s):
    # Converte para float; aceita texto com vírgula decimal. Não convertíveis -> NaN.
    if pd.api.types.is_numeric_dtype(s):
        return s.astype(float)
    t = s.astype("string").str.strip()
    if t.str.contains(",", regex=False).fillna(False).any():
        t = t.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(t, errors="coerce")


def casas_dec(num):
    num = num.dropna()
    if num.empty:
        return 0
    dec = num.astype(str).str.extract(r"\.(\d+)$")[0].dropna()
    return int(dec.str.rstrip("0").str.len().max()) if len(dec) else 0


PADROES = {
    "mes":   r"(ano_?mes|^data|database|periodo|competencia|^mes)",
    "qtd":   r"(quant|qtd|qtde)",
    "valor": r"(^|_)(valor|vl|vlr|montante)",
}

def resolver_colunas(df, cfg, nome):
    out = {}
    for k, padrao in PADROES.items():
        if cfg.get(k):
            out[k] = cfg[k]
            print(f"[{nome}] {k:5s} -> '{out[k]}' (definida em CFG)")
            continue
        cands = [c for c in df.columns if re.search(padrao, norm_nome(c))]
        out[k] = cands[0] if cands else None
        aviso = f"  ⚠️ várias candidatas: {cands}" if len(cands) > 1 else ""
        print(f"[{nome}] {k:5s} -> {out[k]!r}{aviso}")
    faltando = [k for k, v in out.items() if v is None]
    if faltando:
        raise ValueError(f"[{nome}] Não achei coluna para {faltando}. Colunas disponíveis: {list(df.columns)}. Preencha CFG na Etapa 0.")
    return out
