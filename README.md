# Pipeline ETL: Pix vs. TED (Banco Central do Brasil)

> Coleta, classificação, preparação e integração de dados públicos da API OData (Olinda) do Banco Central do Brasil para comparação histórica entre **Pix** e **TED**[cite: 1].

## Visão Geral do Fluxo

* **Etapa 0:** Configuração do ambiente, diretórios, parâmetros temporais e funções auxiliares de requisição/normalização[cite: 1].
* **Etapa 1:** Aquisição das bases **Pix** (`EstatisticasTransacoesPix`) e **TED** (`MeiosdePagamentosMensalDA`) em formato bruto comprimido (`.json.gz`), com retomada incremental e controle de memória[cite: 1].
* **Etapa 2:** Diagnóstico estrutural dos payloads JSON recebidos, classificação quanto à estrutura/origem e definição das variáveis-alvo[cite: 1].
* **Etapa 3:** Construção das tabelas estruturadas e definição da chave de integração (`mes_referencia`)[cite: 1].
* **Etapa 4:** Diagnóstico de qualidade **antes** do tratamento (completude, consistência, precisão e atualidade)[cite: 1].
* **Etapa 5:** Aplicação de tratamentos, padronização de unidades e registro em log[cite: 1].
* **Etapa 6:** Integração via `inner join` (`one_to_one`) e cálculo do ticket médio (`R$`)[cite: 1].
* **Etapa 7:** Diagnóstico de qualidade **depois** do tratamento, comparativo antes × depois e discussão sobre ETL vs. ELT[cite: 1].
* **Etapa 8:** Geração do gráfico comparativo (`grafico_pix_ted.png`), interpretação dos resultados e limitações[cite: 1].
* **Etapa 9:** Exportação dos entregáveis em CSV, PNG e compactação em `entregaveis_n1.zip`[cite: 1].

---

## Etapa 0 – Configuração e Funções Auxiliares

```python
import gzip
import hashlib
import json
import re
import time
import unicodedata
import zipfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from IPython.display import display

# Opções de exibição do pandas
pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 180)
pd.set_option("display.max_colwidth", 80)

# Limite superior de coleta (formato AAAAMM): interrompe sem baixar o mês indicado
PIX_MES_PARADA = "202611"

# Diretórios de trabalho (compatível com Google Colab e execução local)
BASE_DIR = Path("/content") if Path("/content").exists() else Path(".")
RAW_DIR = BASE_DIR / "raw"     # Dados brutos exatamente como recebidos
OUT_DIR = BASE_DIR / "saidas"  # Tabelas tratadas, gráficos e evidências
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Data/hora de referência da coleta (fuso horário de Brasília)
TZ = ZoneInfo("America/Sao_Paulo")
AGORA = datetime(2026, 11, 13, tzinfo=TZ)
DATA_COLETA = AGORA.strftime("%Y-%m-%d %H:%M:%S %Z")
CARIMBO = AGORA.strftime("%Y%m%d_%H%M%S")

# Endpoints da API Olinda do Banco Central do Brasil
URL_PIX = (
    "[https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/](https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/)"
    "versao/v1/odata/EstatisticasTransacoesPix"
)
URL_TED = (
    "[https://olinda.bcb.gov.br/olinda/servico/MPV_DadosAbertos/](https://olinda.bcb.gov.br/olinda/servico/MPV_DadosAbertos/)"
    "versao/v1/odata/MeiosdePagamentosMensalDA"
)

# Sobrescrita manual opcional caso a detecção automática (Etapa 3) precise de ajuste
CFG = {
    "pix": {"mes": None, "qtd": None, "valor": None},
    "ted": {
        "mes": None,
        "qtd": None,
        "valor": None,
        "col_instrumento": None,
        "valores_instrumento": None,
    },
}

PIX_SO_COLUNAS_ESSENCIAIS = False
PIX_MES_INICIAL = "2020-11"  # Início da operação do Pix

# Multiplicadores da unidade original para a unidade-base
MULT_ORIGEM = {
    "pix": {"qtd": 1.0, "valor": 1.0},
    "ted": {"qtd": 1.0, "valor": 1.0},
}

PADROES = {
    "mes": r"(ano_?mes|^data|database|periodo|competencia|^mes)",
    "qtd": r"(quant|qtd|qtde)",
    "valor": r"(^|_)(valor|vl|vlr|montante)",
}


def _get(url, timeout=90, tentativas=3):
    """Executa GET montando a URL manualmente para evitar que '$' vire '%24' no OData."""
    ultimo = None
    for t in range(tentativas):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            ultimo = f"HTTP {r.status_code} em {url}\n{r.text[:600]}"
            if r.status_code < 500:
                break
        except requests.RequestException as e:
            ultimo = f"{type(e).__name__}: {e}"
        time.sleep(2 * (t + 1))
    raise RuntimeError(ultimo)


def baixar_odata(url_base, extra="", page=10000, max_paginas=500):
    """Baixa registros de um endpoint OData paginando via $skip até esgotar ou repetir."""
    registros, ultimo_hash, contexto = [], None, None
    for p in range(max_paginas):
        corpo = _get(f"{url_base}?format=json&top={page}&$skip={p * page}{extra}")
        contexto = corpo.get("@odata.context", contexto)
        lote = corpo.get("value", [])
        if not lote:
            break
        h = hashlib.md5(json.dumps(lote, sort_keys=True).encode()).hexdigest()
        if h == ultimo_hash:
            break
        ultimo_hash = h
        registros.extend(lote)
        if len(lote) < page:
            break
    return registros, contexto


def baixar_pix(select_cols=None):
    """Consulta EstatisticasTransacoesPix mês a mês (AAAAMM)."""
    meses = pd.period_range(PIX_MES_INICIAL, AGORA.strftime("%Y-%m"), freq="M")
    base = f"{URL_PIX}(Database=@Database)"
    tudo, ctx, falhas, select = [], None, [], ""
    for m in meses:
        ym = f"{m.year}{m.month:02d}"
        try:
            reg, c = baixar_odata(base, f"&@Database='{ym}'" + select)
        except RuntimeError as e:
            falhas.append((ym, str(e).splitlines()[0]))
            print(f"  Pix {ym}: FALHOU -> {falhas[-1][1]}")
            continue
        ctx = ctx or c
        if reg and PIX_SO_COLUNAS_ESSENCIAIS and not select:
            cols = resolver_colunas(pd.json_normalize(reg[:200]), CFG["pix"], "PIX")
            chaves = [cols["mes"], cols["qtd"], cols["valor"]]
            select = "&$select=" + ",".join(chaves)
            reg = [{k: r.get(k) for k in chaves} for r in reg]
        tudo.extend(reg)
        print(f"  Pix {ym}: {len(reg):>8,} linhas (acumulado: {len(tudo):,})")
    if not tudo:
        raise RuntimeError(
            f"Nenhum mês do Pix retornou dados. Primeiras falhas: {falhas[:3]}"
        )
    if falhas:
        print(
            f"⚠️ {len(falhas)} mês(es) falharam: {[f[0] for f in falhas]} "
            "(aparecerão como lacunas no diagnóstico)"
        )
    return tudo, ctx


def salvar_bruto(registros, contexto, chave):
    """Salva o payload bruto em JSON comprimido (.json.gz) com carimbo de execução."""
    caminho = RAW_DIR / f"{chave}_bruto_{CARIMBO}.json.gz"
    with gzip.open(caminho, "wt", encoding="utf-8") as f:
        json.dump({"@odata.context": contexto, "value": registros}, f, ensure_ascii=False)
    return caminho


def carregar_bruto(caminho):
    """Lê a lista 'value' de um arquivo bruto .json.gz."""
    with gzip.open(caminho, "rt", encoding="utf-8") as f:
        return json.load(f)["value"]


def ler_inicio_bruto(caminho, n=700):
    """Lê os primeiros n caracteres de um arquivo bruto .json.gz."""
    with gzip.open(caminho, "rt", encoding="utf-8") as f:
        return f.read(n)


def norm_nome(s):
    """Normaliza strings para snake_case sem acentos."""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()


def parse_mes(s):
    """Converte competências (AAAAMM, AAAA-MM, datas ou datetime) para o 1º dia do mês."""
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
    """Converte séries numéricas ou strings com vírgula decimal para float."""
    if pd.api.types.is_numeric_dtype(s):
        return s.astype(float)
    t = s.astype("string").str.strip()
    if t.str.contains(",", regex=False).fillna(False).any():
        t = t.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(t, errors="coerce")


def casas_dec(num):
    """Retorna o maior número de casas decimais significativas em uma série numérica."""
    num = num.dropna()
    if num.empty:
        return 0
    dec = num.astype(str).str.extract(r"\.(\d+)$")[0].dropna()
    return int(dec.str.rstrip("0").str.len().max()) if len(dec) else 0


def resolver_colunas(df, cfg, nome):
    """Identifica colunas de mês, quantidade e valor com base em CFG ou regex."""
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
        raise ValueError(
            f"[{nome}] Não achei coluna para {faltando}. "
            f"Colunas disponíveis: {list(df.columns)}. Preencha CFG na Etapa 0."
        )
    return out
```

---

## Etapa 1 – Aquisição das Bases

| Modalidade | Recurso / Endpoint OData | Conteúdo esperado |
| :--- | :--- | :--- |
| **Pix** | `Pix_DadosAbertos` / `EstatisticasTransacoesPix(Database=@Database)`[cite: 1] | Quantidade e valor de transações Pix liquidadas, por mês (`AAAAMM`)[cite: 1]. |
| **TED** | `MPV_DadosAbertos` / `MeiosdePagamentosMensalDA`[cite: 1] | Estatísticas mensais de meios de pagamento (inclui TED, Pix, boletos, cheques etc.)[cite: 1]. |

```python
PIX_SO_COLUNAS_ESSENCIAIS = True
PIX_MES_PARADA = "202611"  # Interrompe sem baixar este mês (AAAAMM)
TOP_MAX = 1_000_000
PIX_DIR = RAW_DIR / "pix_mensal"  # 1 arquivo .json.gz por mês (permite retomada)


def _get(url, timeout=180, tentativas=2):
    ultimo = None
    for t in range(tentativas):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            ultimo = f"HTTP {r.status_code} em {url}\n{r.text[:800]}"
            if r.status_code < 500:
                break
        except requests.RequestException as e:
            ultimo = f"{type(e).__name__}: {e}"
        time.sleep(3)
    raise RuntimeError(ultimo)


def baixar_odata(url_base, extra="", page=10000, max_paginas=500, paginar=True):
    if paginar:
        registros, ultimo_hash, contexto = [], None, None
        try:
            for p in range(max_paginas):
                corpo = _get(f"{url_base}?format=json&top={page}&$skip={p * page}{extra}")
                contexto = corpo.get("@odata.context", contexto)
                lote = corpo.get("value", [])
                if not lote:
                    break
                h = hashlib.md5(json.dumps(lote, sort_keys=True).encode()).hexdigest()
                if h == ultimo_hash:
                    break
                ultimo_hash = h
                registros.extend(lote)
                if len(lote) < page:
                    break
            return registros, contexto
        except RuntimeError as e:
            print(
                f"  ⚠️ paginação recusada pelo servidor ({str(e).splitlines()[0]}); "
                "tentando pedido único sem $skip..."
            )

    corpo = _get(f"{url_base}?format=json&top={TOP_MAX}{extra}")
    registros = corpo.get("value", [])
    if len(registros) in (100, 1000, 5000, 10000, 50000, 100000):
        print(
            f"  ⚠️ {len(registros):,} linhas é um número 'redondo': "
            "confira se o servidor não truncou o resultado."
        )
    return registros, corpo.get("@odata.context")


def _salvar_atomico(caminho, registros, contexto):
    tmp = caminho.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump({"@odata.context": contexto, "value": registros}, f, ensure_ascii=False)
    tmp.replace(caminho)


def baixar_pix(retomar=True):
    PIX_DIR.mkdir(parents=True, exist_ok=True)
    meses = pd.period_range(PIX_MES_INICIAL, AGORA.strftime("%Y-%m"), freq="M")
    base = f"{URL_PIX}(Database=@Database)"
    p = CFG["pix"]
    chaves = (
        [p["mes"], p["qtd"], p["valor"]]
        if (p["mes"] and p["qtd"] and p["valor"])
        else None
    )
    select = ("&$select=" + ",".join(chaves)) if chaves else None
    ctx, amostra, falhas, seguidas = None, [], [], 0
    novos, reaproveitados, n_total = [], [], 0

    for m in meses:
        ym = f"{m.year}{m.month:02d}"
        if ym >= PIX_MES_PARADA:
            print(f"  Parando em {ym} (PIX_MES_PARADA = {PIX_MES_PARADA}).")
            break
        arq = PIX_DIR / f"pix_{ym}.json.gz"
        if retomar and arq.exists():
            reg = carregar_bruto(arq)
            n_total += len(reg)
            reaproveitados.append(ym)
            if not amostra:
                amostra = reg[:2000]
            print(f"  Pix {ym}: {len(reg):>9,} linhas (arquivo já existia; reaproveitado)")
            del reg
            continue

        t0 = time.time()
        try:
            if chaves is None:
                amo = _get(f"{base}?format=json&top=200&@Database='{ym}'", timeout=120)
                if not amo.get("value"):
                    print(f"  Pix {ym}: sem dados")
                    continue
                cols = resolver_colunas(pd.DataFrame(amo["value"]), CFG["pix"], "PIX")
                chaves = [cols["mes"], cols["qtd"], cols["valor"]]
                select = "&$select=" + ",".join(chaves)
            try:
                reg, c = baixar_odata(
                    base, f"&@Database='{ym}'" + (select or ""), paginar=False
                )
            except RuntimeError:
                if not select:
                    raise
                print("  ⚠️ $select recusado; baixando o mês completo e projetando as colunas")
                select = ""
                reg, c = baixar_odata(base, f"&@Database='{ym}'", paginar=False)
            seguidas = 0
        except RuntimeError as e:
            falhas.append((ym, str(e)))
            seguidas += 1
            print(f"  Pix {ym}: FALHOU -> {str(e).splitlines()[0]}")
            if seguidas >= 3 and not n_total:
                raise RuntimeError(
                    "Os 3 primeiros meses falharam. Resposta COMPLETA do servidor:\n"
                    + falhas[0][1]
                )
            continue

        if not select:
            reg = [{k: r.get(k) for k in chaves} for r in reg]
        ctx = ctx or c
        if reg:
            _salvar_atomico(arq, reg, c)
            n_total += len(reg)
            novos.append(ym)
            if not amostra:
                amostra = reg[:2000]
        print(
            f"  Pix {ym}: {len(reg):>9,} linhas em {time.time() - t0:.0f}s "
            f"(acumulado: {n_total:,})"
        )
        del reg

    if not n_total:
        raise RuntimeError(
            "Nenhum mês do Pix retornou dados. Primeira falha:\n"
            + (falhas[0][1] if falhas else "sem erro HTTP (listas vazias)")
        )
    if falhas:
        print(
            f"⚠️ {len(falhas)} mês(es) falharam: {[f[0] for f in falhas]} "
            "(aparecerão como lacunas no diagnóstico)"
        )
    info = {
        "novos": novos,
        "reaproveitados": reaproveitados,
        "falhas": [f[0] for f in falhas],
        "colunas": chaves,
    }
    return PIX_DIR, n_total, amostra, ctx, info


def carregar_pix_estruturado(pasta):
    """Mantém apenas os registros referentes ao próprio mês de cada arquivo."""
    partes, lidas = [], 0
    for arq in sorted(Path(pasta).glob("pix_*.json.gz")):
        ym = arq.stem.replace(".json", "").split("_")[1]
        d = pd.DataFrame(carregar_bruto(arq))
        lidas += len(d)
        if d.empty:
            continue
        col = next(c for c in d.columns if re.search(r"anomes|database|^mes", norm_nome(c)))
        partes.append(d[d[col].astype(str).str.strip() == ym])
        del d
    df = pd.concat(partes, ignore_index=True)
    print(f"Pix: {lidas:,} linhas lidas dos arquivos -> {len(df):,} mantidas (só o mês de cada arquivo)")
    if len(df) < lidas:
        print("⚠️ A API devolve do mês pedido em diante: as linhas repetidas foram descartadas.")
    print(f"memória ~ {df.memory_usage(deep=True).sum() / 1e9:.2f} GB")
    return df


def ler_inicio_bruto(caminho, n=700):
    caminho = Path(caminho)
    if caminho.is_dir():
        caminho = sorted(caminho.glob("pix_*.json.gz"))[0]
    with gzip.open(caminho, "rt", encoding="utf-8") as f:
        return f.read(n)


def _parse_mes_unicos(s):
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


def parse_mes(s):
    """Fatoriza valores distintos antes do parse para economizar memória."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.to_period("M").dt.to_timestamp()
    codigos, unicos = pd.factorize(s)
    parsed = _parse_mes_unicos(pd.Series(unicos)).to_numpy()
    if len(parsed) == 0:
        return pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    out = pd.Series(parsed[np.where(codigos < 0, 0, codigos)], index=s.index)
    out[codigos < 0] = pd.NaT
    return out


def casas_dec(num):
    num = num.dropna()
    if num.empty:
        return 0
    if len(num) > 200_000:
        num = num.sample(200_000, random_state=0)
    dec = num.astype(str).str.extract(r"\.(\d+)$")[0].dropna()
    return int(dec.str.rstrip("0").str.len().max()) if len(dec) else 0


def baixar_ted():
    base = f"{URL_TED}(AnoMes=@AnoMes)"

    def pedir(ym):
        corpo = _get(f"{base}?format=json&top={TOP_MAX}&@AnoMes='{ym}'", timeout=300)
        return corpo.get("value", []), corpo.get("@odata.context")

    ini = PIX_MES_INICIAL.replace("-", "")
    print(f"  TED: pedindo AnoMes={ini} (o servidor é lento, aguarde)...")
    reg, ctx = pedir(ini)
    meses = sorted({str(r["AnoMes"]) for r in reg})
    print(f"  TED: {len(reg):,} linhas; {len(meses)} mês(es) distintos: {meses[:1]} ... {meses[-1:]}")
    if len(meses) > 1 or not reg:
        return reg, ctx

    tudo = list(reg)
    for m in pd.period_range(PIX_MES_INICIAL, AGORA.strftime("%Y-%m"), freq="M")[1:]:
        ym = f"{m.year}{m.month:02d}"
        if ym >= PIX_MES_PARADA:
            break
        try:
            r, _ = pedir(ym)
        except RuntimeError as e:
            print(f"  TED {ym}: FALHOU -> {str(e).splitlines()[0]}")
            continue
        tudo.extend(r)
    return tudo, ctx


FONTES = {
    "pix": {
        "nome": "Estatísticas de transações Pix (EstatisticasTransacoesPix)",
        "origem": "Banco Central do Brasil – Portal de Dados Abertos / API Olinda (Pix_DadosAbertos)",
        "url": URL_PIX + "(Database=@Database)  [1 requisição por mês, AAAAMM]",
        "pagina_catalogo": "[https://dadosabertos.bcb.gov.br/dataset/pix](https://dadosabertos.bcb.gov.br/dataset/pix)",
    },
    "ted": {
        "nome": "Meios de Pagamentos Mensais (MeiosdePagamentosMensalDA)",
        "origem": "Banco Central do Brasil – Portal de Dados Abertos / API Olinda (MPV_DadosAbertos)",
        "url": URL_TED,
        "pagina_catalogo": "[https://dadosabertos.bcb.gov.br/dataset/estatisticas-meios-pagamentos](https://dadosabertos.bcb.gov.br/dataset/estatisticas-meios-pagamentos)",
    },
}

ARQUIVOS, REGISTROS_BRUTOS, N_REGISTROS, linhas_registro = {}, {}, {}, []
for chave, f in FONTES.items():
    print(f"\nBaixando {chave.upper()} ...")
    obs = ""
    if chave == "pix":
        caminho, n, amostra, ctx, info = baixar_pix()
        obs = (
            f"1 arquivo por mês em {caminho}; baixados agora: {len(info['novos'])}; "
            f"reaproveitados: {len(info['reaproveitados'])}; meses com falha: {info['falhas']}; "
            f"colunas baixadas ($select): {info['colunas']}"
        )
    else:
        amostra, ctx = baixar_ted()
        n = len(amostra)
        if not n:
            raise RuntimeError("A API da TED não devolveu registros.")
        caminho = salvar_bruto(amostra, ctx, chave)

    ARQUIVOS[chave], REGISTROS_BRUTOS[chave], N_REGISTROS[chave] = caminho, amostra, n
    linhas_registro.append({
        "base": f["nome"],
        "origem": f["origem"],
        "url_api": f["url"],
        "catalogo": f["pagina_catalogo"],
        "formato_obtido": "JSON (OData/Olinda, via requests)",
        "data_coleta": DATA_COLETA,
        "arquivo_bruto": str(caminho),
        "n_registros": n,
        "campos_recebidos": ", ".join(amostra[0].keys()),
        "observacoes": obs,
    })
    print(f"[{chave.upper()}] {n:,} registros | bruto em {caminho}")

registro_aquisicao = pd.DataFrame(linhas_registro)
display(registro_aquisicao.T)
```

---

## Etapa 2 – Classificação dos Dados

```python
def evidencia_estrutura(chave):
    reg = REGISTROS_BRUTOS[chave]
    chaves_1 = set(reg[0].keys())
    return {
        "base": chave.upper(),
        "tem_envelope_de_metadados (@odata.context)": "@odata.context" in ler_inicio_bruto(ARQUIVOS[chave], 2000),
        "registros": len(reg),
        "esquema_uniforme (todos com as mesmas chaves)": all(set(r.keys()) == chaves_1 for r in reg),
        "campos_aninhados (dict/list dentro do registro)": any(
            isinstance(v, (dict, list)) for r in reg[:2000] for v in r.values()
        ),
        "n_campos": len(chaves_1),
    }


display(pd.DataFrame([evidencia_estrutura(c) for c in FONTES]).set_index("base").T)

for c in FONTES:
    print(f"\n--- Primeiros 700 caracteres do arquivo bruto de {c.upper()} ---")
    print(ler_inicio_bruto(ARQUIVOS[c], 700))
```

* **Estrutura:** Os dados chegam em JSON com envelope `@odata.context` (**semiestruturado** na chegada), mas possuem esquema uniforme e plano sem aninhamento (**tabular na essência**)[cite: 1].
* **Origem:** **Externa** (Banco Central do Brasil), **pública** e **aberta** (Portal de Dados Abertos / API Olinda)[cite: 1].

```python
classificacao_variaveis = pd.DataFrame(
    [
        [
            "mes_referencia",
            "Competência mensal (AAAAMM)",
            "Temporal / Qualitativa ordinal (ou discreta conforme critério)",
            "Chave de integração entre Pix e TED",
        ],
        [
            "modalidade (Pix/TED)",
            "Instrumento de pagamento",
            "Qualitativa nominal",
            "Identifica a modalidade analisada",
        ],
        [
            "quantidade",
            "Volume / contagem de transações",
            "Quantitativa discreta",
            "Padronizada em milhões de transações",
        ],
        [
            "valor",
            "Montante financeiro movimentado",
            "Quantitativa contínua",
            "Padronizada em R$ bilhões",
        ],
    ],
    columns=["variavel", "o_que_representa", "classificacao", "observacao"],
)
display(classificacao_variaveis)
```

## Etapa 3 – Construção das Tabelas Estruturadas

```python
df_pix_estr = carregar_pix_estruturado(ARQUIVOS["pix"])
df_ted_estr = pd.json_normalize(carregar_bruto(ARQUIVOS["ted"]))

for nome, df in (
    ("PIX", df_pix_estr),
    ("TED (base completa de meios de pagamento)", df_ted_estr),
):
    print(f"\n===== {nome}: {df.shape[0]:,} linhas x {df.shape[1]} colunas =====")
    display(df.dtypes.to_frame("dtype_recebido").T)
    display(df.head(5))

COLS_PIX = resolver_colunas(df_pix_estr, CFG["pix"], "PIX")

# TED possui estrutura larga (uma coluna por meio de pagamento), sem coluna categórica de instrumento
COLS_TED = {"mes": "AnoMes", "qtd": "quantidadeTED", "valor": "valorTED"}
COL_INSTR, VALORES_TED = "colunas quantidadeTED/valorTED (formato largo)", ["TED"]

faltam = [c for c in COLS_TED.values() if c not in df_ted_estr.columns]
assert not faltam, f"colunas ausentes na TED: {faltam}; disponíveis: {list(df_ted_estr.columns)}"

df_ted_sel = df_ted_estr[list(COLS_TED.values())].copy()
print(f"Linhas da TED: {len(df_ted_sel):,}")
display(df_ted_sel.head())
```

* **Chave de integração:** `mes_referencia` (1º dia do mês, tipo data), compartilhada pelas duas bases com granularidade mensal e agregada antes do `inner join` para garantir relação `1:1` no maior intervalo comum.

---

## Etapa 4 – Diagnóstico de Qualidade ANTES do Tratamento

```python
def diagnosticar(df, base, col_mes, cols_num, cols_dim=(), fase="", rotulos=None):
    """Avalia completude, consistência, precisão e atualidade de uma tabela."""
    rotulos = rotulos or {}
    rot = lambda c: rotulos.get(c, c)
    linhas = []

    def add(dim, ind, valor, evid=""):
        linhas.append({
            "base": base,
            "fase": fase,
            "dimensao": dim,
            "indicador": ind,
            "valor": valor,
            "evidencia": evid,
        })

    n = len(df)
    mes = parse_mes(df[col_mes])
    validos = mes.dropna()

    # Completude
    add("Completude", "Linhas", n)
    nulos = df.isna().sum()
    add(
        "Completude",
        "Células ausentes",
        int(nulos.sum()),
        "; ".join(f"{c}: {v}" for c, v in nulos.items() if v > 0) or "nenhuma",
    )
    add(
        "Completude",
        "Linhas 100% preenchidas (%)",
        round(float(df.notna().all(axis=1).mean() * 100), 2) if n else np.nan,
    )
    if len(validos):
        esperado = pd.period_range(validos.min(), validos.max(), freq="M")
        presentes = set(validos.dt.to_period("M"))
        faltam = [str(p) for p in esperado if p not in presentes]
        add(
            "Completude",
            "Meses ausentes dentro do intervalo",
            len(faltam),
            (", ".join(faltam[:12]) + (" ..." if len(faltam) > 12 else ""))
            if faltam
            else "nenhum",
        )
    else:
        add("Completude", "Meses ausentes dentro do intervalo", np.nan, "sem datas válidas")

    # Consistência
    add("Consistência", "Tipo armazenado da coluna de data", str(df[col_mes].dtype))
    add(
        "Consistência",
        "Datas não interpretáveis",
        int((mes.isna() & df[col_mes].notna()).sum()),
    )
    for c in cols_num:
        num = para_numero(df[c])
        add("Consistência", f"{rot(c)}: tipo armazenado", str(df[c].dtype))
        add(
            "Consistência",
            f"{rot(c)}: valores não numéricos",
            int((num.isna() & df[c].notna()).sum()),
        )
        add("Consistência", f"{rot(c)}: valores negativos", int((num < 0).sum()))
        add("Consistência", f"{rot(c)}: valores zero", int((num == 0).sum()))
    add("Consistência", "Linhas duplicadas (exatas)", int(df.duplicated().sum()))
    chave = [col_mes, *cols_dim]
    add(
        "Consistência",
        "Duplicidade na chave (mês + dimensões)",
        int(df.duplicated(subset=chave).sum()),
        "chave: " + ", ".join(chave),
    )

    # Precisão
    for c in cols_num:
        num = para_numero(df[c])
        add(
            "Precisão",
            f"{rot(c)}: casas decimais (máx.)",
            casas_dec(num),
            "após a mudança de unidade o nº de casas deixa de ser comparável"
            if fase == "depois"
            else "",
        )
        serie = (
            pd.DataFrame({"m": mes, "v": num})
            .dropna(subset=["m"])
            .groupby("m")["v"]
            .sum(min_count=1)
            .sort_index()
        )
        var = serie.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
        if len(var) > 3 and var.std() > 0:
            z = (var - var.mean()) / var.std()
            atip = var[z.abs() > 3]
            add(
                "Precisão",
                f"{rot(c)}: variações mensais atípicas (|z|>3)",
                len(atip),
                ", ".join(str(i.to_period("M")) for i in atip.index[:8]) or "nenhuma",
            )
        else:
            add(
                "Precisão",
                f"{rot(c)}: variações mensais atípicas (|z|>3)",
                np.nan,
                "série curta demais",
            )

    # Atualidade
    if len(validos):
        ult, prim = validos.max(), validos.min()
        hoje = pd.Period(AGORA.strftime("%Y-%m"), freq="M")
        add("Atualidade", "Primeiro mês da série", str(prim.to_period("M")))
        add("Atualidade", "Último mês da série", str(ult.to_period("M")))
        add(
            "Atualidade",
            "Defasagem do último mês até hoje (meses)",
            (hoje - ult.to_period("M")).n,
            f"coleta em {DATA_COLETA}",
        )
    return pd.DataFrame(linhas)


def colunas_dim(df, cols):
    return [c for c in df.columns if c not in (cols["mes"], cols["qtd"], cols["valor"])]


diag_pix_antes = diagnosticar(
    df_pix_estr,
    "Pix",
    COLS_PIX["mes"],
    [COLS_PIX["qtd"], COLS_PIX["valor"]],
    colunas_dim(df_pix_estr, COLS_PIX),
    fase="antes",
    rotulos={COLS_PIX["qtd"]: "quantidade", COLS_PIX["valor"]: "valor"},
)

diag_ted_antes = diagnosticar(
    df_ted_sel,
    "TED",
    COLS_TED["mes"],
    [COLS_TED["qtd"], COLS_TED["valor"]],
    colunas_dim(df_ted_sel, COLS_TED),
    fase="antes",
    rotulos={COLS_TED["qtd"]: "quantidade", COLS_TED["valor"]: "valor"},
)

print("===== PIX – antes do tratamento =====")
display(diag_pix_antes.drop(columns=["base", "fase"]))

print("===== TED – antes do tratamento =====")
display(diag_ted_antes.drop(columns=["base", "fase"]))
```

---

## Etapa 5 – Tratamentos Aplicados e Justificativas

Todas as alterações passam por um registro detalhado (indicando a ação executada, a justificativa e o total de linhas impactadas). Diretrizes centrais:

* **Remoção de duplicatas idênticas antes de eliminar dimensões:** evita excluir registros válidos que só se distinguem por alguma categoria.
* **Sem preenchimento de lacunas:** em dados financeiros, estimar ou inserir valores criaria informações fictícias. Os campos vazios permanecem como `NaN` e recebem marcação.
* **Consolidação mensal via soma (reamostragem):** alinha as duas tabelas ao mesmo nível de detalhe (um registro por mês). Caso falte algum item daquele mês, o resultado vira `NaN` (somar apenas o que existe reduziria o total de forma silenciosa).
* **Ajuste de escala:** volumes expressos em milhões de operações e montantes em bilhões de reais.
* **Padronização de identificadores:** colunas no formato `snake_case`, sem acentuação e acompanhadas do prefixo correspondente à modalidade.

> ⚠️ Caso a tabela contenha categorias adicionais (como pessoa física ou jurídica) e já inclua linhas consolidadas de total, somar tudo geraria contagem duplicada. O código emite um alerta sempre que houver mais de um registro por mês.
>
> 

```python
def tratar(df_raw, cols, base, prefixo, mult_qtd, mult_valor, remover_duplicatas=True):
    """Aplica limpeza, tipagem, agregação mensal e padronização de unidades."""
    log = []

    def reg(trat, motivo, antes, depois, afetadas=""):
        log.append({
            "base": base,
            "tratamento": trat,
            "motivo": motivo,
            "linhas_antes": antes,
            "linhas_depois": depois,
            "afetadas": afetadas,
        })

    d = df_raw.copy()

    # 1) Duplicatas exatas
    n0 = len(d)
    if remover_duplicatas:
        d = d.drop_duplicates()
        reg(
            "Remoção de duplicatas exatas",
            "Evitar dupla contagem de uma mesma observação",
            n0,
            len(d),
            n0 - len(d),
        )
    else:
        reg(
            "Duplicatas exatas NÃO removidas",
            "Só 3 colunas foram baixadas: linhas iguais podem ser categorias diferentes",
            n0,
            n0,
            0,
        )

    # 2) Seleção e padronização de nomes
    outras = [c for c in d.columns if c not in (cols["mes"], cols["qtd"], cols["valor"])]
    d = d[[cols["mes"], cols["qtd"], cols["valor"]]].rename(
        columns={
            cols["mes"]: "mes_referencia",
            cols["qtd"]: "quantidade",
            cols["valor"]: "valor",
        }
    )
    reg(
        "Seleção e padronização de nomes de colunas",
        "Manter só as variáveis da comparação; nomes em snake_case, sem acento",
        len(d),
        len(d),
        f"dimensões que serão agregadas: {outras or 'nenhuma'}",
    )

    # 3) Data -> datetime (1º dia do mês)
    antes_na = int(d["mes_referencia"].isna().sum())
    d["mes_referencia"] = parse_mes(d["mes_referencia"])
    reg(
        "Conversão da data para datetime (1º dia do mês)",
        "Permitir ordenar no tempo e integrar pela chave",
        len(d),
        len(d),
        int(d["mes_referencia"].isna().sum()) - antes_na,
    )

    # 4) Numéricos -> float
    coerc = 0
    for c in ("quantidade", "valor"):
        nulos_antes = int(d[c].isna().sum())
        d[c] = para_numero(d[c])
        coerc += int(d[c].isna().sum()) - nulos_antes
    reg(
        "Conversão de quantidade e valor para float",
        "Garantir tipo numérico para cálculo e gráfico",
        len(d),
        len(d),
        coerc,
    )

    # 5) Linhas sem mês válido
    n0 = len(d)
    d = d.dropna(subset=["mes_referencia"])
    reg(
        "Remoção de linhas sem mês válido",
        "Sem data não é possível posicionar a observação na série",
        n0,
        len(d),
        n0 - len(d),
    )

    # 6) Ausentes e negativos (sinalizados, não imputados)
    reg(
        "Valores ausentes mantidos (sem imputação)",
        "Imputar dados financeiros criaria informação inexistente",
        len(d),
        len(d),
        int(d[["quantidade", "valor"]].isna().sum().sum()),
    )
    reg(
        "Valores negativos sinalizados",
        "Quantidade/valor negativos seriam inconsistentes; apenas contados",
        len(d),
        len(d),
        int((d[["quantidade", "valor"]] < 0).sum().sum()),
    )

    # 7) Agregação mensal
    por_mes = int(d.groupby("mes_referencia").size().max()) if len(d) else 0
    n0 = len(d)
    soma = d.groupby("mes_referencia")[["quantidade", "valor"]].sum(min_count=1)
    parcial = d[["quantidade", "valor"]].isna().groupby(d["mes_referencia"]).any()
    soma = soma.mask(parcial)
    d = soma.reset_index()
    reg(
        "Agregação mensal (soma) / reamostragem",
        "Uma linha por mês, mesma granularidade nas duas bases",
        n0,
        len(d),
        f"máx. de linhas por mês antes: {por_mes}",
    )
    reg(
        "Meses com componente ausente viram NaN",
        "Somar só parte das categorias subestimaria o mês sem aviso",
        len(d),
        len(d),
        int(parcial.to_numpy().sum()),
    )
    if por_mes > 1:
        print(
            f"⚠️ [{base}] havia até {por_mes} linhas por mês e elas foram SOMADAS. "
            "Confirme que não existem linhas de total misturadas às de detalhe."
        )

    # 8) Padronização de unidades
    d[f"{prefixo}_quantidade_mi"] = d["quantidade"] * mult_qtd / 1e6
    d[f"{prefixo}_valor_bi"] = d["valor"] * mult_valor / 1e9
    d = d.drop(columns=["quantidade", "valor"]).sort_values("mes_referencia").reset_index(drop=True)
    reg(
        "Padronização de unidades",
        "Quantidade em milhões de transações; valor em R$ bilhões",
        len(d),
        len(d),
        f"multiplicadores de origem: qtd={mult_qtd:g}, valor={mult_valor:g}",
    )
    return d, pd.DataFrame(log)


pix_trat, log_pix = tratar(
    df_pix_estr,
    COLS_PIX,
    "Pix",
    "pix",
    MULT_ORIGEM["pix"]["qtd"],
    MULT_ORIGEM["pix"]["valor"],
    remover_duplicatas=not PIX_SO_COLUNAS_ESSENCIAIS,
)

ted_trat, log_ted = tratar(
    df_ted_sel,
    COLS_TED,
    "TED",
    "ted",
    MULT_ORIGEM["ted"]["qtd"],
    MULT_ORIGEM["ted"]["valor"],
)

log_tratamentos = pd.concat([log_pix, log_ted], ignore_index=True)
display(log_tratamentos)
```
## Etapa 6 – Integração: Tabela Final Pix × TED

```python
final = pix_trat.merge(ted_trat, on="mes_referencia", how="inner", validate="one_to_one")
final = final.sort_values("mes_referencia").reset_index(drop=True)

# Indicador derivado: ticket médio (R$ por transação)
final["pix_ticket_medio_rs"] = final["pix_valor_bi"] * 1e9 / (final["pix_quantidade_mi"] * 1e6)
final["ted_ticket_medio_rs"] = final["ted_valor_bi"] * 1e9 / (final["ted_quantidade_mi"] * 1e6)

print(f"Pix: {pix_trat['mes_referencia'].min():%Y-%m} a {pix_trat['mes_referencia'].max():%Y-%m} ({len(pix_trat)} meses)")
print(f"TED: {ted_trat['mes_referencia'].min():%Y-%m} a {ted_trat['mes_referencia'].max():%Y-%m} ({len(ted_trat)} meses)")
print(f"Final (interseção): {final['mes_referencia'].min():%Y-%m} a {final['mes_referencia'].max():%Y-%m} ({len(final)} meses)")

esperado = pd.period_range(final["mes_referencia"].min(), final["mes_referencia"].max(), freq="M")
print("Meses faltando dentro do intervalo comum:", len(esperado) - len(final))

display(final.head())
display(final.tail())
display(final.describe().T)


def periodicidade(serie_mes):
    m = serie_mes.dropna().drop_duplicates().sort_values()
    if len(m) < 2:
        return "indeterminada"
    meses = (m.dt.year * 12 + m.dt.month).diff().dropna()
    return "mensal" if meses.median() == 1 else f"intervalo mediano de {meses.median():g} mês(es)"


registro_variaveis = pd.DataFrame([
    {
        "base": "Pix",
        "variaveis_usadas": f"{COLS_PIX['mes']}, {COLS_PIX['qtd']}, {COLS_PIX['valor']}",
        "periodicidade_observada": periodicidade(pix_trat["mes_referencia"]),
        "periodo": f"{pix_trat['mes_referencia'].min():%Y-%m} a {pix_trat['mes_referencia'].max():%Y-%m}",
        "unidade_origem_assumida": f"qtd x{MULT_ORIGEM['pix']['qtd']:g} (unidades); valor x{MULT_ORIGEM['pix']['valor']:g} (R$)",
    },
    {
        "base": "TED",
        "variaveis_usadas": f"{COLS_TED['mes']}, {COLS_TED['qtd']}, {COLS_TED['valor']} (filtro {COL_INSTR} = {VALORES_TED})",
        "periodicidade_observada": periodicidade(ted_trat["mes_referencia"]),
        "periodo": f"{ted_trat['mes_referencia'].min():%Y-%m} a {ted_trat['mes_referencia'].max():%Y-%m}",
        "unidade_origem_assumida": f"qtd x{MULT_ORIGEM['ted']['qtd']:g} (unidades); valor x{MULT_ORIGEM['ted']['valor']:g} (R$)",
    },
])
display(registro_variaveis.T)
```

---

## Etapa 7 – Diagnóstico de Qualidade DEPOIS do Tratamento

```python
diag_pix_depois = diagnosticar(
    pix_trat,
    "Pix",
    "mes_referencia",
    ["pix_quantidade_mi", "pix_valor_bi"],
    fase="depois",
    rotulos={"pix_quantidade_mi": "quantidade", "pix_valor_bi": "valor"},
)

diag_ted_depois = diagnosticar(
    ted_trat,
    "TED",
    "mes_referencia",
    ["ted_quantidade_mi", "ted_valor_bi"],
    fase="depois",
    rotulos={"ted_quantidade_mi": "quantidade", "ted_valor_bi": "valor"},
)

diag_final = diagnosticar(
    final,
    "Tabela final",
    "mes_referencia",
    ["pix_quantidade_mi", "pix_valor_bi", "ted_quantidade_mi", "ted_valor_bi"],
    fase="depois",
)


def comparativo(antes, depois, base):
    a = antes[["dimensao", "indicador", "valor"]].rename(columns={"valor": "antes"}).astype({"antes": "object"})
    d = depois[["dimensao", "indicador", "valor"]].rename(columns={"valor": "depois"}).astype({"depois": "object"})
    out = a.merge(d, on=["dimensao", "indicador"], how="outer", sort=False)
    out.insert(0, "base", base)
    return out


comp_pix = comparativo(diag_pix_antes, diag_pix_depois, "Pix")
comp_ted = comparativo(diag_ted_antes, diag_ted_depois, "TED")

print("===== Comparativo Pix: antes x depois =====")
display(comp_pix)

print("===== Comparativo TED: antes x depois =====")
display(comp_ted)

print("===== Tabela final (depois) =====")
display(diag_final.drop(columns=["base", "fase"]))
```

* **ETL ou ELT?** O processo aproxima-se de **ETL**, pois os dados são extraídos das APIs do Banco Central, transformados em memória com `pandas` (limpeza, tipagem, agregação mensal, padronização de unidades e junção) e exportados como saída final, guardando os `.json.gz` apenas como evidência bruta.

---

## Etapa 8 – Gráfico de Comparação

```python
LOG_QTD = True  # Altere para False caso prefira escala linear na quantidade

try:
    plt.style.use("seaborn-v0_8-whitegrid")
except OSError:
    plt.style.use("ggplot")

x = final["mes_referencia"]
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 9), sharex=True)

# Painel 1: Quantidade de transações (milhões)
ax1.plot(x, final["pix_quantidade_mi"], color="#1b9e77", lw=2.2, label="Pix")
ax1.plot(x, final["ted_quantidade_mi"], color="#d95f02", lw=2.2, label="TED")
ax1.set_ylabel("Quantidade de transações por mês (milhões)" + (" – escala log" if LOG_QTD else ""))
ax1.set_title("Quantidade mensal de transações")
if LOG_QTD:
    ax1.set_yscale("log")
ax1.legend(title="Modalidade", loc="upper left")

# Painel 2: Valor transacionado (R$ bilhões)
ax2.plot(x, final["pix_valor_bi"], color="#1b9e77", lw=2.2, label="Pix")
ax2.plot(x, final["ted_valor_bi"], color="#d95f02", lw=2.2, label="TED")
ax2.set_ylabel("Valor transacionado por mês (R$ bilhões)")
ax2.set_title("Valor mensal transacionado")
ax2.set_xlabel("Mês de referência")
ax2.legend(title="Modalidade", loc="upper left")
ax2.xaxis.set_major_locator(mdates.YearLocator())
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax2.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=(1, 4, 7, 10)))

fig.suptitle(
    "Pix x TED no Brasil: evolução mensal (" + f"{x.min():%m/%Y} a {x.max():%m/%Y})",
    fontsize=14,
    fontweight="bold",
)
fig.text(
    0.01,
    0.005,
    "Fonte: Banco Central do Brasil – API de Dados Abertos (Pix_DadosAbertos e MPV_DadosAbertos). "
    f"Coleta em {DATA_COLETA[:10]}. Elaboração própria.",
    fontsize=8,
    ha="left",
    va="bottom",
)
fig.tight_layout(rect=(0, 0.02, 1, 0.97))
fig.savefig(OUT_DIR / "grafico_pix_ted.png", dpi=200, bbox_inches="tight")
plt.show()
```

<img width="1094" height="897" alt="image" src="https://github.com/user-attachments/assets/9f1d36fd-eee1-484e-b083-36dfc24119a3" />



### Interpretação

**Guia para análise com base nos dados consolidados:**

1. **Quantidade:** Em qual momento o Pix supera a TED? Qual é a proporção Pix/TED no encerramento da série?
2. **Valor:** O Pix iguala ou supera a TED em volume financeiro? Confronte `pix_valor_bi` e `ted_valor_bi` nos meses finais.
3. **Ticket médio:** Analise `pix_ticket_medio_rs` frente a `ted_ticket_medio_rs` — o que esses valores indicam sobre o padrão de uso de cada meio (pagamentos cotidianos de menor porte versus movimentações mais elevadas)?
4. **Tendência:** O volume da TED recua em números absolutos ou apenas perde espaço relativo?

---

#### Respostas

1. **Quantidade.** Quando começou a operar (`11/2020`), a TED superava com folga o Pix: por volta de 192 milhões de operações frente a 30 milhões do Pix. O Pix toma a dianteira já em janeiro de 2021 (170 milhões contra 119 milhões) e amplia a vantagem mês a mês. Em setembro de 2026, o Pix alcança aproximadamente 7,17 bilhões de transações e a TED cerca de 61 milhões, deixando a proporção Pix/TED em torno de 117 para 1.
2. **Valor.** Nesse quesito, o cenário muda. Em setembro de 2026, o Pix movimenta perto de R$ 4,13 trilhões, com relação Pix/TED próxima de 0,78. Nos registros observados, o Pix encosta na TED, sem chegar a ultrapassá-la — para concluir que isso nunca ocorreu em toda a janela analisada, vale verificar a tabela completa dos quatro pontos.
3. **Ticket médio.** O contraste é marcante e segue direções opostas. O Pix recuou de algo em torno de R$ 450 (em `09/2026`), enquanto a TED avançou para perto de R$ 67 mil. No encerramento (`09/2026`), o ticket médio da TED equivale a cerca de 150 vezes o do Pix. Esse comportamento sugere que o Pix ganhou espaço em transferências frequentes de menor quantia, ao passo que a TED passou a concentrar operações mais vultosas — trata-se de uma leitura indicativa dos dados, e não de comprovação definitiva.
4. **Tendência da TED.** Em quantidade absoluta, a TED diminuiu: saiu de aproximadamente 192 milhões (pico em `11/2020`) para cerca de 61 milhões, queda próxima de 68%. Já o montante nominal cresceu em torno de 61%, alcançando R$ 4,13 trilhões. Sua fatia no total de operações (`TED + Pix`) despencou de cerca de 87% para algo em torno de 0,9%. Em resumo, perdeu volume e participação de mercado, mas manteve relevância em valores maiores.

---

### Limitações da Comparação (Ponto de Partida)

* **Origem e critérios distintos:** Pix e TED provêm de bases diferentes do Banco Central, cada qual com suas próprias regras de apuração.
* **Escopo do Pix:** Segundo a documentação do BC, as estatísticas não abrangem operações liquidadas internamente no próprio participante sem passagem pelo SPI, o que pode gerar subestimação.
* **Escopo da TED:** É preciso checar se a seleção isola estritamente a TED (sem misturar DOC ou TEC) e qual é a abrangência da fonte (`CIP-SITRAF`/`STR`).
* **Valores nominais:** Não há ajuste inflacionário, de modo que os números em reais reúnem expansão real e variação de preços.
* **Substituição parcial:** O Pix abriu frentes de uso diário e de baixo valor que não existiam na TED, logo a queda desta não representa apenas migração direta.
* **Efeito da escala:** O eixo logarítmico na quantidade ajuda a visualizar séries de ordens distintas, porém destaca visualmente a TED em relação à escala linear.



///COLS_TED = {"mes": "AnoMes", "qtd": "quantidadeTED", "valor": "valorTED"}
COL_INSTR, VALORES_TED = "colunas quantidadeTED/valorTED (formato largo

## Etapa 9 – Saídas e Entregáveis

```python
final_out = final.copy()
final_out["mes_referencia"] = final_out["mes_referencia"].dt.strftime("%Y-%m-%d")
final_out.to_csv(OUT_DIR / "tabela_final_pix_ted.csv", index=False, encoding="utf-8")

registro_aquisicao.to_csv(OUT_DIR / "registro_aquisicao.csv", index=False, encoding="utf-8")
registro_variaveis.to_csv(OUT_DIR / "registro_variaveis.csv", index=False, encoding="utf-8")
classificacao_variaveis.to_csv(OUT_DIR / "classificacao_variaveis.csv", index=False, encoding="utf-8")
pd.concat([diag_pix_antes, diag_ted_antes]).to_csv(
    OUT_DIR / "diagnostico_antes.csv", index=False, encoding="utf-8"
)
pd.concat([diag_pix_depois, diag_ted_depois, diag_final]).to_csv(
    OUT_DIR / "diagnostico_depois.csv", index=False, encoding="utf-8"
)
pd.concat([comp_pix, comp_ted]).to_csv(
    OUT_DIR / "comparativo_antes_depois.csv", index=False, encoding="utf-8"
)
log_tratamentos.to_csv(OUT_DIR / "log_tratamentos.csv", index=False, encoding="utf-8")

zip_path = BASE_DIR / "entregaveis_n1.zip"
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
    for p in list(OUT_DIR.glob("*")) + list(RAW_DIR.glob("*")):
        z.write(p, arcname=f"{p.parent.name}/{p.name}")

print("Arquivos gerados:")
for p in sorted(OUT_DIR.glob("*")):
    print(" -", p)

print("\nPacote completo:", zip_path)

try:
    from google.colab import files
    files.download(str(zip_path))
except Exception:
    pass
```

### Arquivos Gerados em `/content/saidas` e Pacote Final
* `classificacao_variaveis.csv`[cite: 1]
* `comparativo_antes_depois.csv`[cite: 1]
* `diagnostico_antes.csv`[cite: 1]
* `diagnostico_depois.csv`[cite: 1]
* `grafico_pix_ted.png`[cite: 1]
* `log_tratamentos.csv`[cite: 1]
* `registro_aquisicao.csv`[cite: 1]
* `registro_variaveis.csv`[cite: 1]
* `tabela_final_pix_ted.csv`[cite: 1]
* Pacote compactado: `/content/entregaveis_n1.zip`[cite: 1]



Referências
BANCO CENTRAL DO BRASIL. Portal de Dados Abertos – Pix (API EstatisticasTransacoesPix). https://dadosabertos.bcb.gov.br/dataset/pix
BANCO CENTRAL DO BRASIL. Portal de Dados Abertos – Estatísticas de Meios de Pagamentos (recurso MeiosdePagamentosMensalDA). https://dadosabertos.bcb.gov.br/dataset/estatisticas-meios-pagamentos
Complete com: documentação do pandas, matplotlib e requests, material da disciplina e quaisquer outras fontes que você usar (com data de acesso).
