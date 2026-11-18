import gzip
import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import requests

TZ = ZoneInfo("America/Sao_Paulo")
PASTA_BASE = Path("/content")
PASTA_RAW = PASTA_BASE / "raw"
PASTA_SAIDAS = PASTA_BASE / "saidas"
PASTA_RAW.mkdir(parents=True, exist_ok=True)
PASTA_SAIDAS.mkdir(parents=True, exist_ok=True)

PIX_MES_INICIAL = "2020-11"
PIX_MES_PARADA = "202611"
PIX_SO_COLUNAS_ESSENCIAIS = True

DATA_COLETA = datetime(2026, 11, 13, 0, 0, tzinfo=TZ)
STAMP = DATA_COLETA.strftime("%Y%m%d_%H%M%S")
DATA_REF_TXT = DATA_COLETA.strftime("%Y-%m-%d")

URL_PIX = (
    "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/"
    "EstatisticasTransacoesPix(Database=@Database)?@Database='202011'&$format=json"
)
URL_TED = (
    "https://olinda.bcb.gov.br/olinda/servico/MPV_DadosAbertos/versao/v1/odata/"
    "MeiosdePagamentosMensalDA(AnoMes=@AnoMes)?@AnoMes='202011'&$format=json"
)

CFG = {
    "pix": {
        "mes": ["AnoMes", "DataBase", "Data", "Mes"],
        "qtd": ["QUANTIDADE", "Quantidade", "Qtd"],
        "val": ["VALOR", "Valor"],
    },
    "ted": {
        "mes": ["AnoMes", "DataBase", "Data", "Mes"],
        "qtd": ["quantidadeTED", "QuantidadeTED", "QuantidadeTed"],
        "val": ["valorTED", "ValorTED", "ValorTed"],
    },
}

MULT_ORIGEM = {
    "pix_qtd": 1.0,
    "pix_val": 1.0,
    "ted_qtd": 1e3,
    "ted_val": 1e6,
}


def prox_mes(ym: str) -> str:
    a, m = int(ym[:4]), int(ym[4:6])
    return f"{a + 1}01" if m == 12 else f"{a}{m + 1:02d}"


def _get(session: requests.Session, url: str) -> dict:
    for tentativa in range(5):
        try:
            resposta = session.get(url, timeout=(15, 120))
            resposta.raise_for_status()
            return resposta.json()
        except Exception:
            if tentativa == 4:
                raise
            time.sleep(min(20, 2**tentativa))
    return {}


def salvar_gz(arq: Path, obj: dict) -> None:
    with gzip.open(arq, "wt", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def ler_gz(arq: Path) -> dict:
    with gzip.open(arq, "rt", encoding="utf-8") as f:
        return json.load(f)


def ler_inicio_bruto(arq: Path, n: int = 5) -> tuple[dict, list[dict]]:
    if arq.suffix == ".gz":
        with gzip.open(arq, "rt", encoding="utf-8") as f:
            txt = f.read(300000)
    else:
        with open(arq, "rt", encoding="utf-8") as f:
            txt = f.read(300000)

    meta = {}
    for k in ["fonte", "endpoint", "database", "data_coleta", "total_registros"]:
        m = re.search(rf'"{k}"\s*:\s*("[^"]*"|\d+)', txt)
        if m:
            v = m.group(1)
            meta[k] = v.strip('"') if v.startswith('"') else int(v)

    amostra = []
    pos = txt.find('"value"')
    if pos != -1:
        i = txt.find("[", pos)
        dec = json.JSONDecoder()
        if i != -1:
            i += 1
            while len(amostra) < n and i < len(txt):
                while i < len(txt) and txt[i] in " \r\n\t,":
                    i += 1
                if i >= len(txt) or txt[i] == "]":
                    break
                try:
                    obj, j = dec.raw_decode(txt, i)
                except Exception:
                    break
                amostra.append(obj)
                i = j
    return meta, amostra


def norm_nome(nome: str) -> str:
    s = unicodedata.normalize("NFKD", str(nome)).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s.strip().lower()).strip("_")
    return s or "col"


def achar_col(df: pd.DataFrame, candidatas: list[str]) -> str | None:
    mapa = {norm_nome(c): c for c in df.columns}
    for c in candidatas:
        if norm_nome(c) in mapa:
            return mapa[norm_nome(c)]
    return None


def parse_mes(serie: pd.Series) -> pd.Series:
    def _conv(v):
        if pd.isna(v):
            return pd.NaT
        if isinstance(v, (int, np.integer, float, np.floating)):
            s = str(int(round(float(v))))
            if re.fullmatch(r"\d{6}", s):
                return pd.to_datetime(s + "01", format="%Y%m%d", errors="coerce")
            if re.fullmatch(r"\d{8}", s):
                return pd.to_datetime(s[:6] + "01", format="%Y%m%d", errors="coerce")
        s = str(v).strip()
        m = re.match(r"^(\d{4})[-/](\d{1,2})", s)
        if m:
            return pd.to_datetime(f"{m.group(1)}-{int(m.group(2)):02d}-01", errors="coerce")
        m = re.match(r"^(\d{6})$", s)
        if m:
            return pd.to_datetime(s + "01", format="%Y%m%d", errors="coerce")
        m = re.match(r"^(\d{1,2})[-/](\d{4})$", s)
        if m:
            return pd.to_datetime(f"{m.group(2)}-{int(m.group(1)):02d}-01", errors="coerce")
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        return pd.NaT if pd.isna(dt) else pd.Timestamp(dt.year, dt.month, 1)

    return serie.map(_conv)


def para_numero(serie: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(serie):
        return pd.to_numeric(serie, errors="coerce")

    def _conv(v):
        if pd.isna(v):
            return np.nan
        s = re.sub(r"[^\d,.\-]", "", str(v).strip())
        if not s or s in {"-", ".", ","}:
            return np.nan
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            partes = s.split(",")
            if len(partes) == 2 and len(partes[1]) == 3:
                s = "".join(partes)
            else:
                s = s.replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return np.nan

    return serie.map(_conv)


def casas_dec(serie: pd.Series) -> int:
    s = pd.to_numeric(serie, errors="coerce").dropna()
    if s.empty:
        return 0
    if len(s) > 50000:
        s = s.sample(50000, random_state=42)
    mx = 0
    for x in s:
        txt = f"{float(x):.10f}".rstrip("0").rstrip(".")
        if "." in txt:
            mx = max(mx, len(txt.split(".")[1]))
    return mx


def diagnosticar(df_raw: pd.DataFrame, nome: str, cfg: dict, data_ref: pd.Timestamp) -> dict:
    c_mes = achar_col(df_raw, cfg["mes"])
    c_qtd = achar_col(df_raw, cfg["qtd"])
    c_val = achar_col(df_raw, cfg["val"])

    mes = parse_mes(df_raw[c_mes])
    qtd = para_numero(df_raw[c_qtd])
    val = para_numero(df_raw[c_val])

    dup_mes = int(mes.dropna().duplicated().sum())
    dup_lin = int(df_raw.duplicated().sum())

    aux = pd.DataFrame({"mes": mes, "qtd": qtd, "val": val}).dropna(subset=["mes"])
    aux = aux.sort_values("mes")
    mensal = aux.groupby("mes", as_index=True)[["qtd", "val"]].sum(min_count=1) if dup_mes > 0 else aux.set_index("mes")[["qtd", "val"]]

    outliers = {}
    for col in ["qtd", "val"]:
        var_pct = mensal[col].pct_change().replace([np.inf, -np.inf], np.nan).dropna()
        if len(var_pct) >= 4 and var_pct.std(ddof=1) > 0:
            z = (var_pct - var_pct.mean()) / var_pct.std(ddof=1)
            outliers[col] = [d.strftime("%Y-%m") for d in z[z.abs() > 3].index]
        else:
            outliers[col] = []

    ult = mensal.index.max() if len(mensal) else pd.NaT
    pri = mensal.index.min() if len(mensal) else pd.NaT
    defasagem = ((data_ref.year - ult.year) * 12 + (data_ref.month - ult.month)) if pd.notna(ult) else np.nan

    return {
        "base": nome,
        "linhas": int(len(df_raw)),
        "col_mes": c_mes,
        "col_qtd": c_qtd,
        "col_val": c_val,
        "tipo_mes": str(df_raw[c_mes].dtype),
        "tipo_qtd": str(df_raw[c_qtd].dtype),
        "tipo_val": str(df_raw[c_val].dtype),
        "nulos_mes": int(df_raw[c_mes].isna().sum()),
        "nulos_qtd": int(df_raw[c_qtd].isna().sum()),
        "nulos_val": int(df_raw[c_val].isna().sum()),
        "invalidos_mes": int((df_raw[c_mes].notna() & mes.isna()).sum()),
        "invalidos_qtd": int((df_raw[c_qtd].notna() & qtd.isna()).sum()),
        "invalidos_val": int((df_raw[c_val].notna() & val.isna()).sum()),
        "dup_linhas": dup_lin,
        "dup_mes": dup_mes,
        "neg_qtd": int((qtd < 0).sum()),
        "neg_val": int((val < 0).sum()),
        "zero_qtd": int((qtd == 0).sum()),
        "zero_val": int((val == 0).sum()),
        "dec_qtd": casas_dec(qtd),
        "dec_val": casas_dec(val),
        "primeiro_mes": pri.strftime("%Y-%m") if pd.notna(pri) else None,
        "ultimo_mes": ult.strftime("%Y-%m") if pd.notna(ult) else None,
        "meses_unicos": int(mensal.index.nunique()),
        "defasagem_meses": defasagem,
        "var_atipica_qtd": outliers["qtd"],
        "var_atipica_val": outliers["val"],
    }


def executar():
    print("Pasta raw:", PASTA_RAW)
    print("Pasta saidas:", PASTA_SAIDAS)
    print("Data da coleta:", DATA_REF_TXT)

    PIX_DIR = PASTA_RAW / "pix_mensal"
    PIX_DIR.mkdir(parents=True, exist_ok=True)
    ARQ_TED_BRUTO = PASTA_RAW / f"ted_bruto_{STAMP}.json.gz"

    def baixar_odata(url_ini: str, top: int = 10000) -> list[dict]:
        sep = "&" if "?" in url_ini else "?"
        url = url_ini if "$top=" in url_ini else f"{url_ini}{sep}$top={top}"
        out = []
        with requests.Session() as s:
            s.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
            while url:
                data = _get(s, url)
                vals = data.get("value") or []
                if not isinstance(vals, list):
                    break
                out.extend(vals)
                url = data.get("@odata.nextLink")
        return out

    def baixar_pix(mes_ini: str, parada: str, so_essenciais: bool = True, forcar: bool = False) -> list[Path]:
        ym = re.sub(r"\D", "", mes_ini)[:6]
        sel = "&$select=AnoMes,VALOR,QUANTIDADE" if so_essenciais else ""
        arquivos = []
        total_geral = 0

        with requests.Session() as s:
            s.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
            while ym < parada:
                arq_mes = PIX_DIR / f"pix_{ym}.json.gz"

                if arq_mes.exists() and arq_mes.stat().st_size > 100 and not forcar:
                    meta, _ = ler_inicio_bruto(arq_mes, n=1)
                    n_reg = int(meta.get("total_registros", 0))
                    if n_reg > 0:
                        arquivos.append(arq_mes)
                        total_geral += n_reg
                        ym = prox_mes(ym)
                        continue

                url = (
                    "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata/"
                    f"EstatisticasTransacoesPix(Database=@Database)?@Database='{ym}'&$format=json&$top=10000{sel}"
                )
                lote = []
                while url:
                    data = _get(s, url)
                    vals = data.get("value") or []
                    if not isinstance(vals, list):
                        break
                    lote.extend(vals)
                    url = data.get("@odata.nextLink")

                if not lote:
                    print(f"Pix {ym}: sem registros (fim da série disponível).")
                    break

                salvar_gz(arq_mes, {
                    "fonte": "BCB Olinda - Pix_DadosAbertos",
                    "endpoint": f"EstatisticasTransacoesPix(Database='{ym}')",
                    "database": ym,
                    "data_coleta": DATA_COLETA.isoformat(),
                    "total_registros": len(lote),
                    "value": lote,
                })
                arquivos.append(arq_mes)
                total_geral += len(lote)
                print(f"Pix {ym}: {len(lote):,} linhas salvas | acumulado: {total_geral:,}")
                ym = prox_mes(ym)

        return arquivos

    arqs_pix_brutos = baixar_pix(PIX_MES_INICIAL, PIX_MES_PARADA, PIX_SO_COLUNAS_ESSENCIAIS)
    ted_vals = baixar_odata(URL_TED)
    salvar_gz(ARQ_TED_BRUTO, {
        "fonte": "BCB Olinda - MPV_DadosAbertos",
        "endpoint": URL_TED,
        "data_coleta": DATA_COLETA.isoformat(),
        "total_registros": len(ted_vals),
        "value": ted_vals,
    })

    def carregar_pix_estruturado(arquivos: list[Path]):
        partes = []
        lidas_total = 0
        mantidas_total = 0
        for arq in sorted(arquivos):
            obj = ler_gz(arq)
            ym = str(obj.get("database") or re.sub(r"\D", "", arq.stem)[:6])
            vals = obj.get("value") or []
            lidas_total += len(vals)
            if not vals:
                continue

            df_m = pd.DataFrame(vals)
            c_mes = achar_col(df_m, CFG["pix"]["mes"])
            c_qtd = achar_col(df_m, CFG["pix"]["qtd"])
            c_val = achar_col(df_m, CFG["pix"]["val"])
            df_m = df_m[[c_mes, c_qtd, c_val]].copy()

            ym_reg = df_m[c_mes].astype(str).str.replace(r"\D", "", regex=True).str[:6]
            df_m = df_m.loc[ym_reg == ym].copy()
            mantidas_total += len(df_m)
            partes.append(df_m)

        df_out = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame(columns=["AnoMes", "QUANTIDADE", "VALOR"])
        info = {
            "arquivos_lidos": len(arquivos),
            "linhas_lidas_nos_arquivos": lidas_total,
            "linhas_mantidas_do_mes_correspondente": mantidas_total,
            "linhas_descartadas_por_repeticao_api": lidas_total - mantidas_total,
        }
        return df_out, info

    df_pix_raw, info_leitura_pix = carregar_pix_estruturado(arqs_pix_brutos)

    ted_obj = ler_gz(ARQ_TED_BRUTO)
    df_ted_full = pd.DataFrame(ted_obj.get("value") or [])
    c_mes_ted = achar_col(df_ted_full, CFG["ted"]["mes"])
    c_qtd_ted = achar_col(df_ted_full, CFG["ted"]["qtd"])
    c_val_ted = achar_col(df_ted_full, CFG["ted"]["val"])
    df_ted_raw = df_ted_full[[c_mes_ted, c_qtd_ted, c_val_ted]].copy()

    print("Resumo da normalização do Pix bruto:")
    print(pd.DataFrame([info_leitura_pix]).to_string(index=False))

    DATA_REF = pd.Timestamp(DATA_COLETA.date())
    diag_pix_antes = diagnosticar(df_pix_raw, "Pix (antes)", CFG["pix"], DATA_REF)
    diag_ted_antes = diagnosticar(df_ted_raw, "TED (antes)", CFG["ted"], DATA_REF)
    print("\nDiagnóstico antes do tratamento:")
    print(pd.DataFrame([diag_pix_antes, diag_ted_antes]).T.to_string())

    logs = []

    def reg_log(etapa: str, detalhe: str) -> None:
        logs.append({"etapa": etapa, "detalhe": detalhe})

    def tratar(df_raw: pd.DataFrame, nome: str, cfg: dict, mult_qtd: float, mult_val: float) -> pd.DataFrame:
        c_mes = achar_col(df_raw, cfg["mes"])
        c_qtd = achar_col(df_raw, cfg["qtd"])
        c_val = achar_col(df_raw, cfg["val"])

        df = pd.DataFrame({
            "mes_referencia": parse_mes(df_raw[c_mes]),
            "qtd_orig": para_numero(df_raw[c_qtd]),
            "val_orig": para_numero(df_raw[c_val]),
        })

        sem_mes = int(df["mes_referencia"].isna().sum())
        if sem_mes > 0:
            df = df.dropna(subset=["mes_referencia"]).copy()
            reg_log(nome, f"Descartadas {sem_mes} linhas sem mes_referencia válido.")
        else:
            reg_log(nome, "Nenhuma linha descartada por data inválida.")

        neg_q = int((df["qtd_orig"] < 0).sum())
        neg_v = int((df["val_orig"] < 0).sum())
        reg_log(nome, f"Valores negativos mantidos sem alteração (qtd={neg_q}, val={neg_v}).")

        nulos_q = int(df["qtd_orig"].isna().sum())
        nulos_v = int(df["val_orig"].isna().sum())
        reg_log(nome, f"Valores ausentes mantidos como NaN sem imputação (qtd={nulos_q}, val={nulos_v}).")

        linhas_antes = len(df)
        max_por_mes = int(df.groupby("mes_referencia").size().max()) if len(df) else 0
        if max_por_mes > 1 and len(df_raw.columns) <= 4:
            reg_log(nome, f"Aviso: {max_por_mes} linhas por mês sem colunas de dimensão; somando tudo por mês (verificar se não há subtotais na fonte).")

        df_m = df.groupby("mes_referencia", as_index=False)[["qtd_orig", "val_orig"]].sum(min_count=1)
        df_m = df_m.sort_values("mes_referencia").reset_index(drop=True)
        reg_log(nome, f"Agregação mensal concluída: {linhas_antes} -> {len(df_m)} meses únicos.")

        df_m["quantidade_mi"] = (df_m["qtd_orig"] * mult_qtd) / 1e6
        df_m["valor_bi"] = (df_m["val_orig"] * mult_val) / 1e9
        reg_log(nome, f"Unidades padronizadas: qtd * {mult_qtd:g} / 1e6 e val * {mult_val:g} / 1e9.")
        return df_m[["mes_referencia", "quantidade_mi", "valor_bi"]]

    df_pix = tratar(df_pix_raw, "Pix", CFG["pix"], MULT_ORIGEM["pix_qtd"], MULT_ORIGEM["pix_val"])
    df_pix = df_pix.rename(columns={"quantidade_mi": "pix_quantidade_mi", "valor_bi": "pix_valor_bi"})

    df_ted = tratar(df_ted_raw, "TED", CFG["ted"], MULT_ORIGEM["ted_qtd"], MULT_ORIGEM["ted_val"])
    df_ted = df_ted.rename(columns={"quantidade_mi": "ted_quantidade_mi", "valor_bi": "ted_valor_bi"})

    print("\nLog de operações:")
    print(pd.DataFrame(logs).to_string(index=False))

    df_final = pd.merge(df_pix, df_ted, on="mes_referencia", how="inner", validate="one_to_one")
    df_final = df_final.sort_values("mes_referencia").reset_index(drop=True)

    df_final["pix_ticket_medio_rs"] = np.where(
        df_final["pix_quantidade_mi"] > 0,
        (df_final["pix_valor_bi"] * 1e9) / (df_final["pix_quantidade_mi"] * 1e6),
        np.nan,
    )
    df_final["ted_ticket_medio_rs"] = np.where(
        df_final["ted_quantidade_mi"] > 0,
        (df_final["ted_valor_bi"] * 1e9) / (df_final["ted_quantidade_mi"] * 1e6),
        np.nan,
    )

    grade_esperada = pd.date_range(df_final["mes_referencia"].min(), df_final["mes_referencia"].max(), freq="MS")
    meses_faltantes = sorted(set(grade_esperada) - set(df_final["mes_referencia"]))
    validacao_join = {
        "tipo_join": "inner",
        "chave": "mes_referencia",
        "linhas_pix": len(df_pix),
        "linhas_ted": len(df_ted),
        "linhas_final": len(df_final),
        "duplicidades_chave_final": int(df_final["mes_referencia"].duplicated().sum()),
        "nulos_final_total": int(df_final.isna().sum().sum()),
        "inicio_comum": df_final["mes_referencia"].min().strftime("%Y-%m-%d"),
        "fim_comum": df_final["mes_referencia"].max().strftime("%Y-%m-%d"),
        "meses_faltando_no_intervalo": len(meses_faltantes),
    }

    print("\nValidação do join:")
    print(pd.DataFrame([validacao_join]).to_string(index=False))
    print("\nPrimeiras 10 linhas:")
    print(df_final.head(10).to_string(index=False))
    print("\nEstatísticas descritivas:")
    print(df_final.drop(columns=["mes_referencia"]).describe().T.to_string())

    arq_csv = PASTA_SAIDAS / f"pix_vs_ted_mensal_{STAMP}.csv"
    df_export = df_final.copy()
    df_export["mes_referencia"] = df_export["mes_referencia"].dt.strftime("%Y-%m-%d")
    df_export.to_csv(arq_csv, index=False, encoding="utf-8")
    print("\nTabela final salva em:", arq_csv)

    diag_pix_depois = diagnosticar(df_pix, "Pix (depois)", {
        "mes": ["mes_referencia"], "qtd": ["pix_quantidade_mi"], "val": ["pix_valor_bi"]
    }, DATA_REF)
    diag_ted_depois = diagnosticar(df_ted, "TED (depois)", {
        "mes": ["mes_referencia"], "qtd": ["ted_quantidade_mi"], "val": ["ted_valor_bi"]
    }, DATA_REF)

    print("\nComparação Pix antes × depois:")
    print(pd.DataFrame([diag_pix_antes, diag_pix_depois]).T.to_string())
    print("\nComparação TED antes × depois:")
    print(pd.DataFrame([diag_ted_antes, diag_ted_depois]).T.to_string())

    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "axes.edgecolor": "#CCCCCC",
        "axes.linewidth": 0.8,
    })

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 9), sharex=True)
    fig.patch.set_facecolor("white")
    cor_pix = "#008060"
    cor_ted = "#1F4E79"

    ax1.plot(df_final["mes_referencia"], df_final["pix_quantidade_mi"], color=cor_pix, linewidth=2.3, label="Pix (milhões de transações)")
    ax1.plot(df_final["mes_referencia"], df_final["ted_quantidade_mi"], color=cor_ted, linewidth=2.3, label="TED (milhões de transações)")
    ax1.set_yscale("log")
    ax1.set_ylabel("Transações por mês (milhões, escala log)")
    ax1.set_title("Quantidade mensal de transações: Pix vs. TED", fontsize=12, fontweight="bold", pad=10)
    ax1.grid(True, which="major", linestyle="--", alpha=0.45)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}".replace(",", ".") if x >= 1 else f"{x:.2f}".replace(".", ",")))
    ax1.legend(frameon=True, facecolor="white", edgecolor="#DDDDDD", loc="upper left")

    ax2.plot(df_final["mes_referencia"], df_final["pix_valor_bi"], color=cor_pix, linewidth=2.3, label="Pix (R$ bilhões)")
    ax2.plot(df_final["mes_referencia"], df_final["ted_valor_bi"], color=cor_ted, linewidth=2.3, label="TED (R$ bilhões)")
    ax2.set_ylabel("Volume financeiro mensal (R$ bilhões)")
    ax2.set_xlabel("Mês de referência")
    ax2.set_title("Valor financeiro mensal movimentado: Pix vs. TED", fontsize=12, fontweight="bold", pad=10)
    ax2.grid(True, linestyle="--", alpha=0.45)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"R$ {x:,.0f}".replace(",", ".")))
    ax2.legend(frameon=True, facecolor="white", edgecolor="#DDDDDD", loc="upper left")

    for ax in (ax1, ax2):
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle("Evolução mensal de Pix e TED no Brasil (nov/2020 a set/2026)", fontsize=14, fontweight="bold", y=0.98)
    fig.text(0.01, 0.01, f"Fonte: Banco Central do Brasil (APIs Olinda Pix_DadosAbertos e MPV_DadosAbertos). Coleta em {DATA_REF_TXT}.", fontsize=9, color="#555555")

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    arq_png = PASTA_SAIDAS / f"grafico_pix_vs_ted_{STAMP}.png"
    fig.savefig(arq_png, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print("Gráfico salvo em:", arq_png)


if __name__ == "__main__":
    executar()
