# Trabalho Integrador – Pix e TED

Código principal: [`trabalho_pix_ted.py`](trabalho_pix_ted.py)

Documento completo: [`RELATORIO.md`](RELATORIO.md)

## Instalação

```bash
pip install -r requirements.txt
```

## Execução

O código foi preparado para o ambiente `/content` do Google Colab. Para uso local, execute:

```bash
python trabalho_pix_ted.py
```

Por padrão, os dados brutos são gravados em `raw/` e os resultados em `saidas/`.

## Saídas

- `raw/pix_mensal/pix_AAAAMM.json.gz`
- `raw/ted_bruto_<timestamp>.json.gz`
- `saidas/pix_vs_ted_mensal_<timestamp>.csv`
- `saidas/grafico_pix_vs_ted_<timestamp>.png`
