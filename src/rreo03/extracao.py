from sys import platform
from datetime import datetime
import os
import math
import re
import requests
import time
import pandas as pd
from pandas.tseries.offsets import DateOffset
from processamento import processamento
import fire

DIR_EXTRACOES = "src/rreo03/dados/"

QUERY = "http://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo?" \
        "an_exercicio={0}&nr_periodo={1}&co_tipo_demonstrativo=RREO&" \
        "no_anexo=RREO-Anexo%2003&id_ente={2}"

df_municipio = pd.read_excel(
    "src/dicionarios/cd_municipio.xlsx",
    sheet_name="Sheet1",
    usecols=["cod_completo", "capital", "nome_municipio", "cd_uf"]
)


def transformacao(info, corte):
    df = pd.DataFrame(info)
    df.query("coluna != @corte", inplace=True)

    df['data_ref'] = pd.to_datetime(
        df.exercicio.astype('str') + '-' + (df.periodo * 2).astype(
            'str'),
        format='%Y-%m')

    df["lag"] = df.coluna.apply(
        lambda x: re.findall(r'(\d+)', x)[0] if x != '<MR>' else 0). \
        astype("int64")

    df["data"] = [row.data_ref + DateOffset(months=(row['lag'] * -1))
                  for _, row in df.iterrows()]

    df = df[['data', 'demonstrativo', 'uf',
             'cod_ibge', 'populacao', 'coluna', 'cod_conta',
             'conta', 'valor']]

    df = df.assign(mes_num=df.data.dt.month)
    df = df.assign(ano=df.data.dt.year)
    df['bimestre'] = df.mes_num.apply(lambda x: math.ceil(x / 2))
    df["cd_municipio"] = df.cod_ibge.astype('str').str[:6].astype("int64")

    df.rename(
        columns={
            "cod_ibge": "cod_completo",
        }, inplace=True)

    result = pd.merge(df, df_municipio, on="cod_completo", how="left")
    return result


def extracao(anos, bimestres):
    cd_mun = df_municipio.cod_completo.to_list()
    mun = df_municipio.nome_municipio.to_list()

    for ano in anos:
        coluna_corte = [
            'TOTAL (ÚLTIMOS 12 MESES)', f"PREVISÃO ATUALIZADA {ano}"
        ]

        for bimestre in bimestres:
            for cd_municipio, municipio in zip(cd_mun, mun):
                print(f"Extraindo {municipio} - {bimestre} - {ano}.csv")
                url = QUERY.format(ano, bimestre, cd_municipio)
                print(url)
                r = requests.get(url)
                time.sleep(2)
                base = r.json()
                info = base['items']

                if info:
                    result = transformacao(info, coluna_corte)
                    result.to_csv(
                        f"{DIR_EXTRACOES}{municipio}_{ano}{bimestre}.csv",
                        index=False,
                        sep=";")


def exporta_dataset(ano, formato):

    if formato not in ['parquet', 'csv']:
        raise Exception(
            "Formato não especificado. Escolha entre 'parquet' e 'csv."
        )
    arquivos = os.listdir(DIR_EXTRACOES)
    df_list = list()

    for arquivo in arquivos:
        if arquivo.endswith(".csv"):
            if int(arquivo[-9:-5]) in ano:
                df = pd.read_csv(f"{DIR_EXTRACOES}{arquivo}",
                                 sep=";",
                                 parse_dates=["data"])
                df_list.append(df)

    df = pd.concat(df_list)

    # removendo datas repetidas - pegando valor de acordo com sua ultima data
    rreo = df.query("coluna == ['<MR>', '<MR-1>']")
    """
    grp = df.groupby(["data", "uf"], as_index=False)["bimestre"].max()

    rreo = pd.merge(df, grp, on=['data', 'uf', 'bimestre'], how='inner')
    rreo.sort_values("data", inplace=True)
    rreo.query("data.dt.year == @ano", inplace=True)
    """

    df_result = processamento(rreo)
    df_result.sort_values(['ano', 'mes_num'], inplace=True)

    if platform == "linux":
        dir_arquivo = "./src/rreo03"
    else:
        dir_arquivo = "src/rreo03"

    if formato == "parquet":
        df_result.to_parquet(
            f"{dir_arquivo}/rreo_anexo3_stn.parquet.gzip",
            index=False,
            compression="gzip"
        )
    elif formato == "csv":
        df_result.to_csv(
            f"{dir_arquivo}/rreo_anexo3_stn.csv",
            index=False,
            sep=";",
            encoding="utf-8"
        )

    return print(
        f"Arquivo {formato} na pasta {dir_arquivo}"
    )


def executa_extracao(ano_ext=None, bimestre_ext=None, ano_agreg=None,
                     formato="parquet"):

    ultimo_ano = datetime.now().year

    if ano_ext is None:
        ano_ext = [ultimo_ano]
    if bimestre_ext is None:
        bimestre_ext = [1]
    if ano_agreg is None:
        ano_agreg = list(range(2015, ultimo_ano + 1))

    extracao(ano_ext, bimestre_ext)
    exporta_dataset(ano_agreg, formato)


if __name__ == "__main__":
    # no terminal
    # python src\rreo03\extracao.py --ano_ext=[2022] --bimestre_ext=[4]
    fire.Fire(executa_extracao)
