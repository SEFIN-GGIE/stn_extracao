import pandas as pd
from bcb import sgs
import deflatebr as dbr


def processamento(df):
    last_ipca = sgs.get(('IPCA', 433), last=1)
    last_ipca_data = last_ipca.index.strftime('%Y-%m').to_list()[0]

    df = df.assign(
        valor_deflac=dbr.deflate(
            nominal_values=df.valor,
            nominal_dates=df.data,
            real_date=last_ipca_data
        )
    )

    formato = '%Y-%m-%d'
    df['data'] = pd.to_datetime(df.data, format=formato)
    df['ano_bimestre'] = df['ano'].astype('str') + ' ' + df['bimestre'].astype('str') + 'º bi'
    df['mes_ano'] = df['data'].dt.strftime('%b-%y')
    df['mes'] = df['data'].dt.strftime('%b')

    return df
