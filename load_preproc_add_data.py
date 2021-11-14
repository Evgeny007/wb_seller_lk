import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import random


def preprocess(df, agg_func = 'sum'):
    df['pk_date_dt'] = pd.to_datetime(df['pk_date_dt'])
    df['month'] = df['pk_date_dt'].dt.month
    return df.groupby(['brand_name_n','nm_id_n','month'])[['cnt','summ']].agg(agg_func)


def load_combine():
    df_ret = pd.read_csv('/home/tokarev_ev/Документы/lk_seller/hack/returns_data_result_202111122337.csv')
    df_sales = pd.read_csv('/home/tokarev_ev/Документы/lk_seller/hack/sales_data_result_202111122342.csv')
    df_stock = pd.read_csv('/home/tokarev_ev/Документы/lk_seller/hack/stock_data_result_202111122356.csv')

    df_ret = df_ret.pipe(preprocess).rename(columns={'cnt':'returned_pcs', 'summ':'returned_summ'})
    df_sales = df_sales.pipe(preprocess).rename(columns={'cnt':'sales_pcs', 'summ':'sales_summ'})
    df_stock = df_stock.pipe(preprocess, 'mean').rename(columns={'cnt':'stock_pcs', 'summ':'stock_summ'})

    total_df = pd.concat([df_sales, df_ret,df_stock], axis=1).fillna(0).astype(int)
    
    return total_df