import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import random


def brand_funel(df, brand):
    funel_df = df[lambda x: x['ecom_brand'] == brand].pivot_table(index = ['utc_event_week'], 
                                                                  columns = ['ecom_event_action'], values=['ecom_qty','ecom_grand_total'] ,aggfunc = 'sum',)\
    .stack(0).reset_index()\
    .assign(тип_воронки = lambda x: x['level_1'].map({'ecom_grand_total':'выручка', 'ecom_qty':'заказы'}))\
    .set_index(['тип_воронки','utc_event_week']).sort_index()[['a_просмотры','d_покупка']].astype(int)\
    .assign(конверсия_покупки = lambda x: x['d_покупка']/x['a_просмотры'])\
    .assign(результат = lambda x: np.where(x['конверсия_покупки'] < x.groupby(level=0)['конверсия_покупки'].transform('quantile',0.33), 'низкая коверсия',
                                               np.where(x['конверсия_покупки'] > x.groupby(level=0)['конверсия_покупки']
                                                        .transform('quantile',0.66), 'высокая конверсия', 'средняя конверсия')
                                              ))\
    .assign(конверсия_покупки = lambda x: (100*x['конверсия_покупки']).round(1).astype(str)+'%')\
    .rename(columns = {'a_просмотры': 'воронка_просмотры','d_покупка':'воронка_покупки'})
    
    return funel_df

def products_funel(df, brand='7921BD71B70D139454324A845115B9E3'):
    funel_df = df[lambda x: x['ecom_brand'] == brand].pivot_table(index = ['ecom_id','utc_event_week'], 
                                                                  columns = ['ecom_event_action'], values=['ecom_qty','ecom_grand_total'] ,aggfunc = 'sum', )\
    .stack(0).reset_index()\
    .assign(тип_воронки = lambda x: x['level_2'].map({'ecom_grand_total':'выручка', 'ecom_qty':'заказы'}))\
    .set_index(['ecom_id','тип_воронки','utc_event_week'])\
    [lambda x: x['a_просмотры'].notna()]\
    .sort_index()[['a_просмотры','d_покупка']].astype(int)\
    .assign(конверсия_покупки = lambda x: x['d_покупка']/x['a_просмотры'])\
    .assign(результат = lambda x: np.where(x['конверсия_покупки'] < x.groupby(level=0)['конверсия_покупки'].transform('quantile',0.33), 'низкая коверсия',
                                               np.where(x['конверсия_покупки'] > x.groupby(level=0)['конверсия_покупки'].transform('quantile',0.66), 'высокая конверсия', 'средняя конверсия')
                                              ))\
    .assign(конверсия_покупки = lambda x: (100*x['конверсия_покупки']).round(1).astype(str)+'%')\
    .rename(columns = {'a_просмотры': 'воронка_просмотры','d_покупка':'воронка_покупки'})
    
    return funel_df


def supplementary_goods_share(df, brand):
    """ Find share of goods without 'viw_item' but with sales """
    brand_df = df[df['ecom_brand'] == brand]
    
    pcs_share = brand_df.pivot_table(index = ['ecom_id','utc_event_week'], columns = ['ecom_event_action'], values=['ecom_qty'] ,aggfunc = 'sum', )['ecom_qty']\
    .assign(addon = lambda x: np.where(x['a_просмотры'].isna(),'addon','main'))\
    .groupby(['utc_event_week','addon'])['d_покупка'].sum()\
    .pipe(lambda x: x.loc[(slice(None),'addon')]/x.loc[(slice(None),'main')]).rename('доп_товары_доля_шт').to_frame()\
    .assign(доп_товары_доля_шт = lambda x: (100*x['доп_товары_доля_шт']).round(1).astype(str)+'%')

    rub_share = brand_df.pivot_table(index = ['ecom_id','utc_event_week'], columns = ['ecom_event_action'], values=['ecom_grand_total'] ,aggfunc = 'sum', )['ecom_grand_total']\
    .assign(addon = lambda x: np.where(x['a_просмотры'].isna(),'addon','main'))\
    .groupby(['utc_event_week','addon'])['d_покупка'].sum()\
    .pipe(lambda x: x.loc[(slice(None),'addon')]/x.loc[(slice(None),'main')]).rename('доп_товары_доля_руб').to_frame()\
    .assign(доп_товары_доля_руб = lambda x: (100*x['доп_товары_доля_руб']).round(1).astype(str)+'%')
    
    return pd.concat([pcs_share, rub_share], axis=1)