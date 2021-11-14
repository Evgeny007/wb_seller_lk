import numpy as np
import pandas as pd

def monthly_sales_dynamic(df, brand_name = 'Бренд № 1'):
    """ Calculate monthly statistics and dynamics"""
    monthly_data = total_df.loc[brand_name].groupby(level=1).sum()[['sales_pcs','sales_summ','returned_pcs','returned_summ']]
    monthly_data.columns = ['продажи_шт', 'продажи_руб', 'возвраты_шт','возвраты_руб']
    monthly_data_ch = monthly_data.pipe(lambda x: (100*(x - x.shift(1))/x).round(0).astype(str)+'%').replace('nan%','')
    monthly_data_ch.columns = [x + '_изм%' for x in monthly_data.columns]
    
    return pd.concat([monthly_data, monthly_data_ch], axis=1)

def best_worst_sellers(df,brand_name = 'Бренд № 1', month=11, top_n=5):
    brand_df = total_df.loc[brand_name].loc[(slice(None),month),:]
    brand_df.columns = ['продажи_шт', 'продажи_руб','возвраты_шт','возвраты_руб','склад_шт', 'склад_руб' ]
    best_sellers = brand_df.sort_values(by='продажи_руб',ascending=False).iloc[0:top_n,:].assign(тип = 'бестселлеры')
    worst_sellers = brand_df.sort_values(by='продажи_руб',ascending=False).iloc[-top_n:,:].assign(тип = 'менее_продаваемые')
    
    return pd.concat([best_sellers,worst_sellers])


def overstock(df, brand, month=11, max_turnover=2, topN=10):
    """ Calculate top N goods with worst turnover"""
    
    brand_df = df.loc[brand].loc[(slice(None),month),:]
    brand_df['Оборачиваемость_мес'] = (brand_df['stock_pcs']/ (brand_df['sales_pcs'] + 0.01)).round(1)
    brand_df['Излишки_шт'] = ((brand_df['Оборачиваемость_мес'] - max_turnover).clip(lower=0)*brand_df['sales_pcs']).astype(int)
    brand_df['Излишки_руб'] = ((brand_df['Оборачиваемость_мес'] - max_turnover).clip(lower=0)*brand_df['sales_summ']).astype(int)
    brand_df = brand_df.sort_values(by= 'Излишки_руб', ascending = False).head(topN)\
    [['sales_pcs','sales_summ', 'stock_pcs','Оборачиваемость_мес','Излишки_шт','Излишки_руб']]
    
    brand_df_total = brand_df.sum().to_frame().T.assign(nm_id_n = '').assign(month = 'Итого')\
    .set_index(['nm_id_n','month']).assign(Оборачиваемость_мес = lambda x: (x['stock_pcs']/ (x['sales_pcs'] + 0.01)).round(1))
    
    return brand_df.append(brand_df_total).astype(int)


def out_of_stock(df, brand, month=11, min_turnover=0.6, topN=10):
    """ Calculate top N goods with worst turnover"""
    
    brand_df = df.loc[brand].loc[(slice(None),month),:].astype(int)
    brand_df['Оборачиваемость_мес'] = (brand_df['stock_pcs']/ (brand_df['sales_pcs'] + 0.01)).round(1)
    brand_df = brand_df.sort_values(by= 'Оборачиваемость_мес').head(topN)\
    [['sales_pcs','sales_summ', 'stock_pcs','Оборачиваемость_мес']]\
 #   [lambda x: x['Оборачиваемость_мес'] < min_turnover]
    
    brand_df_total = brand_df.sum().to_frame().T.assign(nm_id_n = '').assign(month = 'Итого')\
    .set_index(['nm_id_n','month']).assign(Оборачиваемость_мес = lambda x: (x['stock_pcs']/ (x['sales_pcs'] + 0.01)).round(1))
    result_df  = brand_df.append(brand_df_total).rename(columns={'sales_pcs': 'продажи_шт','sales_summ':'продажи_руб','stock_pcs':'склад_шт'}) 
    
    return result_df

def returns(df, brand, month=11, top_n=5):
    brand_df_returns = df.reset_index()[lambda x: x['month'] == month]\
    .assign(return_ratio = lambda x: (x['returned_pcs']/(x['sales_pcs']+0.01)).round(1))\
    .assign(return_ratio_for_sort = lambda x: x['return_ratio']-x['sales_pcs'])\
    .sort_values(by = 'return_ratio_for_sort')[['sales_pcs', 'sales_summ','returned_pcs','return_ratio']]
    result = pd.concat([brand_df_returns.head(top_n).assign(лучшие_худшие = f'лучшие_{top_n}'),
                        brand_df_returns.tail(top_n).assign(лучшие_худшие = f'худшие_{top_n}')])\
    .rename(columns = {'sales_pcs':'продажи_шт','sales_summ':'продажи_руб','returned_pcs':'возвраты_руб','return_ratio':'коэфф_возврата'})
    
    return result

def supply_and_remove_plan(df, brand_name, max_turnover_weeks = 5):
    result = df.loc[brand_name].reset_index()[lambda x: x['month'] == 11][['nm_id_n','sales_pcs','stock_pcs']]\
    .assign(недельный_спрос_шт = lambda x: (x['sales_pcs']/1.2).astype(int))\
    .assign(мин_запас_шт = lambda x: x['недельный_спрос_шт'])\
    .assign(макс_запас_шт = lambda x: x['недельный_спрос_шт']*3)\
    .assign(необходимо_привезти_шт = lambda x: (x['макс_запас_шт'] - x['stock_pcs']).clip(lower=0).astype(int))\
    .assign(обор_ть_сейчас_недели = lambda x: (x['stock_pcs']/(x['недельный_спрос_шт']+1)).round(1))\
    .assign(необходимо_забрать_шт = lambda x: ((x['обор_ть_сейчас_недели'] - max_turnover_weeks).clip(lower=0)*x['недельный_спрос_шт'])\
            .astype(int)).sort_values(by='необходимо_привезти_шт', ascending=False)
    
    return result