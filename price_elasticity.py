import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import random
from scipy.stats import linregress


def price_elasticity_per_brand(df, brand, elasticity_borders = [-50,0]):
    df_brand = df[(df['ecom_brand'] == brand) & (df['ecom_event_action'] == 'd_покупка')][['ecom_variant','ecom_id','ecom_price', 'ecom_qty']]
    df_brand['price_change_pst'] = (df_brand['ecom_price'] - df_brand.groupby(['ecom_id','ecom_variant'])['ecom_price'].transform('mean'))/df_brand['ecom_price']
    df_brand['qty_change_pst'] = (df_brand['ecom_qty'] - df_brand.groupby(['ecom_id','ecom_variant'])['ecom_qty'].transform('mean'))/df_brand['ecom_qty']
    df_brand_changed = df_brand.groupby('price_change_pst')['ecom_qty'].sum().reset_index() ##[lambda x: x['qty_change_pst'] !=0]\
     
    elastic_koeff = linregress(df_brand_changed['price_change_pst'], df_brand_changed['ecom_qty']).slope          
    
    result_dict = {'elastic_koeff':elastic_koeff}
    return result_dict


def fake_purchase_calc(df, brand, margin_range=[0.3,0.5], min_margin=0.3, margin_search_range = 0.2):
    """ Assign possible purchase price according to margin range and calculation optimal price for every product"""
    # calculate fake possible purchase prices
    df_brand = df[(df['ecom_brand'] == brand) & (df['ecom_event_action'] == 'd_покупка')][['ecom_variant','ecom_id','ecom_price', 'ecom_qty']]
    mean_price_dict = df_brand.groupby(['ecom_variant'])['ecom_price'].mean().to_dict()
    purch_price_dict = {ecom_variant: mean_price*(1 - random.uniform(margin_range[0], margin_range[1])) for 
                       ecom_variant, mean_price in mean_price_dict.items()}
    df_brand['purch_price'] = df_brand['ecom_variant'].map(purch_price_dict)
    df_brand['margin_now'] = ((df_brand.groupby(['ecom_variant'])['ecom_price'].transform('mean')- df_brand['purch_price'])/
                              df_brand.groupby(['ecom_variant'])['ecom_price'].transform('mean')
                             )
    return df_brand
    
    
def price_optimization(df, brand, margin_search_range = 0.1, step = 0.02,min_margin=0.3):
    """ find the best prices, maximazin margin - based on elasticity """
    
    elast_koeff = df.pipe(price_elasticity_per_brand, brand)['elastic_koeff'] # calculate elasticity
    dft = df.pipe(fake_purchase_calc, brand) # assign purchase price with default margin in range [0.3,0.5]
    dft = dft.groupby('ecom_variant').agg({'ecom_price':'mean', 'ecom_qty':'sum', 'purch_price':'mean','margin_now':'mean'})

    best_price_dict={}
    forecast_qty_dict={}
    forecast_extra_marg_dict={}
    for k, row in dft.iterrows():
        # find possible relative price range with min_margin limitations
        relative_price_range = [x for x in np.arange(-margin_search_range,margin_search_range,step) if 
                              (row['ecom_price']*(1 + x) - row['purch_price'])/row['ecom_price'] >= min_margin]
        margin_rub_now = row['margin_now']*row['ecom_price']

        new_price_result_dict={}
        for rel_pr in relative_price_range:
            price_new = row['ecom_price']*(1 + rel_pr)
            margin_rub_new = price_new - row['purch_price']
            ecom_qty_new = row['ecom_qty'] + elast_koeff*rel_pr
            margin_delta = ecom_qty_new * margin_rub_new - row['ecom_qty'] * margin_rub_now 
            if margin_delta> 0 :
                new_price_result_dict[rel_pr] = margin_delta
        if len(new_price_result_dict) > 0:
            best_price_change = [k for k,v in new_price_result_dict.items() if v == np.max(list(new_price_result_dict.values()))][0]

            best_price = int(row['ecom_price']*(1 + best_price_change))
            best_price_dict[k] = best_price
            forecast_qty_dict[k] = row['ecom_qty'] + elast_koeff*(best_price - row['ecom_price'])/row['ecom_price']
            forecast_extra_marg_dict[k] = forecast_qty_dict[k]*(best_price - row['purch_price']) - row['ecom_qty']*(row['ecom_price'] - row['purch_price']) 
    dft['best_price'] = dft.index.map(best_price_dict)
    dft['margin_new'] = (dft['best_price'] - dft['purch_price'])/dft['best_price']
    dft['qty_forecast'] = dft.index.map(forecast_qty_dict)
    dft['extra_margin_forecast'] = dft.index.map(forecast_extra_marg_dict)
    dft = dft.assign(extra_sales = lambda x: x['qty_forecast']*x['best_price']-x['ecom_price']*x['ecom_qty'])
    return dft
    
    
def price_opt_totals(price_opt_df):
    """Calculate totals for df with optimazed prices"""
    total = price_opt_df.assign(sales_now = lambda x: x['ecom_price']*x['ecom_qty'])\
    .assign(sales_new = lambda x: x['qty_forecast']*x['best_price'])\
    .assign(extra_sales = lambda x: x['sales_new'] - x['sales_now'])\
    .dropna().sum().to_frame().T[['ecom_qty','qty_forecast','sales_now','sales_new','extra_sales','extra_margin_forecast']]
    return total