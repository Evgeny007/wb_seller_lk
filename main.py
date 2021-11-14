import numpy as np
import pandas as pd
import warnings
import random
from scipy.stats import linregress
from load_preprocess import load_data, df_preprocessing, data_type_processsing
from load_preproc_add_data import load_combine
from funel import brand_funel, products_funel, supplementary_goods_share
from supplementary_goods_share import load_combine
from price_elasticity import price_elasticity_per_brand, fake_purchase_calc
from price_elasticity import price_optimization, price_opt_totals
from some_stats import monthly_sales_dynamic, best_worst_sellers, overstock, out_of_stock
from some_stats import returns, supply_and_remove_plan



# load main df
df = load_data().pipe(df_preprocessing).pipe(data_type_processsing)

# calculate optimazed prices for brand = '7921BD71B70D139454324A845115B9E3'
price_opt_table = df.pipe(price_optimization, '7921BD71B70D139454324A845115B9E3')

# brand funel for brand = '7921BD71B70D139454324A845115B9E3'
brand_funel_table = brand_funel(df, '7921BD71B70D139454324A845115B9E3')

# funel for products of brand = '7921BD71B70D139454324A845115B9E3'
prod_fun_table = products_funel(df, brand='7921BD71B70D139454324A845115B9E3')

# calculate share of sales of supplementary products
suppl_table = supplementary_goods_share(df, brand='7921BD71B70D139454324A845115B9E3')

################################ tables with another data ###################

# load, preprocess and join data with sales, returns, stock
total_df = load_combine()

# info with monthly sales
monthly_sales = monthly_sales_dynamic(total_df, brand_name = 'Бренд № 1')

# bestsellers and outsiders
bw = best_worst_sellers(total_df,  month=11)

# overstock information
ver_table = overstock(total_df,'Бренд № 1')

# out of stock goods
oos_table = out_of_stock(total_df,'Бренд № 1')

# top returns (goods)
returns_table = returns(total_df, brand='Бренд № 3', month=11)

# supply planing

supply_table = supply_and_remove_plan(df, brand_name, max_turnover_weeks = 5)








