import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import random

def load_data():
    names = ['utc_event_time', 'utc_event_date', 'user_id', 'city_name', 'ecom_event_action', 'ecom_id', 'ecom_brand', 'ecom_variant', 'ecom_currency', 'ecom_price100', 'ecom_qty', 'ecom_grand_total100', 'os_manufacturer', 'device_type', 'traffic_src_kind', 'app_version', 'net_type', 'locale']
    df = pd.read_csv('/home/tokarev_ev/Документы/lk_seller/WB_hack_3.csv',names = names)
    return df
    
def df_preprocessing(df):
    # delete [''] symbols in some cols
    extrac_cols = ['ecom_event_action', 'ecom_id', 'ecom_brand', 'ecom_variant',
              'ecom_currency','ecom_price100', 'ecom_qty','ecom_grand_total100']
    for c in extrac_cols:
        df[c] = df[c].astype(str).str.replace('[','').str.replace(']','')

    for c in extrac_cols:
        df[c] = df[c].astype(str).str.replace("'",'').str.replace("'",'')
    
    # rows with multiple products in row explode to 1 product in 1 row
    multipurchase = df[df['ecom_id'].str.contains(',')] # select the rows
    cols_to_explode = ['ecom_event_action', 'ecom_id', 'ecom_brand', 'ecom_variant', 'ecom_currency', 'ecom_price100', 'ecom_qty','ecom_grand_total100']
    for x in cols_to_explode:
        multipurchase[x] = multipurchase[x].str.split(',') # tolist() them
    eploded_df  = pd.concat([multipurchase[x].explode() for x in cols_to_explode], axis = 1)
    usual_cols = ['utc_event_time', 'utc_event_date','user_id', 'city_name', 'os_manufacturer', 'device_type', 'traffic_src_kind', 'app_version', 'net_type', 'locale']
    multipurchase = multipurchase[usual_cols].merge(eploded_df, left_index = True, right_index = True)
    not_multipurch = df[~df['ecom_id'].str.contains(',')]
    clear_df = pd.concat([not_multipurch, multipurchase]).sort_values(by=['user_id','utc_event_date', 'utc_event_time']).reset_index(drop = True)
    
    return clear_df
    
    
def data_type_processsing(df, N_TOP_BRANDS_QTY = 10):
    df['utc_event_date'] = pd.to_datetime(df['utc_event_date'])
    df['utc_event_time'] = pd.to_datetime(df['utc_event_time'])
    df['ecom_price'] = df['ecom_price100'].astype(int)/100
    df['ecom_grand_total'] = df['ecom_grand_total100'].astype(int)/100
    df['ecom_qty'] = df['ecom_qty'].astype(int)
    df.drop(columns = ['ecom_price100', 'ecom_grand_total100'], inplace = True)
    df['city_name'] = df['city_name'].astype(str)
    
    df.drop(columns = ['net_type', 'app_version', 'traffic_src_kind', 'device_type',
                       'app_version','traffic_src_kind', 'device_type', 'os_manufacturer'], inplace = True)
    for c in df.select_dtypes('object').columns:
        df[c] = df[c].astype('category')
    df = df[lambda x: (x['ecom_currency'] == 'RUB') & (x['locale'] == 'ru')].drop(columns=['ecom_currency','locale'])
    
    # add feature 'week'
    df['utc_event_week'] = df['utc_event_date'].dt.week
    # rename funel stages
    map_evant_action = {'view_item':'a_просмотры', 'begin_checkout': 'b_начало_оформл', 'add_to_cart': 'c_корзина', 'purchase':'d_покупка'}
    df['ecom_event_action'] = df['ecom_event_action'].map(map_evant_action)
    
    #consider only top 10 brands
    top_brands = df['ecom_brand'].value_counts().head(N_TOP_BRANDS_QTY).index.tolist()
    not_top_brands = [x for x in df['ecom_brand'].unique() if x not in top_brands]
    # remove duplicates with same actions by user with same product
    dft = df.set_index('ecom_brand').sort_index().drop(not_top_brands).reset_index()\
    .drop_duplicates(['user_id','ecom_variant','ecom_event_action','utc_event_date'])
    
    # save resulted df
    dft.to_csv('/home/tokarev_ev/Документы/lk_seller/top_N_brands.csv', index=False)
    dft = pd.read_csv('/home/tokarev_ev/Документы/lk_seller/top_N_brands.csv')
    
    
        
    return dft
    
