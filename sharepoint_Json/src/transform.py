import pandas as pd

def transform(dataframes:dict):
    table = {}
    table['time_dim']=dataframes["time_dimension.json"]
    table['product_dim'] = dataframes['product_dimension.json']
    table['store_dim'] = dataframes['store_dimension.json']
    
    table['sales_fact'] = dataframes['sales_fact.json']

    sales_dim = dataframes['sales_dimensions.json']

    #when we want to fetch multiple values we wrap into square braces
    table['suppliers_dim'] = sales_dim[["supplier_id","supplier_name","contact_email","supplier_country","reliability_score"]].drop_duplicates()
    table['region_dim'] = sales_dim[["region_id","region_name","region_country","regional_manager"]].drop_duplicates()
    table['promotion_dim'] = sales_dim[["promotion_id","promotion_name","discount_percentage","start_date","end_date"]].drop_duplicates()

    # fact = dataframes['sales_fact.json'].copy()
    # prod_lookup = table["product_dim"][['product_id','supplier_id']].drop_duplicates()
    # fact = fact.merge(prod_lookup,on='product_id',how='left')
    # store_lookup = table['store_dim'][['store_id','region_id']].drop_duplicates()
    # fact = fact.merge(store_lookup,on='store_id',how='left')
    # table['sales_fact'] = fact

    return table