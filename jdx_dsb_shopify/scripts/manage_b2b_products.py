import logging

import click
import pandas as pd
from jdx_utils.api.secrets import get_secret_from_sm
from jdx_utils.util import log_start_stop, log_runtime
from snowflake.snowpark import Session

from jdx_dsb_shopify.globals import SNOWFLAKE_SECRET_NAME
from jdx_dsb_shopify.util.shopify_utils import ShopifyHelper

logger = logging.getLogger(__name__)

@log_start_stop
def update_snowflake_shopify_b2b_products(
        df,
        dst_table_name: str = 'SHOPIFY_B2B_PRODUCTS',
        mode: str = 'append',
):
    df = df.copy()
    connection_parameters = {
        "account": snowflake_secrets['SNOWFLAKE_ACCOUNT'],
        "user": snowflake_secrets['SNOWFLAKE_USER'],
        "password": snowflake_secrets['SNOWFLAKE_PASSWORD'],
        "warehouse": SNOWFLAKE_WH,
        "database": 'JDX_MARKETING',
        "schema": 'ANALYTICS'
    }

    session = Session.builder.configs(connection_parameters).create()
    full_dst_table_name = f"{dev_sf_conn['database']}.{dev_sf_conn['schema']}.{dst_table_name}"
    logger.info(f"Updating {full_dst_table_name} with {mode} mode...")
    df.columns = [c.upper() for c in df.columns]
    sf_df = session.create_dataframe(df)
    sf_df.write.save_as_table(dst_table_name, mode=mode)
    session.close()


def get_latest_prices():
    snowflake_secrets = get_secret_from_sm(SNOWFLAKE_SECRET_NAME)
    connection_parameters = {
        "account": snowflake_secrets['SNOWFLAKE_ACCOUNT'],
        "user": snowflake_secrets['SNOWFLAKE_USER'],
        "password": snowflake_secrets['SNOWFLAKE_PASSWORD'],
        "warehouse": SNOWFLAKE_WH,
        "database": 'JDX_PLATFORM',
        "schema": 'ANALYTICS',
    }
    session = Session.builder.configs(connection_parameters).create()
    query = f'''
            SELECT *
            FROM JDX_PLATFORM.ANALYTICS.PRICE
        '''
    df = session.create_dataframe(session.sql(query).collect()).to_pandas()
    df = df.sort_values('UPDATE_TS', ascending=False).groupby('ACCOUNT_ID').head(1)
    df.columns = [c.lower() for c in df.columns]
    return df

def update_product_pricing(
        price_df: pd.DataFrame,
        product_short_name: str,
        shopify_helper: ShopifyHelper,
):
    if product_short_name not in ('birch', 'hazel_basic', 'hazel_plus'):
        raise ValueError(f'Unknown product short names: {product_short_name}')

    # get b2b products
    r = shopify_helper.get_products()
    b2b_product = [
        product_info for product_info in r.json()['products']
        if 'b2b' in product_info['tags'] and product_info['product_type'] == product_short_name
    ]

    if len(b2b_product)>1:
        raise ValueError(
            'Found duplicate B2B products for the same product type.  '
            'There should only be one for each product_type.'
        )
    elif len(b2b_product)==1: # create a new product and delete the old product
        response = shopify_helper.create_b2b_products(
            variant_df=price_df.query(f'product_short_name=="{product_short_name}"'),
            status='active',
            product_short_name=product_short_name,
        )
        shopify_helper.delete_product(b2b_product[0]['id'])
        update_snowflake_shopify_b2b_products(pd.DataFrame(response.json()['product']['variants'])
        return response

    else: # create product
        response = shopify_helper.create_b2b_products(
            variant_df=price_df.query(f'product_short_name=={product_short_name}'),
            status = 'active',
            product_short_name = product_short_name,
        )

        update_snowflake_shopify_b2b_products(pd.DataFrame(response.json()['product']['variants'])
        return response



@log_start_stop
@log_runtime
@click.command()
@click.option("--update", is_flag=True, show_default=True, default=True, help="update product variants")
def main():
    # get latest price information from Snowflake
    price_df = get_latest_prices()

    shopify_helper = ShopifyHelper(SHOPIFY_SECRET_NAME)

    # Update FST B2B product
    logger.info('UPDATE: FST B2B Product')
    update_product_pricing(
        price_df=price_df,
        product_short_name='birch',
        shopify_helper=shopify_helper
    )

    logger.info('UPDATE: NIPS-BASIC B2B Product')
    update_product_pricing(
        price_df=price_df,
        product_short_name='hazel_basic',
        shopify_helper=shopify_helper
    )

    logger.info('UPDATE: NIPS-Hazel B2B Product')

    update_product_pricing(
        price_df=price_df,
        product_short_name='hazel_plus',
        shopify_helper=shopify_helper
    )







if __name__ == "__main__":
    main()