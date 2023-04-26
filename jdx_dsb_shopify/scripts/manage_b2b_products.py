import logging

import click
import pandas as pd
from datetime import datetime

import snowflake
from jdx_utils.api.secrets import get_secret_from_sm
from jdx_utils.util import log_start_stop, log_runtime
from snowflake.snowpark import Session

from jdx_dsb_shopify.globals import SNOWFLAKE_SECRET_NAME, SNOWFLAKE_WH, SHOPIFY_SECRET_NAME
from jdx_dsb_shopify.util.logging import setup_logging_env
from jdx_dsb_shopify.util.shopify_utils import ShopifyHelper

logger = logging.getLogger(__name__)

@log_start_stop
def get_last_variant_update(shop_env):
    snowflake_secrets = get_secret_from_sm(SNOWFLAKE_SECRET_NAME)
    connection_parameters = {
        "account": snowflake_secrets['SNOWFLAKE_ACCOUNT'],
        "user": snowflake_secrets['SNOWFLAKE_USER'],
        "password": snowflake_secrets['SNOWFLAKE_PASSWORD'],
        "warehouse": 'COMPUTE_WH',
        "database": 'JDX_PLATFORM',
        "schema": 'ANALYTICS',
    }

    session = Session.builder.configs(connection_parameters).create()
    query = f'''
        SELECT UPDATE_TS AS LAST_MODIFIED
        FROM JDX_PLATFORM.ANALYTICS.SHOPIFY_B2B_PRODUCTS
        WHERE ENV = '{shop_env}'
        ORDER BY UPDATE_TS DESC
        LIMIT 1
    '''

    try:
        data = session.sql(query).collect()
        if len(data)>0:
            df = session.create_dataframe(data).to_pandas()
            last_modified_time = datetime.strptime(df['LAST_MODIFIED'][0], '%Y-%m-%d %H:%M:%S.%f')
            logger.info(f'Shopify B2B products were last modified at {last_modified_time}')
        else:
            last_modified_time = -1
    except snowflake.snowpark.exceptions.SnowparkSQLException:
        last_modified_time = -1
    return last_modified_time


@log_start_stop
def update_snowflake_shopify_b2b_products(
        df,
        dst_table_name: str = 'SHOPIFY_B2B_PRODUCTS',
        mode: str = 'append',
):
    df = df.copy()
    snowflake_secrets = get_secret_from_sm(SNOWFLAKE_SECRET_NAME)
    connection_parameters = {
        "account": snowflake_secrets['SNOWFLAKE_ACCOUNT'],
        "user": snowflake_secrets['SNOWFLAKE_USER'],
        "password": snowflake_secrets['SNOWFLAKE_PASSWORD'],
        "warehouse": SNOWFLAKE_WH,
        "database": 'JDX_PLATFORM',
        "schema": 'ANALYTICS'
    }

    session = Session.builder.configs(connection_parameters).create()
    full_dst_table_name = f"{connection_parameters['database']}.{connection_parameters['schema']}.{dst_table_name}"
    logger.info(f"Updating {full_dst_table_name} with {mode} mode...")
    df['update_ts'] = str(datetime.now())
    df['env']=get_secret_from_sm(SHOPIFY_SECRET_NAME)['SHOP_ENV']
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
    df = df.sort_values('UPDATE_TS', ascending=False).groupby(['ACCOUNT_ID', 'PRODUCT_SHORT_NAME']).head(1)
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
    variant_df = price_df.query(f'product_short_name=="{product_short_name}"')
    if len(variant_df) >0:
        logger.info(f'{variant_df} variants to update price.')

        if len(b2b_product)>1:
            raise ValueError(
                'Found duplicate B2B products for the same product type.  '
                'There should only be one for each product_type.'
            )
        elif len(b2b_product)==1: # create a new product and delete the old product
            response = shopify_helper.create_b2b_products(
                variant_df=variant_df,
                status='active',
                product_short_name=product_short_name,
            )
            shopify_helper.delete_product(b2b_product[0]['id'])
            update_snowflake_shopify_b2b_products(pd.DataFrame(response.json()['product']['variants']))
            return response

        else: # create product
            response = shopify_helper.create_b2b_products(
                variant_df=variant_df,
                status = 'active',
                product_short_name = product_short_name,
            )

            update_snowflake_shopify_b2b_products(pd.DataFrame(response.json()['product']['variants']))
            return response
    else:
        logger.info('No product variants to update.')



@log_start_stop
@log_runtime
@setup_logging_env
def main():
    # get latest price information from Snowflake
    price_df = get_latest_prices()
    last_price_update = datetime.strptime(price_df['update_ts'][0], '%Y-%m-%d %H:%M:%S.%f')
    shopify_helper = ShopifyHelper(SHOPIFY_SECRET_NAME)
    last_variant_update = get_last_variant_update(shopify_helper.shop_env)

    if last_variant_update==-1 or last_price_update>last_variant_update:  # there is a price update
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
    else:
        logger.info('No new price found.')



if __name__ == "__main__":
    main()