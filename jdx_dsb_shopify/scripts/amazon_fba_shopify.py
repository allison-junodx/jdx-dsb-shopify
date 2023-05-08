import logging
import os

import click
import pandas as pd
from google.oauth2.service_account import Credentials
from jdx_slack_bot.util.google_drive_util import append_df2gsheet, get_spreadsheet
from jdx_utils.api.secrets import get_secret_from_sm, get_google_api_creds
from jdx_utils.util import log_start_stop, log_runtime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from snowflake.snowpark import Session

from jdx_dsb_shopify.globals import SNOWFLAKE_SECRET_NAME, SHOPIFY_SECRET_NAME, JOTFORM_SECRET_NAME, \
    JOTFORM_ID_HAZEL, JOTFORM_ID_BIRCH, INVENTORY_SHEET_ID, GOOGLE_API_SECRET_NAME, ORDER_CREATION_SHEET_ID, \
    SLACK_BOT_TOKEN, AMAZON_FBA_USER_SHEET_ID, FST_PRODUCT_ID
from jdx_dsb_shopify.scripts.jotform_integration import get_b2b_orders, get_latest_product_variant_info
from jdx_dsb_shopify.util.jotform_utils import JotformAPIClient, parse_form_names, parse_form_dates
from jdx_dsb_shopify.util.logging import setup_logging_env
from jdx_dsb_shopify.util.platform_db_utils import get_platformdb_conn_str
from jdx_dsb_shopify.util.shopify_utils import ShopifyHelper
from jdx_dsb_shopify.util.util import fuzzy_merge

logger = logging.getLogger(__name__)

creds = get_google_api_creds(GOOGLE_API_SECRET_NAME)
scopes = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]
google_creds = Credentials.from_service_account_info(creds, scopes=scopes)


@log_start_stop
@log_runtime
@click.command()
@click.option("--start_user_number", default=None, help="start_user_number")
@click.option("--batch_size", default=None, help="batch size")
def amazon_fba_shopify(
        start_user_number: int =None,
        batch_size: int =None
):
    shopify_helper = ShopifyHelper(SHOPIFY_SECRET_NAME)
    # get Amazon FBA orders from Google Sheet
    total_amazon_fba_orders = get_spreadsheet(
        spreadsheet_id=AMAZON_FBA_USER_SHEET_ID,
        range='Sheet1',
        creds=google_creds,
    )

    total_amazon_fba_orders['User Number'] = total_amazon_fba_orders['User Number'].astype(int)
    if start_user_number is not None:
        total_amazon_fba_orders = total_amazon_fba_orders.query(f'`User Number`>={int(start_user_number)}')

    if batch_size is not None:
        batch_size = int(batch_size)
        total_amazon_fba_orders = total_amazon_fba_orders.sort_values('User Number', ascending=True).head(batch_size)
    # get latest variant information
    shopify_secrets = get_secret_from_sm(SHOPIFY_SECRET_NAME)
    shop_env = shopify_secrets['SHOP_ENV']
    variant_df = get_latest_product_variant_info(shop_env)

    # create order based on amazon FBA user creation sheet
    # Only create order if STATUS='REGISTERED'
    new_orders = total_amazon_fba_orders.query('Status=="REGISTERED"')
    new_orders['account_name'] = 'Amazon FBA'
    new_orders['product_short_name'] = 'birch'

    if len(new_orders) > 0:
        logger.info(f'Found {len(new_orders)} orders to create.')
        new_orders['account_name_sku'] = new_orders['account_name'] + '|' + new_orders['product_short_name']
        variant_df['account_name_sku'] = variant_df['account_name'] + '|' + variant_df['product_short_name']
        fuzzy_matched_df = fuzzy_merge(
            new_orders, variant_df[['account_name_sku', 'id', 'product_id', 'price']],
            'account_name_sku', 'account_name_sku',
            threshold=90,
            how='left'
        ).rename(columns={'id': 'variant_id'})
        fuzzy_matched_df_cols = [
            'First Name',
            'Last Name',
            'Email',
            'variant_id',
            'product_id'
        ]

        shopify_order_names = list()
        shopify_order_ids = list()
        shopify_order_date=list()
        for order in fuzzy_matched_df.iterrows():
            account_name = order[1]['account_name']
            first_name = order[1]['First Name']
            last_name = order[1]['Last Name']
            email = order[1]['Email']
            variant_id = order[1]['variant_id']
            product_id = order[1]['product_id']
            account_address = {
                "first_name": first_name,
                "last_name": last_name,
                "company": account_name,
                "address1": "11760 Sorrento Valley Rd Suite J",
                "phone": "858-201-7154",
                "city": "San Diego",
                "province": "California",
                "country": "US",
                "zip": "92122"
            }

            order_payload = get_b2b_orders(
                variant_id=variant_id,
                product_id=product_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                account_address=account_address
            )
            logger.info(f'Create order for {account_name} with email: {email}')
            r = shopify_helper.create_order(order_payload)
            if r.status_code in (200, 201):  # successfully created
                logger.info(f"Created shopify order: {r.json()['order']['name']}")
                shopify_order_names.append(r.json()['order']['name'])
                shopify_order_ids.append(r.json()['order']['id'])
                shopify_order_date.append(r.json()['order']["created_at"])
            else:
                shopify_order_names.append('')
                shopify_order_ids.append('')
                shopify_order_date.append('')

        shopify_order_created = pd.concat(
            [
                fuzzy_matched_df[fuzzy_matched_df_cols],
                pd.DataFrame(shopify_order_names, columns=['order_name']),
                pd.DataFrame(shopify_order_ids, columns=['order_id']),
                pd.DataFrame(shopify_order_ids, columns=['order_created_at']),
            ], axis=1
        )


        update_cols = [
            'order_name',
            'First Name',
            'Last Name',
            'Email',
            'order_created_at',
        ]
        response = append_df2gsheet(
            shopify_order_created[update_cols].fillna(''),
            google_creds,
            ORDER_CREATION_SHEET_ID,
            sheet_name='Orders',
        )
        logger.info('Updated order creation report on Google drive:')
        logger.info(response)

        client = WebClient(token=SLACK_BOT_TOKEN)

        slack_channel_map = {
            'dev': '#dsb-slack-test',
            'prd': '#cs-x-dsb',
        }

        info_msg = f'I have created {len(shopify_order_created)} Amazon FBA orders in Shopify. \n'
        review_msg = f'Please review the google sheet along with additional information you need to update lab ' \
                     f'portal orders later on at https://docs.google.com/spreadsheets/d/{AMAZON_FBA_USER_SHEET_ID}. \n'''

        msg = info_msg + review_msg
        try:
            result = client.chat_postMessage(
                channel=slack_channel_map[os.environ['ENV']],
                text=msg
            )
            # Log the result
            logger.info(result)
        except SlackApiError as e:
            logger.error(f"Error posting the message: {e}")


        # Send slack notification and update

    else:
        logger.info('No new Amazon FBA orders created.')





if __name__ == "__main__":
    amazon_fba_shopify()