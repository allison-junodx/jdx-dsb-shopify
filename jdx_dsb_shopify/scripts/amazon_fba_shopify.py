import logging
import os

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

def get_amazon_fba_orders(
        product_id,
        first_name,
        last_name,
        email,
        account_address

):
    if os.environ['ENV'] == 'dev':
        test_flag = True
    else:
        test_flag = False

    return {
        "order": {
            "line_items": [
                {
                    "product_id": product_id,
                    "quantity": 1,
                }
            ],
            "customer": {
                "first_name": first_name,
                "last_name": last_name,
                "email": email
            },
            "email": email,
            "phone": "858-201-7154",
            "tags": "Sync: Failed,",
            "billing_address": account_address,
            "shipping_address": account_address,
            "financial_status": "paid",
            "send_receipt": False,
            "send_fulfillment_receipt": False,
            'test': test_flag,
            "discount_codes": [{"code": "", "amount": "100.0", "type": "percentage"}]
        }
    }

@log_start_stop
@log_runtime
@setup_logging_env
def amazon_fba_shopify():
    shopify_helper = ShopifyHelper(SHOPIFY_SECRET_NAME)
    # get Amazon FBA orders from Google Sheet
    total_amazon_fba_orders = get_spreadsheet(
        spreadsheet_id=AMAZON_FBA_USER_SHEET_ID,
        range='Sheet1',
        creds=google_creds,
    )

    # create order based on amazon FBA user creation sheet
    # Only create order if STATUS='REGISTERED'
    new_orders = total_amazon_fba_orders.query('Status=="REGISTERED"')

    if len(new_orders)>0:
        logger.info(f'Found {len(new_orders)} orders to create.')
        shopify_order_names=list()
        shopify_order_ids = list()
        for order in new_orders.iterrows():
            first_name = order[1]['First Name']
            last_name = order[1]['Lat Name']
            email = order[1]['Email']
            product_id = FST_PRODUCT_ID
            account_address = {
                "first_name": first_name,
                "last_name": last_name,
                "address1": "11760 Sorrento Valley Rd Suite J",
                "phone": "858-201-7154",
                "city": "San Diego",
                "province": "California",
                "country": "US",
                "zip": "92122"
            }

            order_payload = get_b2b_orders(
                variant_id = variant_id,
                product_id = product_id,
                first_name = first_name,
                last_name = last_name,
                email = email,
                account_address = account_address
            )
            logger.info(f'Create order for {account_name} with email: {email}')
            r=shopify_helper.create_order(order_payload)
            if r.status_code in (200,201): #successfully created
                logger.info(f"Created shopify order: {r.json()['order']['name']}")
                shopify_order_names.append(r.json()['order']['name'])
                shopify_order_ids.append(r.json()['order']['id'])
            else:
                shopify_order_names.append('')
                shopify_order_ids.append('')

        shopify_order_created = pd.concat(
            [
                fuzzy_matched_df[fuzzy_matched_df_cols],
                pd.DataFrame(shopify_order_names, columns=['order_name']),
                pd.DataFrame(shopify_order_ids, columns=['order_id'])
            ], axis=1
        )

        # get inventory information
        inventory_df = get_spreadsheet(INVENTORY_SHEET_ID, range='Providers', creds=google_creds)[
            [
                'Kit_Code',
                'Device_ID',
                'ReturnShipping',
                'ExpDate'
            ]
        ]

        inventory_df = inventory_df.rename(columns={
            'Kit_Code': 'kit_code',
            'Device_ID':'sample_number',
            'ReturnShipping': 'return_tracking_number',
            'ExpDate': 'expiration_date'
        })

        inventory_df['kit_code'] = inventory_df['kit_code'].apply(lambda x: x.upper())

        shopify_order_created = shopify_order_created.merge(inventory_df, on='kit_code', how='left')
        update_cols = [
            'order_name',
            'first_name',
            'last_name',
            'email',
            'dob',
            'lmp',
            'kit_code',
            'sample_number',
            'return_tracking_number',
            'expiration_date',
            'order_submitted_at',
        ]
        response = append_df2gsheet(shopify_order_created[update_cols].fillna(''), google_creds, ORDER_CREATION_SHEET_ID)
        logger.info('Updated order creation report on Google drive:')
        logger.info(response)

        client = WebClient(token=SLACK_BOT_TOKEN)

        slack_channel_map = {
            'dev': '#dsb-slack-test',
            'prd': '#cs-x-dsb',
        }

        info_msg = f'I have created {len(shopify_order_created)} orders from Jotform to Shopify. \n'
        review_msg = f'Please review the google sheet along with additional information you need to update lab ' \
                     f'portal orders later on at https://docs.google.com/spreadsheets/d/{ORDER_CREATION_SHEET_ID}. \n'''
        update_msg = 'Once orders are synced over to the lab portal, please update the following information in lab ' \
                     'portal: kit_code, tracking_number, patient DoB, patient LMP, and patient chart. \n'

        msg = info_msg + review_msg + update_msg
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
        logger.info('No new Jotform orders found.')





if __name__ == "__main__":
    amazon_fba_shopify()