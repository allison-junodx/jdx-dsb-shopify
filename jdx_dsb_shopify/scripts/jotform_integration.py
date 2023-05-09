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
    SLACK_BOT_TOKEN
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

def parse_hazel_product(x):
    if 'plus' in x.lower():
        return 'hazel_plus'
    else:
        return 'hazel_basic'

def pull_orders_from_jotform(
        form_id,
        cols: list,
        form_statuses: list = None,
):
    jotform_api_key = get_secret_from_sm(JOTFORM_SECRET_NAME)['API_KEY']
    jotform_client = JotformAPIClient(jotform_api_key)
    forms = jotform_client.get_form_submissions(form_id=form_id)
    if form_statuses:
        logger.info(f'Looking for submissions with the following status: {",".join(form_statuses)}')
        selected_forms = [form for form in forms.json()['content'] if form['status'] in form_statuses]
    else:
        selected_forms = forms.copy()

    logger.info(f'Found {len(selected_forms)} active forms for form: {form_id}')
    form_infos = list()

    for form in selected_forms:
        form_df = pd.DataFrame.from_dict(form['answers'], orient='index')
        form_answers = form_df.query('name.isin(@cols)')[['name','answer']].T
        form_answers.columns = form_answers.iloc[0,:]
        form_answers = form_answers.iloc[1:]
        form_answers = form_answers.rename(columns={'kitCode43':'kit_code', 'kitCode25': 'kit_code'})
        form_answers['created_at'] = form['created_at']
        form_infos.append(form_answers)

    logger.info(form_infos)
    if len(form_infos)>0:
        form_info = pd.concat(form_infos).reset_index().drop(columns=['index'])
    else:
        form_info = None

    return form_info

@log_start_stop
@log_runtime
def all_orders_from_jotform():
    cols = [
        'patientsName',
        'patientsEmail',
        'patientsDob',
        'patientsLmp',
        'imagingCenters',
        'patientsPhone',
        'kitCode25',
        'kitCode43',
        'created_at',
        'hazelTest'
    ]

    form_id_dict = {
        'birch': JOTFORM_ID_BIRCH,
        'hazel': JOTFORM_ID_HAZEL,
        # 'hazel_plus': JOTFORM_ID_HAZEL,
    }
    form_infos = list()
    for k, form_id in form_id_dict.items():
        form_info = pull_orders_from_jotform(form_id=form_id, cols=cols, form_statuses=['ACTIVE', 'ARCHIVED', 'CUSTOM'])
        if form_info is not None:
            form_info[['first_name', 'last_name']] = (
                form_info['patientsName']
                    .apply(lambda x: parse_form_names(x))
                    .apply(pd.Series)
            )

            form_info['dob'] = form_info['patientsDob'].apply(lambda x: parse_form_dates(x))
            form_info['lmp'] = form_info['patientsLmp'].apply(lambda x: parse_form_dates(x))

            if k == 'hazel':
                form_info['product_short_name'] = form_info['hazelTest'].apply(lambda x: parse_hazel_product(x))
            else:
                form_info['product_short_name'] = k
            form_infos.append(form_info)
        else:
            print(f'No new orders found for {k} products.')

    if len(form_infos)>0:
        total_from_info_df = pd.concat(form_infos)
        return total_from_info_df
    else:
        return None


def get_recent_order_df(limit=1000):
    conn_str = get_platformdb_conn_str('dsb-platform-db-readonly')
    query = f'''
            SELECT 
                O.ordered_at, 
                O.id as order_id, 
                O.order_number as lab_portal_order_number, 
                S.order_number AS shopify_order_id,
                o.cancelled, 
                u.email, 
                L.product AS product_sku,
                d.code AS kit_code
            FROM "order" AS O
            LEFT JOIN "user" AS U ON U.id = O.customer_id
            LEFT JOIN ORDER_SOURCE AS S ON S.order_id = O.id
            LEFT JOIN order_line_item L ON L.order_id=O.id
            LEFT JOIN ORDER_FULFILLMENT c ON l.id = c.line_item
            LEFT JOIN KIT d ON c.kit_id = d.id
            WHERE U.internal_test = False AND lower(u.last_name) <> 'test' AND lower(u.first_name) <> 'test'
            AND O.ordered_at>=CURRENT_DATE - INTERVAL '60 day'
            ORDER BY ordered_at DESC
            LIMIT {limit}
        '''

    order_df = pd.read_sql_query(query, con=conn_str)
    return order_df


def get_b2b_orders(
        variant_id,
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
                    "variant_id": variant_id,
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
            "tags": "Sync: Failed",
            "billing_address": account_address,
            "shipping_address": account_address,
            "financial_status": "paid",
            "send_receipt": False,
            "send_fulfillment_receipt": False,
            'test': test_flag,
        }
    }


@log_start_stop
def get_latest_product_variant_info(shop_env):
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
        SELECT *
        FROM JDX_PLATFORM.ANALYTICS.SHOPIFY_B2B_PRODUCTS
        WHERE ENV = '{shop_env}'
        ORDER BY UPDATE_TS DESC
    '''

    data = session.sql(query).collect()
    df = session.create_dataframe(data).to_pandas()
    df = (
        df
            .sort_values('UPDATE_TS', ascending=False)
            .groupby(['TITLE', 'SKU'])
            .head(1)
    )

    df.columns = [c.lower() for c in df.columns]
    df['account_id'] = df['title'].apply(lambda x: x.split('|')[0].strip())
    df['account_name'] = df['title'].apply(lambda x: x.split('|')[1].strip().upper())

    return df

def standardize_name(name):
    name = ''.join(s.lower() if i!=0 else s.upper() for i, s in enumerate(name) )
    return name


@log_start_stop
@log_runtime
@setup_logging_env
def jotform2shopify():
    shopify_helper = ShopifyHelper(SHOPIFY_SECRET_NAME)
    # find all orders from Jotform
    total_form_info_df = (
        all_orders_from_jotform()
            .rename(columns={
            'imagingCenters':'account_name',
            'created_at': 'order_submitted_at',
            'patientsEmail': 'email',
        })
    )

    # clean jotform format
    for c in ('first_name', 'last_name', 'account_name', 'kit_code'):
        total_form_info_df[c] = total_form_info_df[c].astype(str).apply(lambda x: x.upper().strip())

    total_form_info_df = (
        total_form_info_df
            .query('account_name!="TEST"')
            .query('last_name!="TEST"')
            .query('first_name!="TEST"')
    )
    total_form_info_df['email'] = total_form_info_df['email'].apply(lambda x: x.lower().strip())

    # remove orders that are already synced by matching kitcode in platform database
    order_df = get_recent_order_df(limit=1000)
    order_df['email'] = order_df['email'].apply(lambda x: x.lower().strip())
    # match on kit code first
    form_lp_order_df_1 = total_form_info_df[['email', 'kit_code']].merge(
        order_df[['ordered_at','kit_code', 'lab_portal_order_number', 'shopify_order_id']],
        on=['kit_code'], how='left').dropna().drop(columns=['kit_code'])
    # match on emails first
    form_lp_order_df_2 = total_form_info_df[['email']].merge(
        order_df[['ordered_at','email', 'lab_portal_order_number', 'shopify_order_id']], on=['email'],
        how='left').dropna()

    # form_lp_order_df = pd.concat([form_lp_order_df_1, form_lp_order_df_2]).drop_duplicates()
    form_lp_order_df_resolved = (
        form_lp_order_df_1.merge(
            form_lp_order_df_2,
            on=['lab_portal_order_number', 'email', 'ordered_at'],
            how='outer')
    )
    form_lp_order_df_resolved['shopify_order_id'] = (
        form_lp_order_df_resolved[['shopify_order_id_x', 'shopify_order_id_y']]
            .bfill(axis=1).iloc[:, 0]
    )
    form_lp_order_df_resolved = (
        form_lp_order_df_resolved.drop(columns=['shopify_order_id_x', 'shopify_order_id_y']).drop_duplicates()
    )

    total_form_info_df['order_date'] = pd.to_datetime(total_form_info_df['order_submitted_at']).dt.date
    form_lp_order_df_resolved['order_date'] = pd.to_datetime(form_lp_order_df_resolved['ordered_at']).dt.date
    matched_lab_portal_order = total_form_info_df.merge(form_lp_order_df_resolved, on=['email'], how='left')
    matched_lab_portal_order['order_date_diff'] = (
            matched_lab_portal_order['order_date_x'] - matched_lab_portal_order['order_date_y']
    )
    # only matched to the jotform orders that has the closest date
    matched_lab_portal_order = matched_lab_portal_order.sort_values('order_date_diff').groupby(
        'lab_portal_order_number').head(1)
    total_form_info_df_final = total_form_info_df.merge(
        matched_lab_portal_order[['email', 'order_submitted_at', 'lab_portal_order_number']],
        on=['email', 'order_submitted_at'], how='left'
    )

    # get latest variant information
    shopify_secrets = get_secret_from_sm(SHOPIFY_SECRET_NAME)
    shop_env = shopify_secrets['SHOP_ENV']
    variant_df=get_latest_product_variant_info(shop_env)

    # Find orders to be created
    logger.info(total_form_info_df_final.columns)
    new_orders = total_form_info_df_final.query('lab_portal_order_number.isna()').copy()

    if len(new_orders)>0:
        logger.info(f'Found {len(new_orders)} orders to create.')
        new_orders['account_name_sku'] = new_orders['account_name'] + '|' + new_orders['product_short_name']
        variant_df['account_name_sku'] = variant_df['account_name'] + '|' + variant_df['product_short_name']
        fuzzy_matched_df = fuzzy_merge(
            new_orders, variant_df[['account_name_sku', 'id', 'product_id', 'price']],
            'account_name_sku', 'account_name_sku',
            threshold=90,
            how='left'
        ).rename(columns={'id':'variant_id'})
        fuzzy_matched_df_cols = [
            'account_name',
            'first_name',
            'last_name',
            'email',
            'dob',
            'lmp',
            'product_short_name',
            'product_id',
            'variant_id',
            'kit_code',
            'order_submitted_at',
        ]

        shopify_order_names=list()
        shopify_order_ids = list()
        for order in fuzzy_matched_df.iterrows():
            account_name = order[1]['account_name']
            first_name = standardize_name(order[1]['first_name'])
            last_name = standardize_name(order[1]['last_name'])
            email = order[1]['email']
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
    jotform2shopify()