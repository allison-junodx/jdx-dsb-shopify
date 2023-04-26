import logging

import pandas as pd
import requests
import json
from jdx_utils.api.secrets import get_secret_from_sm
from jdx_utils.util import log_start_stop, log_runtime

from jdx_dsb_shopify.globals import FST_BARCODE, FST_SKU, FST_LP, NIPS_BASIC_BARCODE, NIPS_BASIC_SKU, NIPS_BASIC_LP, \
    NIPS_PLUS_BARCODE, NIPS_PLUS_SKU, NIPS_PLUS_LP

logger = logging.getLogger(__name__)

class ShopifyHelper:
    def __init__(self, secret_name):
        self._access_token = get_secret_from_sm(secret_name)['SHOPIFY_TOKEN']
        self._shop_env = get_secret_from_sm(secret_name)['SHOP_ENV']
        self._product_endpoint = f'https://{self._shop_env}/admin/api/2022-07/products.json'
        self._order_endpoint = f'https://{self._shop_env}/admin/api/2022-07/orders.json'
        self._headers = {
            'X-Shopify-Access-Token': self._access_token,
            'Content-Type': 'application/json'
        }

    @property
    def shop_env(self):
        return self._shop_env

    @property
    def headers(self):
        return self._headers

    def get_products(self, product_ids: list = None):
        param_payloads = {
            'ids': product_ids
        }

        r = requests.get(
            self._product_endpoint,
            params = param_payloads,
            headers=self.headers
        )
        return r

    @log_start_stop
    @log_runtime
    def create_product(self, product_info):
        r = requests.post(self._product_endpoint, data=json.dumps(product_info), headers=self.headers)
        return r

    @log_start_stop
    @log_runtime
    def delete_product(self, product_id):
        r = requests.delete(self._product_endpoint.replace('.json', f'/{product_id}.json'), headers=self.headers)
        return r

    @log_start_stop
    @log_runtime
    def create_order(self, order_info):
        r = requests.post(self._order_endpoint, data=json.dumps(order_info), headers=self.headers)
        return r

    @staticmethod
    def create_product_info(
            product_title: str,
            body_html: str = '',
            published: bool = False,
            status: str = 'active',
            product_type: str = 'birch',
            vendor: str = 'junodx-dev-2',
            tags: list = [],
            variants: list = []
    ):
        return {
            'product': {
                'title': product_title,
                'body_html': body_html,
                'status': status,
                'product_type': product_type,
                'vendor': vendor,
                'published': published,
                'tags': tags,
                'variants': variants
            }
        }

    @staticmethod
    def create_product_variants_from_df(
            df: pd.DataFrame,
            barcode: str,
            sku: str,
            compared_at_price: float,
            fulfilment_service: str = 'manual',
            taxable: bool = True
    ):
        variants = []

        # check for all required info
        for row in df.iterrows():
            variant_info = {
                "option1": f"{row[1]['account_id']}|{row[1]['account_name']}",
                'title': row[1]['account_id'],
                'barcode': barcode,
                'sku': sku,
                'compare_at_price': compared_at_price,
                'price': row[1]['price'],
                'fulfillment_service': fulfilment_service,
                'taxable': taxable,
            }
            variants.append(variant_info)

        return variants


    @log_start_stop
    def create_b2b_products(
            self,
            variant_df: pd.DataFrame,
            status: str = 'active',
            product_short_name: str = 'birch'
    ):
        logger.info(f'Creating a Shopify product with product type: {product_short_name}')
        logger.info(f'Create with {len(variant_df)} variants.')
        if product_short_name == 'birch':
            product_info = self.create_product_info(
                product_title='Birch Fetal Gender Test B2B Ultrasound Centers',
                body_html='',
                status=status,
                product_type='birch',
                vendor='junodx',
                published=False,
                tags=['b2b', 'birch', 'imaging_centers'],
                variants=self.create_product_variants_from_df(
                    df=variant_df,
                    barcode=FST_BARCODE,
                    sku=FST_SKU,
                    compared_at_price=FST_LP,
                )

            )
        elif product_short_name == 'hazel_basic':
            product_info = self.create_product_info(
                product_title='Hazel NIPS-Basic Test B2B Ultrasound Centers',
                body_html='',
                status=status,
                product_type='hazel_basic',
                vendor='junodx',
                published=False,
                tags=['b2b', 'hazel_basic', 'imaging_centers'],
                variants=self.create_product_variants_from_df(
                    df=variant_df,
                    barcode=NIPS_BASIC_BARCODE,
                    sku=NIPS_BASIC_SKU,
                    compared_at_price=NIPS_BASIC_LP,
                )

            )
        elif product_short_name == 'hazel_plus':
            product_info = self.create_product_info(
                product_title='Hazel NIPS-Plus Test B2B Ultrasound Centers',
                body_html='',
                status=status,
                product_type='hazel_plus',
                vendor='junodx',
                published=False,
                tags=['b2b', 'hazel_plus', 'imaging_centers'],
                variants=self.create_product_variants_from_df(
                    df=variant_df,
                    barcode=NIPS_PLUS_BARCODE,
                    sku=NIPS_PLUS_SKU,
                    compared_at_price=NIPS_PLUS_LP,
                )

            )

        else:
            raise ValueError(f'Unknown product type: {product_short_name}')

        return self.create_product(product_info)

