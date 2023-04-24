import pandas as pd
import requests
import json
from jdx_utils.api.secrets import get_secret_from_sm

from jdx_dsb_shopify.globals import FST_BARCODE, FST_SKU, FST_LP


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

    def create_product(self, product_info):
        r = requests.post(self._product_endpoint, data=json.dumps(product_info), headers=self.headers)
        return r

    def create_order(self, order_info):
        r = requests.post(self._order_endpoint, data=json.dumps(order_info), headers=self.headers)
        return r

    @staticmethod
    def create_product_info(
            product_title: str,
            body_html: str = '',
            status: str = 'active',
            product_type: str = 'birch',
            vendor: str = 'junodx-dev-2',
            metafields: list = [],
            variants: list = []
    ):
        return {
            'product': {
                'title': product_title,
                'body_html': body_html,
                'status': status,
                'product_type': product_type,
                'vendor': vendor,
                'metafields': metafields,
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
                "option1": row[1]['account_name'],
                'title': row[1]['account_id'],
                'barcode': barcode,
                'sku': sku,
                'compared_at_price': compared_at_price,
                'price': row[1]['variant_price'],
                'fulfillment_service': fulfilment_service,
                'taxable': taxable,
            }
            variants.append(variant_info)

        return variants


    def create_b2b_birch_products(
            self,
            variant_df: pd.DataFrame,
            status:str = 'active',
    ):
        # FST:
        #   sku: 50875e49-4485-4405-b329-69877c13ee2d
        #   barcode: 196852085453
        #   list_price= 89.00
        product_info = self.create_product_info(
            product_title='Birch Fetal Gender Test B2B Ultrasound Centers',
            body_html='',
            status=status,
            product_type='birch',
            vendor='junodx',
            metafields=[
                {
                    'b2b': True,
                }
            ],
            variants=self.create_product_variants_from_df(
                df = variant_df,
                barcode = FST_BARCODE,
                sku = FST_SKU,
                compared_at_price=FST_LP,
            )

        )

        return self.create_product(product_info)

    def create_b2b_hazel_products(
            self,
            variant_df: pd.DataFrame,
            status: str = 'active',
    ):
        # FST:
        #   sku: 50875e49-4485-4405-b329-69877c13ee2d
        #   barcode: 196852085453
        #   list_price= 89.00
        product_info = self.create_product_info(
            product_title='Hazel NIPS-Basic Test B2B Ultrasound Centers',
            body_html='',
            status=status,
            product_type='hazel',
            vendor='junodx',
            metafields=[
                {
                    'b2b': True,
                }
            ],
            variants=self.create_product_variants_from_df(
                df=variant_df,
                barcode=FST_BARCODE,
                sku=FST_SKU,
                compared_at_price=FST_LP,
            )

        )

        return self.create_product(product_info)