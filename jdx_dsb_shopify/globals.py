import os

shopify_secret_name = {
    'dev': 'dsb-shopify-dev2-secret',
    'beta': 'dsb-shopify-beta-secrets',
    'prd': 'dsb-shopify-app-token'
}

SHOPIFY_SECRET_NAME = shopify_secret_name[os.environ['ENV']]

# product details
FST_SKU = '50875e49-4485-4405-b329-69877c13ee2d'
FST_BARCODE = '196852085453'
FST_LP = 89.00

NIPS_BASIC_SKU = '2adc5ddf-e519-480c-966d-1385c8592d81'
NIPS_BASIC_BARCODE = '197644902637'
NIPS_BASIC_LP = 599.00

NIPS_PLUS_SKU = '98e2f68b-1b55-48df-b9d3-8953bf926b7d'
NIPS_PLUS_BARCODE = '197644105045'
NIPS_PLUS_LP = 699.00


# Get Snowflake credentials
SNOWFLAKE_SECRET_NAME = 'dsb-snowflake-secrets'
SNOWFLAKE_WH = 'DSB_ANALYTICS_WH'

# Jotform details
JOTFORM_SECRET_NAME = 'dsb-jotform-api-key'
JOTFORM_ID_REDRAW ='231075275142955'
JOTFORM_ID_HAZEL = '231069003892959'
JOTFORM_ID_BIRCH = '231068067067962'
