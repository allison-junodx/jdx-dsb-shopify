import logging
from collections import namedtuple
from datetime import datetime

import pandas as pd
import snowflake
from jdx_utils.api.secrets import get_secret_from_sm
from jdx_utils.util import log_start_stop, log_runtime
from snowflake.snowpark import Session

from jdx_dsb_shopify.globals import SNOWFLAKE_SECRET_NAME, SNOWFLAKE_WH, SHOPIFY_SECRET_NAME, JOTFORM_SECRET_NAME, \
    JOTFORM_ID_HAZEL, JOTFORM_ID_BIRCH
from jdx_dsb_shopify.util.jotform_utils import JotformAPIClient, parse_form_names, parse_form_dates
from jdx_dsb_shopify.util.logging import setup_logging_env
from jdx_dsb_shopify.util.shopify_utils import ShopifyHelper


logger = logging.getLogger(__name__)

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
        form_infos.append(form_answers)

    form_info = pd.concat(form_infos).reset_index().drop(columns=['index'])

    return form_info

@log_start_stop
@log_runtime
def all_orders_from_jotform():
    cols = [
        'patientsName',
        'paitentsEmail',
        'patientsDob',
        'patientsLmp',
        'imagingCenters',
        'patientsPhone',
        'kitCode25'
    ]

    form_id_dict = {
        'birch': JOTFORM_ID_BIRCH,
        'hazel_basic': JOTFORM_ID_HAZEL,
        # 'hazel_plus': JOTFORM_ID_HAZEL,
    }
    form_infos = list()
    for k, form_id in form_id_dict.items():
        form_info = pull_orders_from_jotform(form_id=form_id, cols=cols, form_statuses=['ACTIVE'])
        form_info[['first_name', 'last_name']] = (
            form_info['patientsName']
                .apply(lambda x: parse_form_names(x))
                .apply(pd.Series)
        )

        form_info['dob'] = form_info['patientsDob'].apply(lambda x: parse_form_dates(x))
        form_info['lmp'] = form_info['patientsLmp'].apply(lambda x: parse_form_dates(x))

        form_info['product_short_name'] = k
        form_infos.append(form_info)

    total_from_info_df = pd.concat(form_infos)

    return total_from_info_df


@log_start_stop
@log_runtime
@setup_logging_env
def jotform2shopify():

    # find all orders from Jotform
    total_from_info_df = all_orders_from_jotform()

    # remove orders that are already synced by matching kitcode in platform database




if __name__ == "__main__":
    jotform2shopify()