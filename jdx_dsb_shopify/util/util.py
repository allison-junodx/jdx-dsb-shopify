import logging

import pandas as pd
from thefuzz import fuzz
from thefuzz import process
import logging

logger=logging.getLogger(__name__)

def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, **kwargs):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """
    df_1 = df_1.copy()
    df_2 = df_2.copy()

    s = df_2[key2].tolist()

    df_1[['matched', 'score']] = df_1[key1].apply(
        lambda x: process.extractOne(x, s)
    ).apply(pd.Series)

    df_1 = df_1.merge(df_2, left_on=['matched'], right_on=[key2], **kwargs).query(f'score>={threshold}')

    return df_1

