from numba import njit
import numpy as np


def unique_list(list_to_simplify):
    list_to_simplify = set(list_to_simplify)
    list_to_simplify = list(list_to_simplify)
    list_to_simplify = [element for element in list_to_simplify
                        if len(element) > 0 and element != 'empty_user_id']

    return list_to_simplify


@njit(parallel=True)
def normalize_columns(data):
    '''
    Performs a row-wise normalization, meant to be used with profile level features such as count of pageviews in
    individual section where we want to have the number of pagevieews on a given section and / or share of pageviews
    in a given section on all pageviews as this might provide additional / less noisy information
    :param data:
    :return:
    '''
    N = data.shape[0]
    M = data.shape[1]
    normalized_data = np.array([[data[i, j] / np.sum(data[i, :])
                                 for j in range(M)] for i in range(N)])

    return normalized_data
