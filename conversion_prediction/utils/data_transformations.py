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
    N = data.shape[0]
    M = data.shape[1]
    normalized_data = np.array([[data[i, j] / np.sum(data[i, :])
                                 for j in range(M)] for i in range(N)])

    return normalized_data
