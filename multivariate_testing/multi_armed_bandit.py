import pandas as pd
import argparse
import json
from scipy.stats import beta
from typing import Dict, List


def evaluate_test(
    arm_results: Dict,
    ndraws: int=10000
) -> pd.Series:
    '''
    :param arm_results: contains the results of two or more multivariate test variants example format:
    {'variant-a': {'conversions': 10, 'total': 1000}, 'variant-b': {'conversions': 5, 'total': 900}}
    :param ndraws: number of draws for beta distribution simulations, impacts processing times
    :return:
    '''
    simulated_data = simulate_distribution(arm_results, ndraws)
    win_probabilities = calculate_win_probabilities(simulated_data)
    win_probabilities = format_win_probabilities(win_probabilities, list(arm_results.keys()))
    print(f'Win probabilities: \n{win_probabilities}')

    return win_probabilities


def simulate_distribution(
        arm_results: Dict,
        ndraws: int
) -> pd.DataFrame:
    '''
    Bootstraps samples from beta distribution given the empirical parameters for conversion and non-conversion events
    :param arm_results:
    :param ndraws:
    :return:
    '''
    simulated_data = pd.DataFrame(index=range(0, ndraws), columns=list(arm_results.keys()))
    for arm in arm_results.keys():
        simulated_data[arm] = beta.rvs(
            a=arm_results[arm]['conversions'],
            b=arm_results[arm]['total'] - arm_results[arm]['conversions'],
            size=ndraws
        )

    return simulated_data


def format_win_probabilities(
        win_probabilities: pd.Series,
        arm_keys: List
) -> pd.Series:
    '''
    In case there is one probability winning in all simulation cases, pads the other variants with 0 % winning
    probabilities
    :param win_probabilities:
    :param arm_keys:
    :return:
    '''
    for arm in arm_keys:
        if arm not in win_probabilities.index:
            win_probabilities[arm] = 0.0
    win_probabilities.sort_index(inplace=True)

    return win_probabilities


def calculate_win_probabilities(
        simulated_data: pd.DataFrame
) -> pd.Series:
    '''
    Calculates likelyhoods each variant being the winning one
    :param simulated_data:
    :return:
    '''
    row_maxes = simulated_data.apply(calculate_row_max_arm, axis=1)
    row_max_count = row_maxes.value_counts()
    win_probabilities = row_max_count / row_max_count.sum()

    return win_probabilities


def calculate_row_max_arm(
        row: pd.Series
) -> str:
    '''
    Finds the winning probability for a given row
    :param row:
    :return:
    '''
    row = row.squeeze()
    max_value_arm = str(row[row == row.max()].index[0])

    return max_value_arm

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("arm_results",
                        help='''Results for individual arms in the format of 
                        {
                            "variant-a": 
                                {"conversions": 10, "total": 1000}, 
                            "variant-b": 
                                {"conversions": 5, "total": 900}
                        }
                        ''',
                        type=json.loads)
    parser.add_argument('--ndraws',
                        help='Number of draws for boostrap simulation',
                        type=int,
                        default=10000,
                        required=False)

    args = parser.parse_args()
    args = vars(args)
    if len(args['arm_results'].keys()) < 2:
        raise ValueError('Not enough variants to evaluate (min 2)')
    for arm_key in args['arm_results'].keys():
        if args['arm_results'][arm_key]['total'] < args['arm_results'][arm_key]['conversions']:
            raise ValueError(f'Arm {arm_key} has more conversions events than total events')

    test_result = evaluate_test(
        arm_results=args['arm_results'],
        ndraws=args['ndraws']
    )

evaluate_test({"variant-a": {"conversions": 6, "total": 10000}, "variant-b": {"conversions": 5, "total": 10000}})