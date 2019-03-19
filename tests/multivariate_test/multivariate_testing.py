import pandas as pd
import unittest
from command.multivariate_testing.multi_armed_bandit import evaluate_test


class MultivariateTestUnit(unittest.TestCase):
    def test_including_results_for_all_variants(self):
        test_result = evaluate_test(
            arm_results= {
                'variant-a': {
                    'conversions': 10,
                    'total': 10000
                              },
                'variant-b': {
                    'conversions': 5,
                    'total': 10000
                }
            }
        )

        self.assertListEqual(['variant-a', 'variant-b'], list(test_result.keys()))

    def test_clear_winning_alternative(self):
        test_result = evaluate_test(
            arm_results= {
                'variant-a': {
                    'conversions': 10,
                    'total': 10000
                              },
                'variant-b': {
                    'conversions': 100,
                    'total': 10000
                }
            }
        )

        self.assertGreater(float(test_result['variant-b']), float(test_result['variant-a']))


if __name__ == '__main__':
    unittest.main()
#
#     class GetAudienceScoreBulkTestCase(unittest.TestCase):
#         def test_optimization_entities_deltas(self):
#             for platform in platforms:
#                 with self.subTest(platform=platform):
#                     actual, expected = get_actual_end_expected_data(
#                         platform=platform,
#                         entity_level='optimization_entity',
#                         score_type=ScoreType.delta
#                     )
#                     assert_frame_equal(actual, expected)