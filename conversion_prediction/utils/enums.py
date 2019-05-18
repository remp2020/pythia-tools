from enum import Enum


class NormalizedFeatureHandling(Enum):
    IGNORE='ignore'
    ADD='add'
    REPLACE_WITH='replace'


class SplitType(Enum):
    RANDOM = 'random'
    TIME_BASED = 'time_based'
