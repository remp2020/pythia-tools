import enum


class NormalizedFeatureHandling(enum):
    IGNORE='ignore'
    ADD='add'
    REPLACE_WITH='replace'


class SplitType(enum):
    RANDOM = 'random'
    TIME_BASED = 'time_based'
