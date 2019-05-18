import enum
from enum import Enum

normalized_feature_handling = enum(
    IGNORE='ignore',
    ADD='add',
    REPLACE_WITH='replace'
)

split_type = enum(
    RANDOM = 'random',
    TIME_BASED = 'time_based'
)
