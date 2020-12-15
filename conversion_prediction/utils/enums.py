from enum import Enum


# We support 3 types of handling for normalized feature creation:
# 1. No normalized features
# 2. Add normalized features (keeping the non-normalized versions too)
# 3. Replace the non-normalized features with normalized
# We keep track of these modes due to the need to keep 2 sets of column names with option (3.)
class NormalizedFeatureHandling(Enum):
    IGNORE = 'ignore'
    ADD = 'add'
    REPLACE_WITH = 'replace'


# Split type for train vs test, time based more accurately represents how the data appears in reality and
# usually results in lower accuracies
class SplitType(Enum):
    RANDOM = 'random'
    TIME_BASED = 'time_based'


# In order to possibly minimize the memory requirements of our training pipeline, we need to better manage
# the artifacts that we create, by either dropping or dumping them into a csv, the next 3 enums are used to:
# 1. Determine which more to employ - ArtifactRetentionMode
# 2. Define the artifacts themselves - ModelArtifacts
# 3. Define sets of artifacts that would be handled in this way - ArtifactRetentionCollection
class ArtifactRetentionMode(Enum):
    DUMP = 'dump'
    DROP = 'drop'


class ModelArtifacts(Enum):
    USER_PROFILES = 'user_profiles'
    TRAIN_DATA_FEATURES = 'X_train'
    TRAIN_DATA_OUTCOME = 'Y_train'
    UNDERSAMPLED_TRAIN_DATA_FEATURES = 'X_train_undersampled'
    UNDERSAMPLED_TRAIN_DATA_OUTCOME = 'Y_train_undersampled'
    TEST_DATA_FEATURES = 'X_test'
    TEST_DATA_OUTCOME = 'Y_test'
    MODEL = 'model'
    OUTCOME_FRAME = 'outcome_frame'
    VARIABLE_IMPORTANCES = 'variable_importances'
    PREDICTION_DATA = 'prediction_data'
    PREDICTIONS = 'predictions'


class ArtifactRetentionCollection(Enum):
    ALL = [
        ModelArtifacts.USER_PROFILES,
        ModelArtifacts.TRAIN_DATA_FEATURES,
        ModelArtifacts.TRAIN_DATA_OUTCOME,
        ModelArtifacts.UNDERSAMPLED_TRAIN_DATA_FEATURES,
        ModelArtifacts.UNDERSAMPLED_TRAIN_DATA_OUTCOME,
        ModelArtifacts.TEST_DATA_FEATURES,
        ModelArtifacts.TEST_DATA_OUTCOME,
        ModelArtifacts.MODEL,
        ModelArtifacts.OUTCOME_FRAME,
        ModelArtifacts.VARIABLE_IMPORTANCES
    ]
    MODEL_TUNING = [
        ModelArtifacts.USER_PROFILES,
        ModelArtifacts.TRAIN_DATA_FEATURES,
        ModelArtifacts.TRAIN_DATA_OUTCOME,
        ModelArtifacts.UNDERSAMPLED_TRAIN_DATA_FEATURES,
        ModelArtifacts.UNDERSAMPLED_TRAIN_DATA_OUTCOME,
        ModelArtifacts.TEST_DATA_FEATURES,
        ModelArtifacts.TEST_DATA_OUTCOME,
        ModelArtifacts.OUTCOME_FRAME,
        ModelArtifacts.VARIABLE_IMPORTANCES
    ]
    UNDERSAMPLING_TUNING = [
        ModelArtifacts.USER_PROFILES,
        ModelArtifacts.TRAIN_DATA_FEATURES,
        ModelArtifacts.TRAIN_DATA_OUTCOME,
        ModelArtifacts.TEST_DATA_FEATURES,
        ModelArtifacts.TEST_DATA_OUTCOME,
        ModelArtifacts.OUTCOME_FRAME,
        ModelArtifacts.VARIABLE_IMPORTANCES
    ]
    MODEL_RETRAINING = [
        ModelArtifacts.MODEL,
        ModelArtifacts.OUTCOME_FRAME,
        ModelArtifacts.VARIABLE_IMPORTANCES
    ]
    PREDICTION =[
        ModelArtifacts.PREDICTIONS
    ]


class DataRetrievalMode(Enum):
    MODEL_TRAIN_DATA = 'model_train_data'
    PREDICT_DATA = 'predict_data'
