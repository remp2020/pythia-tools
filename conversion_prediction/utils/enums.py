from enum import Enum


class NormalizedFeatureHandling(Enum):
    IGNORE='ignore'
    ADD='add'
    REPLACE_WITH='replace'


class SplitType(Enum):
    RANDOM = 'random'
    TIME_BASED = 'time_based'


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


class ArtifactRetentionMode(Enum):
    DUMP = 'dump'
    DROP = 'drop'


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
    ]
    UNDERSAMPLING_TUNING = [
        ModelArtifacts.USER_PROFILES,
        ModelArtifacts.TRAIN_DATA_FEATURES,
        ModelArtifacts.TRAIN_DATA_OUTCOME,
        ModelArtifacts.TEST_DATA_FEATURES,
        ModelArtifacts.TEST_DATA_OUTCOME,
    ]
    MODEL_RETRAINING = [
        ModelArtifacts.MODEL,
        ModelArtifacts.OUTCOME_FRAME,
        ModelArtifacts.VARIABLE_IMPORTANCES
    ]
    PREDICTION =[
        ModelArtifacts.PREDICTIONS
    ]
