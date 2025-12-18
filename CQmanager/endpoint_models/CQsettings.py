from datetime import datetime

from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator

from CQmanager.core.config import config
from CQmanager.utilities.checkups import check_if_idat_pair_exists


class CQsettings(BaseModel):
    """
    Pydantic model for configuring CNV analysis settings with validation.

    Defines and validates settings for CNV analysis, including Sentrix ID, preprocessing method, bin size, and minimum
    probes per bin. Automatically generates a timestamp. Ensures Sentrix ID corresponds to an existing IDAT pair and
    preprocessing method is valid. Applies constraints on bin size and probes per bin.

    Attributes:
        sentrix_id (str): Unique identifier for the Sentrix sample.
        timestamp (str): Timestamp of creation (default: current time in 'YYYY-MM-DD_HH-MM-SS' format).
        preprocessing_method (str): Method used for preprocessing (must be in available_preprocessing_methods).
        bin_size (int): Bin size for CNV analysis (default: default_bin_size, constrained between ge_bin_size and le_bin_size).
        min_probes_per_bin (int): Minimum probes per bin for CNV analysis (default: default_min_probes_per_bin,
            constrained between ge_min_probes_per_bin and le_min_probes_per_bin).

    Raises:
        HTTPException: If validations fail for sentrix_id (not a string, empty, or missing IDAT pair) or
            preprocessing_method (not a string or not in available methods).
    """

    sentrix_id: str
    timestamp: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    )

    preprocessing_method: str
    downsize_to: str = CommonArrayType.NO_DOWNSIZING.value
    bin_size: int = Field(
        default=config.default_bin_size,
        ge=config.ge_bin_size,
        le=config.le_bin_size,
        description="Bin size for cnv analysis",
    )
    min_probes_per_bin: int = Field(
        default=config.default_min_probes_per_bin,
        ge=config.ge_min_probes_per_bin,
        le=config.le_min_probes_per_bin,
        description="Min probes per bin for cnv analysis",
    )
    type: str = Field(default="")

    @field_validator("sentrix_id")
    @classmethod
    def validate_sentrix_id(cls, value):
        if not isinstance(value, str):
            raise HTTPException(
                status_code=422,
                detail=f"Sentrix id has to be a string. Supplied type is {type(value)}",
            )

        if len(value) < 1:
            raise HTTPException(
                status_code=422,
                detail=f"Sentrix id has to have at least one character. Supplied sentrix id character number is {len(value)}",
            )

        if not check_if_idat_pair_exists(
            sentrix_id=value, idat_directory=config.idat_directory
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Idat pair for Sentrix id {value} has not been found.",
            )
        return value

    @field_validator("preprocessing_method")
    @classmethod
    def validate_preprocessing_method(cls, value):
        if not isinstance(value, str):
            raise HTTPException(
                status_code=422,
                detail=f"preprocessing method has to be a string. Supplied type is {type(value)}",
            )

        if value not in config.available_preprocessing_methods:
            raise HTTPException(
                status_code=422,
                detail=f"preprocessing method has to be one of {config.available_preprocessing_methods}. Supplied method is {value}",
            )

        return value

    @field_validator("downsize_to")
    @classmethod
    def validate_downsizing_types(cls, value):
        if not isinstance(value, str):
            raise HTTPException(
                status_code=422,
                detail=f"downsize_to has to be a string. Supplied type is {type(value)}",
            )

        if value not in CommonArrayType.members_list():
            raise HTTPException(
                status_code=422,
                detail=f"downsizing type has to be one of {', '.join(CommonArrayType.members_list())}. Supplied method is {value}",
            )

        return value

    def model_post_init(self, __context):
        self.type = self.__class__.__name__

    def __getitem__(self, key: str):
        """Allow dict-like access to fields."""
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(f"'{key}' is not a valid field")

    def __setitem__(self, key: str, value):
        """Allow dict-like setting of fields."""
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            raise KeyError(f"'{key}' is not a valid field")
