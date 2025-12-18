from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator

from CQmanager.core.config import config


class CQmissingSettings(BaseModel):
    """
    Pydantic model for partial CNV analysis settings with validation.

    Defines and validates a subset of CNV analysis settings, including preprocessing method, bin size, and minimum probes
    per bin. Ensures the preprocessing method is valid and applies constraints on bin size and probes per bin.

    Attributes:
        preprocessing_method (str): Method used for preprocessing (must be in available_preprocessing_methods).
        bin_size (int): Bin size for CNV analysis (default: default_bin_size, constrained between ge_bin_size and le_bin_size).
        min_probes_per_bin (int): Minimum probes per bin for CNV analysis (default: default_min_probes_per_bin,
            constrained between ge_min_probes_per_bin and le_min_probes_per_bin).
        downsize_to (str): Downsizing target array type (default: NO_DOWNSIZING).

    Raises:
        HTTPException: If preprocessing_method is not a string or not in available_preprocessing_methods.
    """

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
