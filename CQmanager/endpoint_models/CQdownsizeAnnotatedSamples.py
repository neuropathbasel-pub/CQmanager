from cnquant_dependencies.enums.PreprocessingMethods import PreprocessingMethods
from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator

from CQmanager.core.config import config


class CQdownsizeAnnotatedSamples(BaseModel):
    preprocessing_method: str = PreprocessingMethods.ILLUMINA.value
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

        if value.lower() not in PreprocessingMethods.members_list():
            raise HTTPException(
                status_code=422,
                detail=f"preprocessing method has to be one of {config.available_preprocessing_methods}. Supplied method is {value}",
            )

        return value.lower()

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
