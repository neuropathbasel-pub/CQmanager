from datetime import datetime

from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator

from CQmanager.core.config import config


class SummaryPlotting(BaseModel):
    """
    Pydantic model for configuring summary plot generation with validation.

    Defines settings for generating summary plots, including preprocessing method, methylation classes, minimum Sentrix IDs
    per plot, and a timestamp. Validates the preprocessing method and ensures it is one of the available options.
    Automatically generates a timestamp.

    Attributes:
        preprocessing_method (str): Preprocessing method for summary plots (default: 'illumina', must be in available_preprocessing_methods).
        timestamp (str): Timestamp of creation (default: current time in 'YYYY-MM-DD_HH-MM-SS' format).
        methylation_classes (str): Methylation classes to plot (default: 'None').
        min_sentrix_ids_per_plot (int): Minimum number of Sentrix IDs required per summary plot (default: 3, must be >= 0).

    Raises:
        HTTPException: If preprocessing_method is not a string or not in available_preprocessing_methods.
    """

    preprocessing_method: str = Field(
        default="illumina",
        description="Preprocessing method for which the summary plots shall be made. One of 'illumina' or 'swan'.",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    )

    methylation_classes: str = Field(
        default="None",
        description="Methylation classes string to plot.",
    )

    min_sentrix_ids_per_plot: int = Field(
        default=3,
        ge=0,
        description="Minimum Sentrix ids for per summary plot.",
    )

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
