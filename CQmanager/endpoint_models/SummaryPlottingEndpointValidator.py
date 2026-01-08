from datetime import datetime
from typing import Optional

from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
from pydantic import BaseModel, Field, field_validator, model_validator

from CQmanager.core.logging import logger


class SummaryPlottingEndpointValidator(BaseModel):
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
    methylation_classes: str = Field(
        default="None",
        description="Methylation classes string to plot.",
    )
    bin_size: Optional[str | int] = Field(
        default="None",
        description="Bin size for the summary plots.",
    )
    min_probes_per_bin: Optional[str | int] = Field(
        default="None",
        description="Minimum number of probes per bin.",
    )
    downsize_to: str = Field(
        default="none",
        description="Downsizing target specified as comma-separated string of CommonArrayType",
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    )

    # Get downsizing targets to use
    @field_validator("downsize_to")
    @classmethod
    def validate_downsize_to(cls, value) -> str:
        downsizing_targets: list[CommonArrayType] = []
        if isinstance(value, str) and value.strip().lower() == "none":
            downsizing_targets = CommonArrayType.get_members()
        else:
            valid_downsizing_targets: list[str] = [
                member.lower() for member in CommonArrayType.members_list()
            ]

            for element in value.split(","):
                if element.lower() not in valid_downsizing_targets:
                    logger.warning(
                        msg=f"Invalid downsize_to '{element}'. Valid: {CommonArrayType.members_list()}.\nThe plotter will proceed by plotting all downsizing targets if no valid targets are provided."
                    )
                else:
                    current_downsize_target = CommonArrayType.get_member_from_string(
                        value=element.lower()
                    )
                    if current_downsize_target is not None:
                        downsizing_targets += [current_downsize_target]

        if not downsizing_targets:
            downsizing_targets = CommonArrayType.get_members()
        return_value: str = ",".join([target.value for target in downsizing_targets])
        return return_value

    @field_validator("bin_size")
    @classmethod
    def validate_bin_size(cls, value) -> Optional[str | int]:
        if value is None:
            return value
        if isinstance(value, str) and value.strip().lower() == "none":
            return None
        try:
            bin_size = int(value)
            if bin_size <= 0:
                raise ValueError("Bin size must be a positive integer.")
        except ValueError:
            raise ValueError("Bin size must be a positive integer.")
        return bin_size

    @field_validator("min_probes_per_bin")
    @classmethod
    def validate_min_probes_per_bin(cls, value) -> Optional[str | int]:
        if value is None:
            return value
        if isinstance(value, str) and value.strip().lower() == "none":
            return None
        try:
            min_probes_per_bin = int(value)
            if min_probes_per_bin <= 0:
                raise ValueError("Bin size must be a positive integer.")
        except ValueError:
            raise ValueError("Bin size must be a positive integer.")
        return min_probes_per_bin

    @model_validator(mode="after")
    def validate_bin_size_vs_min_probes(self):
        if (
            self.bin_size is not None
            and self.min_probes_per_bin is not None
            and isinstance(self.bin_size, int)
            and isinstance(self.min_probes_per_bin, int)
        ):
            if self.bin_size < self.min_probes_per_bin:
                raise ValueError(
                    "Bin size must not be smaller than min_probes_per_bin."
                )
        return self
