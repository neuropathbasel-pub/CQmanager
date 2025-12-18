from typing import Union, cast

from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
from cnquant_dependencies.enums.PreprocessingMethods import PreprocessingMethods


class AnalysisTaskData(dict):
    def __new__(cls, task_data: dict[str, Union[str, int]]) -> "AnalysisTaskData":
        if not isinstance(task_data, dict):
            raise ValueError("task_data must be a dictionary.")
        valid_task_data_keys = {
            "bin_size",
            "min_probes_per_bin",
            "preprocessing_method",
            "sentrix_id",
        }
        task_data_keys = set(task_data.keys())
        valid_preprocessing_methods = PreprocessingMethods.members_list()
        valid_downsizing_targets: list[str] = list(CommonArrayType.members_list())

        task_dict: dict[str, Union[str, int]] = dict()

        if not all([key in task_data_keys for key in valid_task_data_keys]):
            raise ValueError(
                f"task_data must have the following keys: {', '.join(valid_task_data_keys)}.\nInput keys were: {', '.join(task_data_keys)}"
            )
        task_dict: dict[str, Union[str, int]] = dict()
        try:
            task_dict["bin_size"] = int(task_data["bin_size"])
            task_dict["min_probes_per_bin"] = int(task_data["min_probes_per_bin"])

            found_downsize_to = task_data.get("downsize_to", None)
            if found_downsize_to is None:
                task_dict["downsize_to"] = "NO_DOWNSIZING"

            elif found_downsize_to not in valid_downsizing_targets:
                raise ValueError(
                    f"Invalid downsize_to: {task_data['downsize_to']}. Must be one of {valid_downsizing_targets}"
                )
            else:
                task_dict["downsize_to"] = str(task_data["downsize_to"])

            if (
                str(task_data["preprocessing_method"])
                not in valid_preprocessing_methods
            ):
                raise ValueError(
                    f"Invalid preprocessing_method: {task_data['preprocessing_method']}. Must be one of {valid_preprocessing_methods}"
                )
            task_dict["preprocessing_method"] = str(
                task_data["preprocessing_method"]
            ).lower()
            task_dict["sentrix_ids"] = str(task_data["sentrix_id"])
        except Exception as e:
            raise ValueError(f"Invalid data types in task_data: {e}")

        # instance = super().__new__(cls)
        # instance.update(cast(AnalysisTaskData, task_dict))
        # return instance
        return cast(AnalysisTaskData, task_dict)

    def copy(self) -> "AnalysisTaskData":
        """Return a shallow copy of the task data as AnalysisTaskData."""
        return cast(AnalysisTaskData, type(self)(super().copy()))

    def __str__(self) -> str:
        """Return a user-friendly string representation of the task data."""
        return f"AnalysisTaskData({super().__str__()})"

    def __repr__(self) -> str:
        """Return an unambiguous string representation for debugging."""
        return f"AnalysisTaskData({super().__repr__()})"
