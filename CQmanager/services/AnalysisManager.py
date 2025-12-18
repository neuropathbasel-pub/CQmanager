import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Union, cast

from cnquant_dependencies.bin_settings_functions import make_bin_settings_string
from cnquant_dependencies.check_for_missing_files import (
    check_for_missing_manifest_parquet_files,
)
from cnquant_dependencies.enums.ArrayType import ArrayType
from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
from cnquant_dependencies.models.StatusJson import (
    get_status_json_path,
    load_analysis_status_json,
    success_status_string,
)
from cnquant_dependencies.paths_functions import (
    get_sentrix_ids,
)
from docker.types import LogConfig
from icecream import ic

from CQmanager.core.config import config
from CQmanager.docker_classes.docker_settings import (
    cqcalc_container_name_prefix,
)
from CQmanager.endpoint_models.CQdownsizeAnnotatedSamples import (
    CQdownsizeAnnotatedSamples,
)
from CQmanager.endpoint_models.CQmissingSettings import CQmissingSettings
from CQmanager.endpoint_models.CQsettings import CQsettings
from CQmanager.models.AnalysisTaskData import AnalysisTaskData
from CQmanager.models.BatchRequestProcessor import BatchRequestProcessor
from CQmanager.service_helpers.AnalysisManager_helpers import (
    analyze_single_sentrix_id,
    get_annotated_sentrix_ids,
    get_missing_annotated_sentrix_ids_to_analyze,
    get_missing_sentrix_ids_to_analyze,
    get_reference_sentrix_ids,
)
from CQmanager.service_helpers.docker_execution_command import make_an_execution_command
from CQmanager.services.docker_runners import docker_runner


class AnalysisManager:
    def __init__(
        self,
        docker_log_config: LogConfig,
        config=config,
        logger: logging.Logger = logging.getLogger(name=__name__),
        batch_processor: BatchRequestProcessor = BatchRequestProcessor(),
        CQ_manager_batch_size: int = 100,
        CQ_manager_batch_timeout: int = 600,
        process_not_ready_data_intervals: int = 600,
        docker_runner=docker_runner,
    ):
        self._running: bool = False
        self.check_for_not_ready_data: bool = False
        self.logger: logging.Logger = logger
        self.config = config
        self.docker_log_config = docker_log_config
        self.last_processed = time.time()
        self.lock = asyncio.Lock()
        self.batch_processor: BatchRequestProcessor = batch_processor
        self.unique_sentrix_ids_to_analyze_with_downsizing: dict[
            tuple[str, int, int], set[str]
        ] = dict()
        # self.unique_sentrix_ids_to_analyze_with_downsizing: set[str]
        # self._unique_sentrix_ids_to_analyze_with_downsizing: list[AnalysisTaskData] = []
        self.CQ_manager_batch_size = CQ_manager_batch_size
        self.CQ_manager_batch_timeout = CQ_manager_batch_timeout
        self.process_not_ready_data_intervals = process_not_ready_data_intervals
        self.docker_runner = docker_runner
        self.recreate_files = False

    async def check_and_generate_missing_manifest_files(self):
        # Check if the Illumina manifest parquet files are available
        valid_array_types: list[ArrayType] = ArrayType.valid_array_types()
        available_illumina_manifests = [
            array_type
            for array_type in valid_array_types
            if Path(config.MANIFEST_FILES_AND_NAMES[array_type]["file_path"]).exists()
        ]
        if len(available_illumina_manifests) == 0:
            error_message = (
                "No Illumina manifest parquet files are available. At least one is required to run CNV analysis. "
                "Please provide the manifest parquet files and restart CQmanager."
            )
            self.logger.error(msg=error_message)
            raise FileNotFoundError(error_message)

        missing_manifest_parquet_files_for_array_types = (
            check_for_missing_manifest_parquet_files(
                manifest_files_and_names=config.MANIFEST_FILES_AND_NAMES,
            )
        )
        if (
            len(missing_manifest_parquet_files_for_array_types) > 0
            or not config.genes_path.exists()
        ):
            message: str = f"CQmanager has detected that {len(missing_manifest_parquet_files_for_array_types)} array types are missing their manifest parquet files: {[array_type.value for array_type in missing_manifest_parquet_files_for_array_types]}. The files will be generated shortly. Any analysis tasks will be started once the files are available."
            self.logger.info(msg=message)
            execution_command: str = f"prepare_missing_manifest_parquet_files --recreate_files {str(self.recreate_files)}"
            container_name: str = "cqcalc_manifest_files_generator"

            if not self.docker_runner.is_container_running(
                container_name_or_id=container_name
            ):
                self.docker_runner.generate_manifest_parquet_files(
                    execution_command=execution_command,
                    log_config=self.docker_log_config,
                    container_name=container_name,
                )
                await asyncio.sleep(delay=2)
            else:
                self.logger.info(
                    msg="Manifest parquet file generation container is already running."
                )
            # FIXME: Add timeout handling
            timeout_counter: int = 0
            max_timeout: int = 1000
            while (
                self.docker_runner.is_container_running(
                    container_name_or_id=container_name
                )
                and timeout_counter < max_timeout
            ):
                await asyncio.sleep(delay=10)
                timeout_counter += 10

            if timeout_counter >= max_timeout:
                message = (
                    "Timeout waiting for manifest file generation container to finish."
                )
                self.logger.error(msg=message)
                raise TimeoutError(message)

            if (
                len(
                    check_for_missing_manifest_parquet_files(
                        manifest_files_and_names=config.MANIFEST_FILES_AND_NAMES,
                    )
                )
                == 0
            ):
                self.logger.info(
                    msg="Required manifest parquet files are now available."
                )
            else:
                message = "Failed to generate required manifest parquet files. CQmanager cannot proceed without them."
                self.logger.error(msg=message)
                raise FileNotFoundError(message)

        else:
            message = "All required manifest parquet files are available."

        return {"status": message}

    async def start(self):
        self.logger.info(msg="Starting AnalysisManager")
        self.logger.info(msg="Checking for required manifest parquet files")

        self._running = True

        asyncio.create_task(coro=self.process_batch_task())
        self.check_for_not_ready_data = True
        asyncio.create_task(coro=self.process_not_ready_data())
        return {"status": "AnalysisManager is ready"}

    async def stop(self):
        self.logger.info(msg="Stopping AnalysisManager")
        self._running = False
        self.check_for_not_ready_data = False
        # TODO: Decide what to do on exit

        await asyncio.sleep(delay=1)
        self.logger.info(msg="AnalysisManager stopped.")

    async def put_task(self, task_data: dict) -> None:
        self.logger.debug(msg=f"Received task data: {task_data}")
        if isinstance(task_data, CQsettings):
            sentrix_ids_to_analyze = await analyze_single_sentrix_id(
                task_data=task_data
            )
            self.batch_processor.add_batch_requests(
                batch_requests=sentrix_ids_to_analyze
            )

        elif isinstance(task_data, CQmissingSettings):
            sentrix_ids_to_analyze = await get_missing_sentrix_ids_to_analyze(
                task_data=task_data, config=config, downsize_to=task_data.downsize_to
            )
            self.batch_processor.add_batch_requests(
                batch_requests=sentrix_ids_to_analyze
            )
        elif isinstance(task_data, CQdownsizeAnnotatedSamples):
            # FIXME: This is not working properly yet
            # sentrix_ids_to_analyze = await get_missing_sentrix_ids_to_analyze(
            #     task_data=task_data, config=config
            # )
            # Get missing non-downsized sentrix IDs to process
            non_reduced_samples_to_analyze = (
                await get_missing_annotated_sentrix_ids_to_analyze(
                    task_data=task_data,
                    config=config,
                    downsize_to=CommonArrayType.NO_DOWNSIZING.value,
                )
            )
            self.batch_processor.add_batch_requests(
                batch_requests=non_reduced_samples_to_analyze
            )
            # Get annotated, non-reference Sentrix IDs to process with downsizing

            annotated_sentrix_ids: set[str] = get_annotated_sentrix_ids(
                config=config
            ) - get_reference_sentrix_ids(config=config)

            # preprocessing_method = task_data["preprocessing_method"]
            # bin_size = task_data["bin_size"]
            # min_probes_per_bin = task_data["min_probes_per_bin"]

            # current_key = tuple(
            #     [
            #         task_data["preprocessing_method"],
            #         task_data["bin_size"],
            #         task_data["min_probes_per_bin"],
            #     ]
            # )

            current_key: tuple[str, int, int] = (
                task_data["preprocessing_method"],
                task_data["bin_size"],
                task_data["min_probes_per_bin"],
            )

            async with self.lock:  # Lock the list modification
                if (
                    self.unique_sentrix_ids_to_analyze_with_downsizing.get(
                        current_key,
                        None,
                    )
                    is None
                ):
                    self.unique_sentrix_ids_to_analyze_with_downsizing[current_key] = (
                        annotated_sentrix_ids
                    )
                else:
                    self.unique_sentrix_ids_to_analyze_with_downsizing[current_key] |= (
                        annotated_sentrix_ids
                    )

        return None

    async def process_not_ready_data(self):
        # FIXME: This will be an endless loop due to annotated sentrix ids that are not there
        valid_array_types: list[str] = [
            array_type.value for array_type in ArrayType.valid_array_types()
        ]
        self.logger.info(msg="Not ready data processing loop started")
        while self.check_for_not_ready_data:
            await asyncio.sleep(delay=self.process_not_ready_data_intervals)
            async with self.lock:
                analysis_settings = list(
                    self.unique_sentrix_ids_to_analyze_with_downsizing.keys()
                )

                if len(analysis_settings) != 0:
                    self.logger.debug(
                        msg=f"Processing not ready data for settings: {analysis_settings}"
                    )
                    for settings in analysis_settings:
                        sentrix_ids_to_remove: set[str] = set()
                        preprocessing_method: str = settings[0]
                        bin_size: int = settings[1]
                        min_probes_per_bin: int = settings[2]

                        # Get available Sentrix IDs without downsizing
                        for sentrix_id in list(
                            self.unique_sentrix_ids_to_analyze_with_downsizing[settings]
                        ):
                            sentrix_id_directory: Path = (
                                self.config.results_directory
                                / preprocessing_method
                                / make_bin_settings_string(
                                    bin_size=bin_size,
                                    min_probes_per_bin=min_probes_per_bin,
                                )
                                / sentrix_id
                            )
                            non_downsized_status_json_path: Path = get_status_json_path(
                                sentrix_id=sentrix_id,
                                sentrix_id_directory=sentrix_id_directory,
                                downsize_to=CommonArrayType.NO_DOWNSIZING.value,
                            )
                            if not non_downsized_status_json_path.exists():
                                continue

                            non_downsized_status_json = load_analysis_status_json(
                                status_json_path=non_downsized_status_json_path
                            )
                            detected_array_type: Optional[str] = cast(
                                Optional[str],
                                non_downsized_status_json.get("array_type", None),
                            )
                            if (
                                detected_array_type is not None
                                and detected_array_type in valid_array_types
                            ):
                                available_downsizing_targets: list[
                                    "CommonArrayType"
                                ] = CommonArrayType.available_downsizing_targets(
                                    array_type=detected_array_type
                                )
                                for downsizing_target in available_downsizing_targets:
                                    target_status_json_path: Path = (
                                        get_status_json_path(
                                            sentrix_id=sentrix_id,
                                            sentrix_id_directory=sentrix_id_directory,
                                            downsize_to=downsizing_target.value,
                                        )
                                    )
                                    if target_status_json_path.exists():
                                        sentrix_ids_to_remove.add(sentrix_id)
                                    else:
                                        new_batch_request = AnalysisTaskData(
                                            {
                                                "sentrix_id": sentrix_id,
                                                "preprocessing_method": preprocessing_method,
                                                "bin_size": bin_size,
                                                "min_probes_per_bin": min_probes_per_bin,
                                                "downsize_to": downsizing_target.value,
                                            }
                                        )

                                        self.batch_processor.add_batch_requests(
                                            batch_requests=[new_batch_request]
                                        )
                            else:
                                sentrix_ids_to_remove.add(sentrix_id)

                        else:
                            self.unique_sentrix_ids_to_analyze_with_downsizing[
                                settings
                            ] -= sentrix_ids_to_remove

        return None

    async def process_batch_task(self, turned_off: bool = False):
        self.logger.info(msg="Batch processing loop started")
        delay: int = 10
        # TODO: Do not even start it if the files are missing
        # manifest_parquet_files_provided: bool = False
        # missing_manifest_parquet_files_for_array_types = (
        #     check_for_missing_manifest_parquet_files(
        #         manifest_files_and_names=config.MANIFEST_FILES_AND_NAMES,
        #     )
        # )
        # # FIXME
        # manifest_parquet_files_provided = True
        while self._running:
            await asyncio.sleep(delay=delay)

            time_elapsed = time.time() - self.last_processed
            async with self.lock:
                number_of_currently_running_cqcalc_containers = (
                    self.docker_runner.check_running_CNV_containers(
                        name_prefix=cqcalc_container_name_prefix
                    )
                )
                if number_of_currently_running_cqcalc_containers == -1:
                    self.logger.error(
                        msg="Error checking running cqcalc containers. Will retry in 10 to 15 seconds."
                    )
                    await asyncio.sleep(delay=delay)
                    continue
                if (
                    number_of_currently_running_cqcalc_containers
                    >= self.config.max_number_of_cqcalc_containers
                ):
                    self.logger.debug(
                        "Currently running maximum number of cqcalc containers"
                    )
                    await asyncio.sleep(delay=delay)
                    continue
                # The case that there is a batch with more than CQ_manager_batch_size sentrix IDs
                if (
                    self.batch_processor.get_highest_number_of_sentrix_ids()
                    >= self.CQ_manager_batch_size
                ):
                    self.logger.debug(
                        "Submitting a batch based on CQ_manager_batch_size"
                    )
                    command_dictionary: Optional[
                        dict[tuple[int, int, str, str], list[str]]
                    ] = self.batch_processor.split_and_return_command_if_exceeds_limit(
                        limit=self.CQ_manager_batch_size
                    )
                    if command_dictionary is not None:
                        number_of_sentrix_ids = len(list(command_dictionary.keys())[0])
                    else:
                        self.logger.debug("No command dictionary found after splitting")
                        await asyncio.sleep(delay=delay)
                        continue
                # The case that the time passed since last submission is greater than CQ_manager_batch_timeout
                elif (
                    self.batch_processor.get_highest_number_of_sentrix_ids() > 0
                    and time_elapsed >= self.CQ_manager_batch_timeout
                ):
                    self.logger.debug(
                        "Submitting a batch based on CQ_manager_batch_timeout"
                    )
                    self.logger.debug(
                        "Popping commands from the BatchRequestsProcessor"
                    )
                    command_dictionary: Optional[
                        dict[tuple[int, int, str, str], list[str]]
                    ] = self.batch_processor.pop_element_with_the_highest_number_of_sentrix_ids()
                    if command_dictionary is not None:
                        number_of_sentrix_ids = len(list(command_dictionary.keys())[0])
                    else:
                        continue
                else:
                    self.logger.debug("No batch to submit")
                    command_dictionary = None
                    number_of_sentrix_ids = 0
                    await asyncio.sleep(delay=delay)
                    continue

                if command_dictionary is not None:
                    self.logger.debug("Starting a new container")
                    self.last_processed = time.time()
                    container_name: str = f"{cqcalc_container_name_prefix}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
                    execution_command = f"cqcalc --analysis_command {make_an_execution_command(batch=command_dictionary)}"
                    if turned_off:
                        continue
                    self.docker_runner.start_analysis_container(
                        execution_command=execution_command,
                        container_name=container_name,
                        log_config=self.docker_log_config,
                    )

                    key_elements = tuple(command_dictionary)[0]

                    if key_elements[3] == "NO_DOWNSIZING":
                        downsizing_string = "no downsizing"
                    else:
                        downsizing_string = f"{key_elements[3]} downsizing method"
                    preprocessing_method = key_elements[2]
                    bin_size = key_elements[0]
                    min_probes_per_bin = key_elements[1]

                    self.logger.info(
                        msg=f"{datetime.now().strftime('%Y-%m-%d:%H:%M:%S')}: Submitted a batch of {number_of_sentrix_ids} Sentrix IDs for CNV analysis with {preprocessing_method} preprocessing, bin sizes of {bin_size}, minimum probes per bin of {min_probes_per_bin} and {downsizing_string}"
                    )
                    continue

        self.logger.info(msg="Batch processing loop stopped")


# backup

# async def process_not_ready_data(self):
#         success_status_string: str = "analysis_completed_successfully"
#         valid_array_types: list[str] = [
#             array_type.value for array_type in ArrayType.valid_array_types()
#         ]
#         self.logger.info(msg="Not ready data processing loop started")
#         while self.check_for_not_ready_data:
#             await asyncio.sleep(delay=self.process_not_ready_data_intervals)
#             async with self.lock:
#                 if self._unique_sentrix_ids_to_analyze_with_downsizing:
#                     list_indices_to_remove: list[int] = []
#                     for i, element in enumerate(
#                         self._unique_sentrix_ids_to_analyze_with_downsizing
#                     ):
#                         sentrix_id: str = element["sentrix_ids"]

#                         preprocessing_method: str = element["preprocessing_method"]
#                         sentrix_id_directory: Path = (
#                             self.config.results_directory
#                             / preprocessing_method
#                             / make_bin_settings_string(
#                                 bin_size=element["bin_size"],
#                                 min_probes_per_bin=element["min_probes_per_bin"],
#                             )
#                             / sentrix_id
#                         )
#                         no_downsizing_status_json_path = get_status_json_path(
#                             sentrix_id=sentrix_id,
#                             sentrix_id_directory=sentrix_id_directory,
#                             downsize_to=CommonArrayType.NO_DOWNSIZING.value,
#                         )

#                         no_downsizing_status_json = load_analysis_status_json(
#                             status_json_path=no_downsizing_status_json_path
#                         )
#                         if no_downsizing_status_json:
#                             detected_array_type = no_downsizing_status_json.get(
#                                 "array_type", "UNKNOWN"
#                             )
#                             if detected_array_type not in valid_array_types:
#                                 list_indices_to_remove.append(i)
#                                 continue
#                             available_downsizing_targets: list[str] = []
#                             available_task_data: list[AnalysisTaskData] = []
#                             if no_downsizing_status_json.get(
#                                 success_status_string, False
#                             ):
#                                 available_downsizing_targets += [
#                                     target.value
#                                     for target in CommonArrayType.available_downsizing_targets(
#                                         array_type=detected_array_type
#                                     )
#                                 ]
#                                 for target in available_downsizing_targets:
#                                     target_data_available: list[bool] = []
#                                     target_status_json_path = get_status_json_path(
#                                         sentrix_id=sentrix_id,
#                                         sentrix_id_directory=sentrix_id_directory,
#                                         downsize_to=target,
#                                     )
#                                     target_status_json = load_analysis_status_json(
#                                         status_json_path=target_status_json_path
#                                     )
#                                     if target_status_json:
#                                         if not target_status_json.get(
#                                             success_status_string, False
#                                         ):
#                                             downsizing_task_data: AnalysisTaskData = (
#                                                 element.copy()
#                                             )
#                                             downsizing_task_data["downsize_to"] = target
#                                             downsizing_task_data = cast(
#                                                 AnalysisTaskData, downsizing_task_data
#                                             )
#                                             available_task_data += [
#                                                 downsizing_task_data
#                                             ]
#                                         else:
#                                             target_data_available += [True]

#                                     else:
#                                         downsizing_task_data: AnalysisTaskData = (
#                                             element.copy()
#                                         )
#                                         downsizing_task_data["downsize_to"] = target
#                                         downsizing_task_data = cast(
#                                             AnalysisTaskData, downsizing_task_data
#                                         )
#                                         available_task_data += [downsizing_task_data]

#                                     list_indices_to_remove.append(i)
#                                 self.batch_processor.add_batch_requests(
#                                     batch_requests=available_task_data
#                                 )
#                             else:
#                                 list_indices_to_remove.append(i)

#                     list_indices_to_remove.sort(reverse=True)
#                     for index in list_indices_to_remove:
#                         del self._unique_sentrix_ids_to_analyze_with_downsizing[index]
#                 else:
#                     self.logger.debug("No reduced data to process")

#         return None
