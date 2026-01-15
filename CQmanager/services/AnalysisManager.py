import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from cnquant_dependencies.bin_settings_functions import make_bin_settings_string
from cnquant_dependencies.check_for_missing_files import (
    check_for_missing_manifest_parquet_files,
)
from cnquant_dependencies.enums.ArrayType import ArrayType
from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
from cnquant_dependencies.models.AnnotatedCasesLoader import AnnotatedCasesLoader
from docker.types import LogConfig

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
    get_missing_sentrix_ids_to_analyze,
)
from CQmanager.service_helpers.docker_execution_command import make_an_execution_command
from CQmanager.services.docker_runners import docker_runner
from CQmanager.supporting_scripts_for_downsizing import (
    append_missing_info_to_the_analyzed_sentrix_ids_dictionary,
    get_analyzed_sentrix_ids_dictionary,
    higher_probes_downsizing_targets,
)


class AnalysisManager:
    def __init__(
        self,
        docker_log_config: LogConfig,
        annotated_cases_loader: AnnotatedCasesLoader,
        config=config,
        logger: logging.Logger = logging.getLogger(name=__name__),
        batch_processor: BatchRequestProcessor = BatchRequestProcessor(),
        CQ_manager_batch_size: int = 100,
        CQ_manager_batch_timeout: int = 600,
        docker_runner=docker_runner,
        recreate_files: bool = False,
    ):
        self._running: bool = False
        self.check_for_not_ready_data: bool = False
        self.logger: logging.Logger = logger
        self.config = config
        self.docker_log_config: LogConfig = docker_log_config
        self.last_processed = time.time()
        self.lock = asyncio.Lock()
        self.batch_processor: BatchRequestProcessor = batch_processor
        self.annotated_cases_loader = annotated_cases_loader
        self.CQ_manager_batch_size = CQ_manager_batch_size
        self.CQ_manager_batch_timeout = CQ_manager_batch_timeout
        self.docker_runner = docker_runner
        self.recreate_files: bool = recreate_files

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

        asyncio.create_task(
            coro=self.process_batch_task(turned_off=False)
        )  # This flag is for testing purposes only
        self.check_for_not_ready_data = True
        return {"status": "AnalysisManager is ready"}

    async def stop(self):
        self.logger.info(msg="Stopping AnalysisManager")
        self._running = False
        self.check_for_not_ready_data = False
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
            # If this is crashing the workers in the future, put it on a separate thread
            sentrix_ids_to_analyze = await get_missing_sentrix_ids_to_analyze(
                task_data=task_data, config=config, downsize_to=task_data.downsize_to
            )
            self.batch_processor.add_batch_requests(
                batch_requests=sentrix_ids_to_analyze
            )
        elif isinstance(task_data, CQdownsizeAnnotatedSamples):
            asyncio.create_task(coro=self._process_downsize_task(task_data=task_data))
            return None

        return None

    async def process_batch_task(self, turned_off: bool = False):
        self.logger.info(msg="Batch processing loop started")
        delay: int = 10
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
                        msg="Currently running maximum number of cqcalc containers"
                    )
                    await asyncio.sleep(delay=delay)
                    continue
                # The case that there is a batch with more than CQ_manager_batch_size sentrix IDs
                if (
                    self.batch_processor.get_highest_number_of_sentrix_ids()
                    >= self.CQ_manager_batch_size
                ):
                    self.logger.debug(
                        msg="Submitting a batch based on CQ_manager_batch_size"
                    )
                    command_dictionary: Optional[
                        dict[tuple[int, int, str, str], list[str]]
                    ] = self.batch_processor.split_and_return_command_if_exceeds_limit(
                        limit=self.CQ_manager_batch_size
                    )
                    if command_dictionary is None:
                        self.logger.debug("No command dictionary found after splitting")
                        await asyncio.sleep(delay=delay)
                        continue

                # The case that the time passed since last submission is greater than CQ_manager_batch_timeout
                elif (
                    self.batch_processor.get_highest_number_of_sentrix_ids() > 0
                    and time_elapsed >= self.CQ_manager_batch_timeout
                ):
                    self.logger.debug(
                        msg="Submitting a batch based on CQ_manager_batch_timeout"
                    )
                    self.logger.debug(
                        msg="Popping commands from the BatchRequestsProcessor"
                    )
                    command_dictionary: Optional[
                        dict[tuple[int, int, str, str], list[str]]
                    ] = self.batch_processor.pop_element_with_the_highest_number_of_sentrix_ids()

                    if command_dictionary is None:
                        continue

                else:
                    self.logger.debug(msg="No batch to submit")
                    command_dictionary = None
                    number_of_sentrix_ids = 0
                    await asyncio.sleep(delay=delay)
                    continue

                if command_dictionary is not None:
                    self.logger.debug(msg="Starting a new container")
                    self.last_processed = time.time()
                    container_name: str = f"{cqcalc_container_name_prefix}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
                    execution_command = f"cqcalc --analysis_command {make_an_execution_command(batch=command_dictionary)}"
                    number_of_sentrix_ids: int = 0
                    for key in command_dictionary.keys():
                        number_of_sentrix_ids += len(command_dictionary[key])

                    if not turned_off:
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
                        msg=f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}: Submitted a batch of {number_of_sentrix_ids} Sentrix IDs for CNV analysis with {preprocessing_method} preprocessing, bin sizes of {bin_size}, minimum probes per bin of {min_probes_per_bin} and {downsizing_string}"
                    )
                    continue

        self.logger.info(msg="Batch processing loop stopped")

    async def _process_downsize_task(
        self, task_data: CQdownsizeAnnotatedSamples
    ) -> None:
        try:
            await asyncio.to_thread(self._compute_downsize_tasks, task_data)
        except Exception as e:
            self.logger.error(msg=f"Error in _process_downsize_task: {e}")

    def _compute_downsize_tasks(self, task_data: CQdownsizeAnnotatedSamples) -> None:
        all_methylation_classes: list[str] = (
            self.annotated_cases_loader.get_methylation_classes_selection()
        )
        for methylation_class in all_methylation_classes:
            self.logger.debug(
                msg=f"Analyzing missing samples for the summary plots with CQcalc.\nCurrently processing methylation class: {methylation_class}"
            )
            list_of_analysis_tasks: list[AnalysisTaskData] = []
            suggested_downsizing_targets: list[CommonArrayType] = [
                CommonArrayType.NO_DOWNSIZING
            ]

            annotated_sentrix_ids: list[str] = (
                self.annotated_cases_loader.get_annotated_sentrix_ids(
                    methylation_classes_selection=[methylation_class]
                )
            )

            analyzed_sentrix_ids_dictionary: dict[
                str, dict[str, dict[str, ArrayType | dict[CommonArrayType, bool]]]
            ] = get_analyzed_sentrix_ids_dictionary(
                analyzed_sentrix_ids_directory=config.results_directory
                / task_data.preprocessing_method
                / make_bin_settings_string(bin_size=50000, min_probes_per_bin=20),
                list_of_sentrix_ids_to_consider=annotated_sentrix_ids,
                logger=self.logger,
            )

            missing_sentrix_ids = set(annotated_sentrix_ids) - set(
                analyzed_sentrix_ids_dictionary.keys()
            )

            analyzed_sentrix_ids_dictionary = (
                append_missing_info_to_the_analyzed_sentrix_ids_dictionary(
                    missing_sentrix_ids=missing_sentrix_ids,
                    analyzed_sentrix_ids_dictionary=analyzed_sentrix_ids_dictionary,
                    idat_directory=config.idat_directory,
                    logger=self.logger,
                )
            )

            all_array_types_in_analyzed_dictionary: set[ArrayType] = set()
            for (
                sentrix_id,
                analysis_info,
            ) in analyzed_sentrix_ids_dictionary.items():
                try:
                    all_array_types_in_analyzed_dictionary.add(
                        ArrayType(value=analysis_info.get("array_type"))
                    )
                except Exception:
                    pass

            downsizing_target_members = {
                member: set(CommonArrayType.get_array_types(convert_from_to=member))
                for member in CommonArrayType.get_members()
                if member != CommonArrayType.NO_DOWNSIZING
            }

            matching_targets = [
                (key, value_set)
                for key, value_set in downsizing_target_members.items()
                if all_array_types_in_analyzed_dictionary.issubset(value_set)
            ]

            if matching_targets:
                matching_targets.sort(key=lambda x: len(x[1]))
                matching_target = matching_targets[0][0]
                possible_downsizing_targets = higher_probes_downsizing_targets.get(
                    matching_target, None
                )
                if possible_downsizing_targets is not None:
                    suggested_downsizing_targets.extend(possible_downsizing_targets)

            if suggested_downsizing_targets is not None:
                for target in suggested_downsizing_targets:
                    for sentrix_id in analyzed_sentrix_ids_dictionary.keys():
                        analysis_info = analyzed_sentrix_ids_dictionary[sentrix_id]
                        downsizing_targets = analysis_info.get("downsizing_targets", {})
                        if target not in downsizing_targets:
                            list_of_analysis_tasks.append(
                                AnalysisTaskData(
                                    task_data={
                                        "sentrix_id": sentrix_id,
                                        "preprocessing_method": task_data.preprocessing_method,
                                        "bin_size": task_data.bin_size,
                                        "min_probes_per_bin": task_data.min_probes_per_bin,
                                        "downsize_to": target.value,
                                    }
                                )
                            )

            if list_of_analysis_tasks:
                self.batch_processor.add_batch_requests(
                    batch_requests=list_of_analysis_tasks
                )
                self.logger.info(
                    msg=f"Added {len(list_of_analysis_tasks)} tasks for methylation class '{methylation_class}' to the batch processor."
                )
