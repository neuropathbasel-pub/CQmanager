from cnquant_dependencies.blacklists import blacklisted_methylation_classes
from cnquant_dependencies.models.AnnotatedCasesLoader import AnnotatedCasesLoader

from CQmanager.core.config import config
from CQmanager.core.logging import docker_log_config, logger
from CQmanager.services.AnalysisManager import AnalysisManager
from CQmanager.services.Cooldown import Cooldown
from CQmanager.services.FileCleaner import FileCleaner
from CQmanager.services.SummaryPlotter import SummaryPlotter
from CQmanager.services.TaskQueue import TaskQueue

annotated_cases_loader = AnnotatedCasesLoader(
    annotation_file_path=config.annotation_file_path,
    sentrix_ids_column_in_annotation_file=config.sentrix_ids_column_in_annotation_file,
    methylation_classes_column_in_annotation_file=config.methylation_classes_column_in_annotation_file,
    blacklisted_methylation_classes=blacklisted_methylation_classes,
    logger=logger,
)
task_queuer = TaskQueue()

summary_plotter = SummaryPlotter()
analysis_manager = AnalysisManager(
    logger=logger,
    annotated_cases_loader=annotated_cases_loader,
    config=config,
    docker_log_config=docker_log_config,
    CQmanager_batch_size=config.CQmanager_batch_size,
    CQmanager_batch_timeout=config.CQmanager_batch_timeout,
    # process_not_ready_data_intervals=config.process_not_ready_data_intervals,
)
file_cleaner = FileCleaner(
    results_directory=config.results_directory,
    temp_directory=config.temp_directory,
    logger=logger,
)
cooldown_manager = Cooldown(
    cooldown_interval=config.endpoint_request_cooldown_interval, logger=logger
)
