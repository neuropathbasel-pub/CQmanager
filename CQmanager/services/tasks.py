from CQmanager.core.config import config
from CQmanager.core.logging import docker_log_config, logger
from CQmanager.docker_classes.CQviewersRunner import CQviewersRunner
from CQmanager.docker_classes.DockerRunner import DockerRunner
from CQmanager.services.AnalysisManager import AnalysisManager
from CQmanager.services.SummaryPlotter import SummaryPlotter
from CQmanager.services.TaskQueue import TaskQueue

task_queuer = TaskQueue()

summary_plotter = SummaryPlotter()
analysis_manager = AnalysisManager(
    logger=logger,
    config=config,
    docker_log_config=docker_log_config,
    CQ_manager_batch_size=config.CQ_manager_batch_size,
    CQ_manager_batch_timeout=config.CQ_manager_batch_timeout,
    process_not_ready_data_intervals=config.process_not_ready_data_intervals,
)


