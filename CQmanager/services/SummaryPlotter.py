from CQmanager.core.logging import logger
from CQmanager.docker_classes.docker_settings import (
    cqall_plotter_container_name_prefix,
)
from CQmanager.services.docker_runners import docker_runner


class SummaryPlotter:
    async def start(self):
        logger.info(msg="Starting the Manager of the SummaryPlotter")
        return {"status": "SummaryPlotter manager is ready"}

    async def stop(self):
        docker_runner.stop_summary_plotting_container()
        logger.info(msg="Stopping the Manager of the SummaryPlotter")

    def order_plots(self, task_data: dict):
        execution_command = f"--preprocessing_method {task_data['preprocessing_method']} --methylation_classes {task_data['methylation_classes']} --bin_size {task_data['bin_size']} --min_probes_per_bin {task_data['min_probes_per_bin']} --downsize_to {task_data['downsize_to']}"
        container_name = f"{cqall_plotter_container_name_prefix}_{task_data['timestamp'].replace(':', '-')}"
        docker_runner.start_cqall_plotter_container(
            execution_command=execution_command, container_name=container_name
        )

    def __str__(self):
        return "SummaryPlotter()"

    def __repr__(self):
        return "SummaryPlotter()"
