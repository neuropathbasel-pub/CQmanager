import logging
import traceback
from typing import TYPE_CHECKING

import docker
from docker.errors import APIError, NotFound
from docker.types import LogConfig
from icecream import ic
from requests.exceptions import HTTPError

from CQmanager.core.config import AppConfig
from CQmanager.core.logging import logger
from CQmanager.docker_classes.docker_functions import (
    pull_docker_images_if_not_available_locally,
)
from CQmanager.docker_classes.docker_settings import (
    cq_manager_container_prefix,
    cqall_plotter_container_name_prefix,
    cqcalc_and_cqall_plotter_environment_variables,
    cqcalc_and_cqall_plotter_volumes,
)

if TYPE_CHECKING:
    from CQmanager.core.config import AppConfig


class DockerRunner:
    """Class for managing CQcalc and CQall_plotter containers"""

    def __init__(
        self,
        config: "AppConfig",
        logger: logging.Logger = logging.getLogger(name=__name__),
        cqcalc_and_cqall_plotter_volumes: dict[str, dict[str, str]] = (
            cqcalc_and_cqall_plotter_volumes
        ),
        cqcalc_and_cqall_plotter_environment_variables: dict[str, str] = (
            cqcalc_and_cqall_plotter_environment_variables
        ),
    ):
        self.cqcalc_image: str = config.cqcalc_image
        self.cqall_plotter_image: str = config.cqall_plotter_image
        self.cqcalc_and_cqall_plotter_volumes: dict[str, dict[str, str]] = (
            cqcalc_and_cqall_plotter_volumes
        )
        self.cqcalc_and_cqall_plotter_environment_variables: dict[str, str] = (
            cqcalc_and_cqall_plotter_environment_variables
        )
        self.logger = logger
        self.user_id: int = config.LOCAL_USER_ID
        self.group_id: int = config.LOCAL_GROUP_ID
        self.REMOTE_USER_ID: int = config.REMOTE_USER_ID
        self.REMOTE_GROUP_ID: int = config.REMOTE_GROUP_ID
        self.detach_containers = config.detach_containers
        self.autoremove_containers = config.autoremove_containers

    def check_if_docker_images_are_downloaded(self) -> None:
        try:
            client = docker.from_env()

        except Exception:
            error = traceback.format_exc()
            logger.error(msg=f"Error in DockerRunner init.\n:{error}")
            client = None

        if client is not None:
            pull_docker_images_if_not_available_locally(
                client=client, image_name=self.cqcalc_image
            )
            pull_docker_images_if_not_available_locally(
                client=client, image_name=self.cqall_plotter_image
            )

        return None

    def check_running_CNV_containers(self, name_prefix: str):
        """Count running Docker containers with names starting with a given prefix.

        Args:
            name_prefix (str): Prefix to filter container names.

        Returns:
            int: Number of running containers with names starting with the prefix.
        """
        try:
            client = docker.from_env()
            containers_list = client.containers.list(filters={"status": "running"})
            client.close()

            return len(
                [
                    container.name
                    for container in containers_list
                    if container.name.startswith(name_prefix)
                ]
            )
        except Exception:
            return -1

    def is_container_running(self, container_name_or_id: str) -> bool:
        """
        Check if a specific container is running.

        Args:
            container_name_or_id (str): Name or ID of the container.

        Returns:
            bool: True if running, False otherwise (or if not found).
        """
        client = docker.from_env()
        try:
            container = client.containers.get(container_name_or_id)
            container.reload()  # Refresh status from Docker
            return container.status == "running"
        except NotFound:
            self.logger.warning(f"Container {container_name_or_id} not found.")
            return False
        except Exception as e:
            self.logger.error(f"Error checking container {container_name_or_id}: {e}")
            return False
        finally:
            client.close()

    def generate_manifest_parquet_files(
        self,
        execution_command: str,
        log_config: LogConfig,
        container_name: str = "cqcalc_manifest_files_generator",
    ):
        """Generate manifest parquet files using a Docker container."""
        client = docker.from_env()
        try:
            client.containers.run(
                image=self.cqcalc_image,
                command=execution_command,
                name=container_name,
                volumes=self.cqcalc_and_cqall_plotter_volumes,
                detach=self.detach_containers,  # type: ignore
                auto_remove=self.autoremove_containers,
                userns_mode="host",
                stdout=True,
                stderr=True,
                tty=False,
                stdin_open=False,
                environment=self.cqcalc_and_cqall_plotter_environment_variables,
                user=f"{self.user_id}:{self.group_id}",
                log_config=log_config,
                labels={"app": "CQmanager"},
            )  # type: ignore
        except APIError:
            error_string = traceback.format_exc()
            message = f"Failed to run container {container_name}: {error_string}"
            logger.error(msg=message)
        except HTTPError:
            error = traceback.format_exc()
            message = f"Failed to run container {container_name}: {error}"
            logger.error(msg=error)
        except Exception:
            error = traceback.format_exc()
            message = f"Failed to run container {container_name}: {error}"
            logger.error(msg=error)
        client.close()

    def start_analysis_container(
        self,
        execution_command: str,
        log_config: LogConfig,
        container_name: str = "name_not_specified",
    ) -> None:
        """Start a Docker container for analysis with specified configuration.

        Args:
            execution_command (str): Command to execute in the container.
            container_name (str, optional): Name of the container. Defaults to "not_specified_name".

        Returns:
            Container: The running Docker container object.

        Raises:
            APIError: If the container fails to start, logs the error and raises the exception.
        """
        client = docker.from_env()

        try:
            client.containers.run(
                image=self.cqcalc_image,
                command=execution_command,
                name=container_name,
                volumes=self.cqcalc_and_cqall_plotter_volumes,
                detach=self.detach_containers,  # type: ignore
                auto_remove=self.autoremove_containers,
                userns_mode="host",
                stdout=True,
                stderr=True,
                tty=False,
                stdin_open=False,
                environment=self.cqcalc_and_cqall_plotter_environment_variables,
                user=f"{self.user_id}:{self.group_id}",
                log_config=log_config,
                labels={"app": "CQmanager"},
            )  # type: ignore
        except APIError:
            error_string = traceback.format_exc()
            message = f"Failed to run container {container_name}: {error_string}"
            logger.error(msg=message)
        except HTTPError:
            error = traceback.format_exc()
            message = f"Failed to run container {container_name}: {error}"
            logger.error(msg=error)
        except Exception:
            error = traceback.format_exc()
            message = f"Failed to run container {container_name}: {error}"
            logger.error(msg=error)
        client.close()

    def start_cqall_plotter_container(
        self,
        execution_command: str,
        container_name: str,
    ) -> None:
        """Start a Docker container for cqall plotter with specified configuration.

        Args:
            execution_command (str): Command to execute in the container.
            container_name (str): Name of the container.

        Raises:
            APIError: If the container fails to start, logs the error.
        """
        client = docker.from_env()
        try:
            logger.debug(
                msg=f"Starting cqall_plotter container from the image {self.cqall_plotter_image} with execution command: {execution_command}"
            )
            client.containers.run(
                image=self.cqall_plotter_image,
                command=execution_command,
                name=container_name,
                volumes=self.cqcalc_and_cqall_plotter_volumes,
                detach=self.detach_containers,
                auto_remove=self.autoremove_containers,
                userns_mode="host",
                stdout=True,
                stderr=True,
                environment=self.cqcalc_and_cqall_plotter_environment_variables,
                user=f"{self.user_id}:{self.group_id}",
            )
            logger.debug(msg="Started cqall_plotter container")

        except APIError:
            error = traceback.format_exc()
            message = f"Failed to run container {container_name}: {error}"
            logger.error(msg=message)
        except HTTPError:
            error = traceback.format_exc()
            message = f"Failed to run container {container_name}: {error}"
            logger.error(msg=error)
        except Exception:
            error = traceback.format_exc()
            message = f"Failed to run container {container_name}: {error}"
            logger.error(msg=error)
        finally:
            client.close()

    def return_running_containers(
        self, container_name_prefix: str, return_names: bool = False
    ) -> list[str]:
        """Retrieve IDs or names of running Docker containers with a given name prefix.

        Args:
            container_name_prefix (str): Prefix to filter container names.
            return_names (bool, optional): If True, return container names; otherwise, return IDs. Defaults to False.

        Returns:
            list[str]: List of container IDs or names matching the prefix.
        """
        client = docker.from_env()
        containers_list = client.containers.list(filters={"status": "running"})
        if return_names:
            running_containers: list[str] = [
                str(object=container.name)
                for container in containers_list
                if container.name.startswith(container_name_prefix)
            ]
        else:
            running_containers: list[str] = [
                str(object=container.id)
                for container in containers_list
                if container.name.startswith(container_name_prefix)
            ]
        client.close()
        return running_containers

    def stop_analysis_containers(
        self, container_name_prefix: str = cq_manager_container_prefix
    ) -> list[str]:
        """Stop running Docker containers with a given name prefix and return their names.

        Args:
            container_name_prefix (str, optional): Prefix to filter container names. Defaults to cq_manager_container_prefix.

        Returns:
            list[str]: List of names of stopped containers.

        Raises:
            APIError: Logs error if stopping a container fails.
        """
        client = docker.from_env()
        running_containers = client.containers.list(filters={"status": "running"})

        stopped_containers: list[str] = []
        for container in running_containers:
            try:
                if container.name.startswith(container_name_prefix):
                    container.stop()
                    stopped_containers += [str(container.name)]
                    logger.info(
                        msg=f"Stopped container: {container.id} ({container.name})"
                    )
            except APIError:
                error = traceback.format_exc()
                logger.error(msg=error)
            except HTTPError:
                error = traceback.format_exc()
                logger.error(msg=error)
            except Exception:
                error = traceback.format_exc()
                logger.error(msg=error)

        client.close()
        return stopped_containers

    def stop_summary_plotting_container(
        self, container_name=cqall_plotter_container_name_prefix
    ) -> str:
        """Stop running Docker containers with a given name prefix and return status.

        Args:
            container_name (str, optional): Prefix to filter container names. Defaults to cqall_plotter_container_name_prefix.

        Returns:
            str: Status message indicating success, failure, or no matching containers.
        """

        client = docker.from_env()
        containers_list = client.containers.list(filters={"status": "running"})

        container_to_stop_names: list[str] = [
            str(container.name)
            for container in containers_list
            if str(container.name).startswith(container_name)
        ]
        if container_to_stop_names:
            try:
                for container_to_stop in container_to_stop_names:
                    client.containers.get(container_id=container_to_stop).stop()
                return_status: str = (
                    "Stopped all Summary Plotting containers managed by CQmanager"
                )
            except Exception:
                error = traceback.format_exc()
                logger.error(msg=error)
                return_status: str = error

        else:
            return_status: str = (
                "There were no Summary Plotting containers managed by CQmanager"
            )

        client.close()
        return return_status

    def __str__(self):
        return "DockerRunner()"

    def __repr__(self):
        return "DockerRunner()"
