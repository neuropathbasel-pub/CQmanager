from CQmanager.core.config import (
    config,
)

cq_manager_container_prefix: str = "cqmanager"
cqcalc_container_name_prefix: str = f"{cq_manager_container_prefix}_cqcalc"
cqall_plotter_container_name_prefix: str = (
    f"{cq_manager_container_prefix}_cqall_plotter"
)


cqviewers_local_volumes: dict[str, dict[str, str]] = {
    str(object=config.results_directory): {
        "bind": str(object=config.results_directory),
        "mode": "rw",
    },
    str(object=config.summary_plots_base_directory): {
        "bind": str(object=config.summary_plots_base_directory),
        "mode": "rw",
    },
    str(object=config.diagnoses_directory): {
        "bind": str(object=config.diagnoses_directory),
        "mode": "rw",
    },
    str(object=config.temp_directory): {
        "bind": str(object=config.temp_directory),
        "mode": "rw",
    },
    str(object=config.temp_directory): {"bind": "/data/", "mode": "rw"},
    str(object=config.manifests_directory): {
        "bind": str(object=config.manifests_directory),
        "mode": "rw",
    },
    str(object=config.log_directory): {
        "bind": str(object=config.log_directory),
        "mode": "rw",
    },
}

cqviewers_remote_volumes: dict[str, dict[str, str]] = {
    str(object=config.remote_server_results_directory): {
        "bind": str(object=config.remote_server_results_directory),
        "mode": "rw",
    },
    str(object=config.remote_server_summary_plots_base_directory): {
        "bind": str(object=config.remote_server_summary_plots_base_directory),
        "mode": "rw",
    },
    str(object=config.remote_server_diagnoses_directory): {
        "bind": str(object=config.remote_server_diagnoses_directory),
        "mode": "rw",
    },
    str(object=config.remote_server_temp_directory): {
        "bind": str(object=config.remote_server_temp_directory),
        "mode": "rw",
    },
    str(object=config.remote_server_temp_directory): {
        "bind": "/data/",
        "mode": "rw",
    },
    str(object=config.remote_server_log_directory): {
        "bind": str(object=config.remote_server_log_directory),
        "mode": "rw",
    },
}

cqcalc_and_cqall_plotter_volumes: dict[str, dict[str, str]] = {
    str(object=config.idat_directory): {
        "bind": str(config.idat_directory),
        "mode": "rw",
    },
    str(object=config.results_directory): {
        "bind": str(object=config.results_directory),
        "mode": "rw",
    },
    str(object=config.summary_plots_base_directory): {
        "bind": str(config.summary_plots_base_directory),
        "mode": "rw",
    },
    str(object=config.diagnoses_directory): {
        "bind": str(config.diagnoses_directory),
        "mode": "rw",
    },
    str(object=config.temp_directory): {
        "bind": str(config.temp_directory),
        "mode": "rw",
    },
    str(object=config.manifests_directory): {
        "bind": str(config.manifests_directory),
        "mode": "rw",
    },
    str(object=config.log_directory): {"bind": str(config.log_directory), "mode": "rw"},
}

cqcalc_and_cqall_plotter_environment_variables: dict[str, str] = {
    "idat_directory": f"{config.idat_directory}",
    "results_directory": f"{config.results_directory}",
    "summary_plots_base_directory": f"{config.summary_plots_base_directory}",
    "diagnoses_directory": f"{config.diagnoses_directory}",
    "temp_directory": f"{config.temp_directory}",
    "log_directory": f"{config.log_directory}",
    "manifests_directory": f"{config.manifests_directory}",
    "minimum_idat_size": f"{config.minimum_idat_size}",
    "check_if_idats_have_equal_sizes": f"{config.check_if_idats_have_equal_sizes}",
    "minimal_number_of_sentrix_ids_for_summary_plot": f"{config.minimal_number_of_sentrix_ids_for_summary_plot}",
    "maximum_number_of_genes_to_plot": f"{config.maximum_number_of_genes_to_plot}",
    "GAPS_file_name": f"{config.GAPS_file_name}",
    "GENES_file_name": f"{config.GENES_file_name}",
    "MANIFEST_ARCHIVE_OR_FILE_NAME_450k": f"{config.MANIFEST_ARCHIVE_OR_FILE_NAME_450k}",
    "MANIFEST_ARCHIVE_OR_FILE_NAME_EPIC_v1": f"{config.MANIFEST_ARCHIVE_OR_FILE_NAME_EPIC_v1}",
    "MANIFEST_ARCHIVE_OR_FILE_NAME_EPIC_v2": f"{config.MANIFEST_ARCHIVE_OR_FILE_NAME_EPIC_v2}",
    "MANIFEST_ARCHIVE_OR_FILE_NAME_MSA48": f"{config.MANIFEST_ARCHIVE_OR_FILE_NAME_MSA48}",
    "MANIFEST_FILE_NAME_450k": f"{config.MANIFEST_FILE_NAME_450k}",
    "MANIFEST_FILE_NAME_EPIC_v1": f"{config.MANIFEST_FILE_NAME_EPIC_v1}",
    "MANIFEST_FILE_NAME_EPIC_v2": f"{config.MANIFEST_FILE_NAME_EPIC_v2}",
    "MANIFEST_FILE_NAME_MSA48": f"{config.MANIFEST_FILE_NAME_MSA48}",
    "log_level": config.containers_log_level,
    "DATA_ANNOTATION_SHEET": f"{config.DATA_ANNOTATION_SHEET}",
    "REFERENCE_DATA_ANNOTATION_SHEET": f"{config.REFERENCE_DATA_ANNOTATION_SHEET}",
}

cqviewers_local_environment_variables: dict[str, str] = {
    "results_directory": f"{config.results_directory}",
    "summary_plots_base_directory": f"{config.summary_plots_base_directory}",
    "diagnoses_directory": f"{config.diagnoses_directory}",
    "log_directory": f"{config.log_directory}",
    "REDIS_HOST": str(config.REDIS_HOST),
    "REDIS_PORT": str(config.REDIS_PORT),
    "CACHING_DB_cqcase": str(config.CACHING_DB_cqcase),
    "CACHING_DB_cqall": str(config.CACHING_DB_cqall),
    "timeout": str(config.gunicorn_timeout),
    "USE_CACHE": str(config.USE_CACHE),
    "server_name": f"{config.server_name}",
}

cqviewers_remote_environment_variables: dict[str, str] = {
    "results_directory": f"{config.remote_server_results_directory}",
    "summary_plots_base_directory": f"{config.remote_server_summary_plots_base_directory}",
    "diagnoses_directory": f"{config.remote_server_diagnoses_directory}",
    "log_directory": f"{config.remote_server_log_directory}",
    "REDIS_HOST": str(config.REDIS_HOST),
    "REDIS_PORT": str(config.REDIS_PORT),
    "CACHING_DB_cqcase": str(config.CACHING_DB_cqcase),
    "CACHING_DB_cqall": str(config.CACHING_DB_cqall),
    "timeout": str(config.gunicorn_timeout),
    "USE_CACHE": str(config.USE_CACHE),
    "server_name": f"{config.server_name}",
}

cnviewers_images_and_commands: dict[str, dict[str, str | dict[str, int]]] = {
    config.CQcase_container_name: {
        "image": config.cqcase_image,
        "execution_command": f"gunicorn cqcase.app:server --workers={config.workers} --bind=0.0.0.0:{config.cqcase_host_app_port} --name cqcase --timeout {config.gunicorn_timeout}",
        "ports": {f"{config.cqcase_host_app_port}/tcp": config.cqcase_host_app_port},
    },
    config.CQall_container_name: {
        "image": config.cqall_image,
        "execution_command": f"gunicorn cqall.app:server --workers={config.workers} --bind=0.0.0.0:{config.cqall_host_app_port} --name cqall --timeout {config.gunicorn_timeout}",
        "ports": {f"{config.cqall_host_app_port}/tcp": config.cqall_host_app_port},
    },
    config.cnquant_redis_name: {"image": "redis:latest", "execution_command": ""},
}
