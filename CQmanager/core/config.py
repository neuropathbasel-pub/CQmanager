import os
from pathlib import Path

from cnquant_dependencies.enums.ArrayType import ArrayType
from dotenv import load_dotenv
from pydantic import computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    app_name: str = "CQmanager"
    # ===========================================
    # Directory Paths
    # ===========================================
    idat_directory: Path
    diagnoses_directory: Path
    results_directory: Path
    summary_plots_base_directory: Path
    log_directory: Path
    temp_directory: Path
    manifests_directory: Path
    # ===========================================
    # Remote server-specific settings
    # ===========================================
    # Remote server paths
    remote_server_log_directory: Path
    remote_server_idat_directory: Path
    remote_server_diagnoses_directory: Path
    remote_server_results_directory: Path
    remote_server_summary_plots_base_directory: Path
    remote_server_temp_directory: Path
    REMOTE_USER_ID: int = 1000
    REMOTE_GROUP_ID: int = 1000
    # ===========================================
    # Logger settings
    # ===========================================
    log_level: str = "debug"
    # ===========================================
    # Permission settings
    # ===========================================
    LOCAL_USER_ID: int = os.getuid()
    LOCAL_GROUP_ID: int = os.getgid()
    REMOTE_USER_ID: int = os.getgid()
    REMOTE_GROUP_ID: int = os.getuid()
    # ===========================================
    # Data annotation-specific settings
    # ===========================================
    DATA_ANNOTATION_SHEET: str
    REFERENCE_DATA_ANNOTATION_SHEET: str
    sentrix_ids_column_in_annotation_file: str = "Sentrix_id"
    methylation_classes_column_in_annotation_file: str = "MC"
    # ===========================================
    # CQmanager-specific settings
    # ===========================================
    CQmanager_gunicorn_host_address: str = "127.0.0.1"
    CQmanager_gunicorn_port: int = 8002
    CQ_manager_batch_timeout: int = 300
    CQ_manager_batch_size: int = 100
    max_number_of_cqcalc_containers: int = 9
    CQviewers_host: str
    CQviewers_user: str
    CQall_container_name: str = "cqall"
    CQcase_container_name: str = "cqcase"
    cnquant_redis_name: str = "cnquant_redis"
    CQviewers_docker_network_name: str = "cnquant_network"
    base_url_CQviewers: str
    initiate_cqcase_and_cqall_on_startup: bool = True
    run_CQviewers_on_remote_server: bool = True
    notify_if_CQcase_and_CQall_are_not_running: bool = True
    detach_containers: bool = True
    autoremove_containers: bool = True
    rerun_failed_analyses: bool = False
    containers_log_level: str = "info"
    process_not_ready_data_intervals: int = 10
    # ===========================================
    # Email notification settings
    # ===========================================
    crash_email_sender: str = ""
    crash_email_receivers: str = ""
    crash_email_sender_password: str = ""
    # ===========================================
    # CQcalc-specific settings
    # ===========================================
    GAPS_file_name: str = "gaps.csv.gz"
    GENES_file_name: str = "gene_loci.parquet"
    check_if_idats_have_equal_sizes: bool = True
    minimum_idat_size: int = 1
    ENDING_CONTROL_PROBES: str = "_control-probes"
    ZIP_ENDING: str = "_cnv.zip"
    # ===========================================
    # CQcase- and CQall-specific settings
    # ===========================================
    server_name: str
    cqcase_host_app_port: int = 8052
    cqall_host_app_port: int = 8050
    email_notification_port: int = 587
    REDIS_HOST: str = "cnquant_redis"
    REDIS_PORT: int = 8052
    CACHING_DB_cqcase: int = 0
    CACHING_DB_cqall: int = 1
    maximum_number_of_genes_to_plot: int = 600
    workers: int = 1
    timeout: int = 300
    minimal_number_of_sentrix_ids_for_summary_plot: int = 3
    gunicorn_timeout: int = 300
    intervals_for_checking_CQcase_and_CQall_status: int = 1800
    USE_CACHE: bool = False
    CQviewers_docker_network_name: str = "cnquant_network"

    # ===========================================
    # Bins-specific settings
    # ===========================================
    ge_bin_size: int = 1000
    le_bin_size: int = 200_000
    default_bin_size: int = 50_000
    ge_min_probes_per_bin: int = 10
    le_min_probes_per_bin: int = 50
    default_min_probes_per_bin: int = 20
    # ===========================================
    # Docker images names
    # ===========================================
    cqcalc_image: str = "neuropathologiebasel/cqcalc:latest"
    cqcase_image: str = "neuropathologiebasel/cqcase:latest"
    cqall_image: str = "neuropathologiebasel/cqall:latest"
    cqall_plotter_image: str = "neuropathologiebasel/cqall_plotter:latest"
    # ===========================================
    # Manifest URLs
    # ===========================================
    # Local manifests archive and file names
    MANIFEST_ARCHIVE_OR_FILE_NAME_450k: str = "humanmethylation450_15017482_v1-2.csv"
    MANIFEST_ARCHIVE_OR_FILE_NAME_EPIC_v1: str = (
        "infinium-methylationepic-v-1-0-b5-manifest-file-csv.zip"
    )
    MANIFEST_ARCHIVE_OR_FILE_NAME_EPIC_v2: str = (
        "InfiniumMethylationEPICv2.0ProductFiles(ZIPFormat).zip"
    )
    MANIFEST_ARCHIVE_OR_FILE_NAME_MSA48: str = "MSA-48v1-0_20102838_A1.csv"

    MANIFEST_FILE_NAME_450k: str = "humanmethylation450_15017482_v1-2.csv"
    MANIFEST_FILE_NAME_EPIC_v1: str = (
        "infinium-methylationepic-v-1-0-b5-manifest-file.csv"
    )
    MANIFEST_FILE_NAME_EPIC_v2: str = "EPIC-8v2-0_A2.csv"
    MANIFEST_FILE_NAME_MSA48: str = "MSA-48v1-0_20102838_A1.csv"

    ##########################################################
    # Validate paths existences
    @field_validator(
        "idat_directory",
        "diagnoses_directory",
        "results_directory",
        "summary_plots_base_directory",
        "log_directory",
        "temp_directory",
        "manifests_directory",
    )
    @classmethod
    def validate_directory_exists(cls, path: Path) -> Path:
        if not path.exists():
            raise ValueError(f"Directory does not exist: {path}")
        return path

    # ===========================================
    # Computed fields for file paths
    @computed_field
    @property
    def manifests_parquet_directory(self) -> Path:
        directory: Path = self.manifests_directory / Path("manifests_parquet_files")
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    @computed_field
    @property
    def log_file_path(self) -> Path:
        return self.log_directory / "CQmanager.log"

    @computed_field
    @property
    def annotation_file_path(self) -> Path:
        return self.diagnoses_directory / "data_annotation.csv"

    @computed_field
    @property
    def reference_annotation_file_path(self) -> Path:
        return self.diagnoses_directory / "reference_data_annotation.csv"

    @computed_field
    @property
    def MANIFEST_DIR(self) -> Path:
        return self.manifests_directory / "manifest_files_v0"

    @computed_field
    @property
    def DOWNLOAD_DIR(self) -> Path:
        return self.temp_directory / "manifests"

    @computed_field
    @property
    def CNV_GRID(self) -> Path:
        return self.temp_directory / "cnv_grid.json"

    @computed_field
    @property
    def GAPS(self) -> Path:
        return self.manifests_directory / self.GAPS_file_name

    @computed_field
    @property
    def genes_path(self) -> Path:
        return self.manifests_directory / self.GENES_file_name

    @computed_field
    @property
    def remote_annotation_file_path(self) -> Path:
        return self.log_directory / "data_annotation.csv"

    @computed_field
    @property
    def remote_reference_annotation_file_path(self) -> Path:
        return self.log_directory / "reference_data_annotation.csv"

    @computed_field
    @property
    def available_preprocessing_methods(self) -> list[str]:
        return [
            "illumina",
            "swan",
            "noob",
        ]

    @computed_field
    @property
    def send_crash_reports(self) -> bool:
        if any(
            [
                config.crash_email_sender is None,
                config.crash_email_receivers is None,
                config.crash_email_sender_password is None,
            ]
        ) or any(
            [
                len(config.crash_email_sender) <= 0,
                len(config.crash_email_receivers) <= 0,
                len(config.crash_email_sender_password) <= 0,
            ]
        ):
            send_crash_reports: bool = False
        else:
            send_crash_reports: bool = True
        return send_crash_reports

    @computed_field
    @property
    def MANIFEST_FILES_AND_NAMES(self) -> dict[ArrayType, dict[str, str | Path]]:
        archived_manifests_directory: Path = (
            self.manifests_directory / "archived_manifests"
        )

        manifest_files_dictionary: dict[ArrayType, dict[str, str | Path]] = {
            ArrayType.ILLUMINA_450K: {
                "file_path": archived_manifests_directory
                / self.MANIFEST_ARCHIVE_OR_FILE_NAME_450k,
                "manifest_file_name": self.MANIFEST_FILE_NAME_450k,
                "raw_manifest_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_450K.value}_manifest_raw.parquet",
                "manifest_probes_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_450K.value}_manifest_probes.parquet",
                "manifest_controls_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_450K.value}_manifest_controls.parquet",
            },
            ArrayType.ILLUMINA_EPIC: {
                "file_path": archived_manifests_directory
                / self.MANIFEST_ARCHIVE_OR_FILE_NAME_EPIC_v1,
                "manifest_file_name": self.MANIFEST_FILE_NAME_EPIC_v1,
                "raw_manifest_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_EPIC.value}_manifest_raw.parquet",
                "manifest_probes_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_EPIC.value}_manifest_probes.parquet",
                "manifest_controls_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_EPIC.value}_manifest_controls.parquet",
            },
            ArrayType.ILLUMINA_EPIC_V2: {
                "file_path": archived_manifests_directory
                / self.MANIFEST_ARCHIVE_OR_FILE_NAME_EPIC_v2,
                "manifest_file_name": self.MANIFEST_FILE_NAME_EPIC_v2,
                "raw_manifest_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_EPIC_V2.value}_manifest_raw.parquet",
                "manifest_probes_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_EPIC_V2.value}_manifest_probes.parquet",
                "manifest_controls_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_EPIC_V2.value}_manifest_controls.parquet",
            },
            ArrayType.ILLUMINA_MSA48: {
                "file_path": archived_manifests_directory
                / self.MANIFEST_ARCHIVE_OR_FILE_NAME_MSA48,
                "manifest_file_name": self.MANIFEST_FILE_NAME_MSA48,
                "raw_manifest_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_MSA48.value}_manifest_raw.parquet",
                "manifest_probes_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_MSA48.value}_manifest_probes.parquet",
                "manifest_controls_parquet_path": self.manifests_parquet_directory
                / f"{ArrayType.ILLUMINA_MSA48.value}_manifest_controls.parquet",
            },
        }
        return manifest_files_dictionary

    # ===========================================
    # Pydantic Settings
    # ===========================================
    model_config = SettingsConfigDict(
        env_file=[
            ".env",
            "../.env",
        ],
        env_file_encoding="utf-8",
        extra="ignore",
    )


# First search for .env file in the current working directory. If that fails, load .env file from the parent directory
if not load_dotenv():
    fallback_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    load_dotenv(dotenv_path=fallback_path)

config = AppConfig()  # pyright: ignore[reportCallIssue]
