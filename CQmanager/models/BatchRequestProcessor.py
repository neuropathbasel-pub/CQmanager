from collections import defaultdict

from CQmanager.models.AnalysisTaskData import AnalysisTaskData


class BatchRequestProcessor:
    """
    Processes and manages a central collection of batch requests by grouping them based on shared parameters,
    aggregating sentrix IDs, and generating final analysis commands. Maintains a poppable central list
    of final commands.

    Attributes:
        final_commands (list[dict]): Central list of processed command dictionaries.

    Methods:
        add_batch_requests(batch_requests): Processes and adds batch requests to the central list.
        add_sentrix_ids(existing_commands, new_batch_requests): Merges sentrix IDs from new requests into existing commands.
        _process_requests(batch_requests): Processes batch requests into grouped commands (helper method).
        pop_exceeding_limit(limit): Pops a command from the central list where 'number_of_sentrix_ids' >= limit.
    """

    def __init__(self):
        self.queue: dict[tuple[int, int, str, str], list[str]] = defaultdict(list)

    def is_the_queue_empty(self) -> bool:
        """
        Checks if there are elements in the queue.

        Returns:
            bool: True if there are commands, False otherwise.
        """
        if not self.queue:
            return False
        else:
            return True

    def empty_queue(self) -> None:
        """
        Empties the central list of commands.
        """
        self.queue: dict[tuple[int, int, str, str], list[str]] = defaultdict(list)
        return None

    def add_batch_requests(self, batch_requests: list[AnalysisTaskData]) -> None:
        """
        Processes the given batch requests and adds the resulting commands to the central list.

        Args:
            batch_requests (list[dict]): List of batch request dictionaries to process and add.

        Raises:
            ValueError: If batch_requests is invalid.
        """
        if batch_requests:
            self.add_to_queue(batch_requests=batch_requests)

    def add_to_queue(self, batch_requests: list[AnalysisTaskData]):
        if not batch_requests:
            return None
        for request in batch_requests:
            if not isinstance(request, dict):
                raise ValueError("Each batch request must be a dictionary.")

            sentrix_id = request.get("sentrix_ids", None)
            min_probes_per_bin = request.get("min_probes_per_bin", 20)
            bin_size = request.get("bin_size", 50000)
            preprocessing_method = request.get("preprocessing_method", "illumina")
            downsize_to = request.get("downsize_to", "NO_DOWNSIZING")

            if sentrix_id is not None:
                key = (
                    int(bin_size),
                    int(min_probes_per_bin),
                    str(preprocessing_method),
                    str(downsize_to),
                )
                self.queue[key].append(str(sentrix_id))

        return None

    def pop_exceeding_limit(
        self, limit: int
    ) -> dict[tuple[int, int, str, str], list[str]] | None:
        """
        Pops and returns the first queue entry where the number of sentrix IDs >= limit.
        Returns None if no such entry exists or limit is invalid.

        Args:
            limit (int): The minimum number of sentrix IDs required.

        Returns:
            dict[tuple[int, int, str, str], list[str]] | None: A dict with the matching key-value pair, or None.

        Raises:
            ValueError: If limit is not a positive integer.
        """
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("Limit must be a positive integer.")

        # Find the first key where len(sentrix_ids) >= limit
        matching_key = next((k for k, v in self.queue.items() if len(v) >= limit), None)
        if matching_key is not None:
            # Create and return the dict, then delete from queue
            return_dict = {matching_key: self.queue[matching_key]}
            del self.queue[matching_key]
            return return_dict
        return None

    def pop_element_with_the_highest_number_of_sentrix_ids(
        self,
    ) -> dict[tuple[int, int, str, str], list[str]] | None:
        """
        Pops and returns the queue entry with the highest number of sentrix IDs.
        If multiple entries have the same count, returns the first one encountered.
        Returns None if the queue is empty or all entries have no sentrix IDs.
        """
        if not self.queue:
            return None

        key_with_max_ids = max(self.queue, key=lambda k: len(self.queue[k]))

        if len(self.queue[key_with_max_ids]) > 0:
            return_dict = {key_with_max_ids: self.queue[key_with_max_ids]}
            del self.queue[key_with_max_ids]
            return return_dict

        return None

    def get_total_number_of_sentrix_ids(self) -> int:
        """
        Returns the total number of sentrix IDs across all batches in the queue.

        Returns:
            int: Total count of sentrix IDs.
        """
        return sum(len(batch) for batch in self.queue.values())

    def get_highest_number_of_sentrix_ids(self) -> int:
        """
        Returns the highest number of sentrix IDs in the central queue.

        Returns:
            int: Highest count of sentrix IDs in a single command.
        """

        return max((len(batch) for batch in self.queue.values()), default=0)

    def split_and_return_command_if_exceeds_limit(
        self, limit: int
    ) -> dict[tuple[int, int, str, str], list[str]] | None:
        """
        Splits and returns the first 'limit' sentrix IDs from the first queue entry where the list length >= limit.
        The remaining sentrix IDs are kept in the queue. Returns None if no such entry exists or limit is invalid.

        Args:
            limit (int): The number of sentrix IDs to split and return.

        Returns:
            dict[tuple[int, int, str, str], list[str]] | None: A dict with the key and the first 'limit' sentrix IDs, or None.

        Raises:
            ValueError: If limit is not a positive integer.
        """
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("Limit must be a positive integer.")

        matching_key = next((k for k, v in self.queue.items() if len(v) >= limit), None)
        if matching_key is not None:
            sentrix_ids = self.queue[matching_key]
            return_ids = sentrix_ids[:limit]
            leftover_ids = sentrix_ids[limit:]

            if leftover_ids:
                self.queue[matching_key] = leftover_ids
            else:
                del self.queue[matching_key]

            return {matching_key: return_ids}

        return None

    def queue_length(self) -> int:
        return sum(len(self.queue[key]) > 0 for key in self.queue)


if __name__ == "__main__":
    from cnquant_dependencies.enums.CommonArrayType import CommonArrayType
    # from icecream import ic

    batch_processor = BatchRequestProcessor()

    batch_requests: list[AnalysisTaskData] = [
        AnalysisTaskData(task)
        for task in [
            {
                "bin_size": 50000,
                "downsize_to": "EPIC_v2_EPIC_v1_to_HM450K",
                "min_probes_per_bin": 20,
                "preprocessing_method": "illumina",
                "sentrix_id": "10003885068_R01C02",
                "timestamp": "2025-10-08_10-08-14",
            },
            {
                "bin_size": 50000,
                "downsize_to": "EPIC_v2_EPIC_v1_to_HM450K",
                "min_probes_per_bin": 20,
                "preprocessing_method": "illumina",
                "sentrix_id": "209548830013_R01C01_1",
                "timestamp": "2025-10-08_10-08-14",
            },
            {
                "bin_size": 40000,
                "downsize_to": "EPIC_v2_EPIC_v1_to_HM450K",
                "min_probes_per_bin": 20,
                "preprocessing_method": "illumina",
                "sentrix_id": "10003885068_R01C02",
                "timestamp": "2025-10-08_10-08-14",
            },
            {
                "bin_size": 50000,
                "downsize_to": "EPIC_v2_EPIC_v1_to_HM450K",
                "min_probes_per_bin": 30,
                "preprocessing_method": "illumina",
                "sentrix_id": "10003885068_R01C02",
                "timestamp": "2025-10-08_10-08-14",
            },
            {
                "bin_size": 50000,
                "downsize_to": "NO_DOWNSIZING",
                "min_probes_per_bin": 20,
                "preprocessing_method": "illumina",
                "sentrix_id": "10003885068_R01C02",
                "timestamp": "2025-10-08_10-08-14",
            },
            {
                "bin_size": 50000,
                "downsize_to": "EPIC_v2_EPIC_v1_to_HM450K",
                "min_probes_per_bin": 20,
                "preprocessing_method": "noob",
                "sentrix_id": "10003885068_R01C02",
                "timestamp": "2025-10-08_10-08-14",
            },
        ]
    ]
    batch_processor.add_batch_requests(batch_requests=batch_requests)
    queue_length = batch_processor.queue_length()
    # ic(queue_length)
