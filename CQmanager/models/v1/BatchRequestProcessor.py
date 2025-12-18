from collections import defaultdict
from typing import Union

from CQmanager.models.AnalysisTaskData import AnalysisTaskData

# TODO: add functionality to prepare a command with max number of sentrix ids


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
        self.commands = []

    def is_there_any_command(self) -> bool:
        """
        Checks if there are any commands in the central list.

        Returns:
            bool: True if there are commands, False otherwise.
        """
        return len(self.commands) > 0

    def empty_commands(self) -> None:
        """
        Empties the central list of commands.
        """
        self.commands = []

    def add_batch_requests(self, batch_requests: list[AnalysisTaskData]) -> None:
        """
        Processes the given batch requests and adds the resulting commands to the central list.

        Args:
            batch_requests (list[dict]): List of batch request dictionaries to process and add.

        Raises:
            ValueError: If batch_requests is invalid.
        """
        if self.commands:
            self.add_sentrix_ids(new_batch_requests=batch_requests)
        else:
            processed = self._process_requests(batch_requests=batch_requests)
            self.commands.extend(processed)

    def _process_requests(self, batch_requests: list[AnalysisTaskData]) -> list[dict]:
        """
        Groups batch requests by identical parameters (excluding 'sentrix_id' and 'timestamp'),
        collects and sorts sentrix IDs, and returns a list of final analysis commands.

        Args:
            batch_requests (list[dict]): List of request dictionaries.

        Returns:
            list[dict]: List of grouped command dictionaries with 'sentrix_ids' as a comma-separated string.

        Raises:
            ValueError: If batch_requests is empty or contains invalid structures.
        """
        if not batch_requests:
            return []

        groups = defaultdict(list)

        for request in batch_requests:
            if not isinstance(request, dict):
                raise ValueError("Each batch request must be a dictionary.")

            key = tuple(
                sorted(
                    (k, v)
                    for k, v in request.items()
                    if k not in {"sentrix_ids", "timestamp", "number_of_sentrix_ids"}
                )
            )
            sentrix_id = request.get("sentrix_ids")
            if not sentrix_id:
                raise ValueError("Each request must have a 'sentrix_ids' key.")
            groups[key].append(sentrix_id)

        # Build final commands
        final_commands = []
        for key, sentrix_ids in groups.items():
            # Deduplicate and sort sentrix_ids
            unique_sorted_ids = sorted(set(sentrix_ids))
            sentrix_ids_str = ",".join(unique_sorted_ids)

            # Reconstruct the command dict from the key
            command = dict(key)
            command["sentrix_ids"] = sentrix_ids_str
            command["number_of_sentrix_ids"] = len(unique_sorted_ids)
            final_commands.append(command)
        return final_commands

    def add_sentrix_ids(self, new_batch_requests: list[AnalysisTaskData]) -> None:
        """
        Adds sentrix IDs from new batch requests to existing commands in the central list by matching shared parameters.
        Updates 'sentrix_ids' and 'number_of_sentrix_ids' in matching commands. Adds new commands if no match is found.

        Args:
            new_batch_requests (list[dict]): List of new batch request dictionaries to add sentrix IDs from.

        Raises:
            ValueError: If inputs are invalid.
        """
        if not isinstance(new_batch_requests, list):
            raise ValueError("new_batch_requests must be a list.")

        # Create a dict for quick lookup of existing commands by their grouping key
        existing_lookup = {}
        for cmd in self.commands:
            key = tuple(
                sorted(
                    (k, v)
                    for k, v in cmd.items()
                    if k not in {"sentrix_ids", "number_of_sentrix_ids", "timestamp"}
                )
            )
            existing_lookup[key] = cmd

        # Process new requests and merge
        for request in new_batch_requests:
            if not isinstance(request, dict):
                raise ValueError("Each new batch request must be a dictionary.")

            key = tuple(
                sorted(
                    (k, v)
                    for k, v in request.items()
                    if k not in {"sentrix_ids", "timestamp"}
                )
            )
            sentrix_id = request.get("sentrix_ids")
            if not sentrix_id:
                raise ValueError("Each new request must have a 'sentrix_ids'.")

            if key in existing_lookup:
                # Merge into existing command
                cmd = existing_lookup[key]
                existing_ids = set(cmd["sentrix_ids"].split(","))
                existing_ids.add(sentrix_id)
                cmd["sentrix_ids"] = ",".join(sorted(existing_ids))
                cmd["number_of_sentrix_ids"] = len(existing_ids)
            else:
                # Add as new command (if no match)
                new_cmd = dict(key)
                new_cmd["sentrix_ids"] = sentrix_id
                new_cmd["number_of_sentrix_ids"] = 1
                self.commands.append(new_cmd)
                existing_lookup[key] = new_cmd  # Update lookup

        return None

    def pop_exceeding_limit(self, limit: int) -> dict | None:
        """
        Pops and returns the first dictionary from the central list where 'number_of_sentrix_ids'
        is greater than or equal to the specified limit.

        Args:
            limit (int): The threshold for 'number_of_sentrix_ids'.

        Returns:
            dict | None: The popped dictionary if found, else None.

        Raises:
            ValueError: If limit is not a positive integer.
        """
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("Limit must be a positive integer.")

        for i, cmd in enumerate(iterable=self.commands):
            if cmd.get("number_of_sentrix_ids", 0) >= limit:
                return self.commands.pop(i)
        return None

    def pop_element_with_the_highest_number_of_sentrix_ids(self) -> dict | None:
        """
        Pops and returns the first dictionary from the central list where 'number_of_sentrix_ids'
        is greater than or equal to the specified limit.

        Args:
            limit (int): The threshold for 'number_of_sentrix_ids'.

        Returns:
            dict | None: The popped dictionary if found, else None.

        Raises:
            ValueError: If limit is not a positive integer.
        """
        highest_sentrix_id_count = -1
        for i, cmd in enumerate(iterable=self.commands):
            if cmd.get("number_of_sentrix_ids", 0) > highest_sentrix_id_count:
                highest_sentrix_id_count = cmd.get("number_of_sentrix_ids", 0)
        if highest_sentrix_id_count > 0:
            for i, cmd in enumerate(iterable=self.commands):
                if cmd.get("number_of_sentrix_ids", 0) == highest_sentrix_id_count:
                    return self.commands.pop(i)
        return None

    def get_total_number_of_sentrix_ids(self) -> int:
        """
        Returns the total number of sentrix IDs across all commands in the central list.

        Returns:
            int: Total count of sentrix IDs.
        """
        total = 0
        for cmd in self.commands:
            total += cmd.get("number_of_sentrix_ids", 0)
        return total

    def get_highest_number_of_sentrix_ids(self) -> int:
        """
        Returns the highest number of sentrix IDs in any single command in the central list.

        Returns:
            int: Highest count of sentrix IDs in a single command.
        """
        highest_sentrix_id_count = -1
        for cmd in self.commands:
            if cmd.get("number_of_sentrix_ids", 0) > highest_sentrix_id_count:
                highest_sentrix_id_count = cmd.get("number_of_sentrix_ids", 0)
        return highest_sentrix_id_count if highest_sentrix_id_count > 0 else 0

    def split_and_return_command_if_exceeds_limit(
        self, n: int
    ) -> Union[dict[str, Union[str, int]], None]:
        if not isinstance(n, int) or n <= 0:
            raise ValueError("n must be a positive integer.")

        for i, cmd in enumerate(iterable=self.commands):
            if cmd.get("number_of_sentrix_ids", 0) > n:
                sentrix_ids_list = sorted(cmd["sentrix_ids"].split(","))
                new_sentrix_ids = sentrix_ids_list[:n]
                new_cmd = cmd.copy()
                new_cmd["sentrix_ids"] = ",".join(new_sentrix_ids)
                new_cmd["number_of_sentrix_ids"] = len(new_sentrix_ids)

                remaining_sentrix_ids = sentrix_ids_list[n:]
                cmd["sentrix_ids"] = ",".join(remaining_sentrix_ids)
                cmd["number_of_sentrix_ids"] = len(remaining_sentrix_ids)
                return new_cmd
        return None
