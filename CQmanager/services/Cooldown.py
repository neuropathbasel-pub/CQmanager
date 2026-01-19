import logging
import time


class Cooldown:
    def __init__(
        self,
        cooldown_interval: int,
        logger: logging.Logger = logging.getLogger(name=__name__),
    ):
        self.logger: logging.Logger = logger
        self.cooldown_interval: int = cooldown_interval
        self.endpoint_cooldowns: dict[str, int] = {}
        self.logger.debug(msg=f"{self.__class__.__name__} instance created")

    def is_on_cooldown(self, endpoint_name: str) -> bool:
        if self.endpoint_cooldowns.get(endpoint_name) is None:
            self.endpoint_cooldowns[endpoint_name] = 0
            return False

        current_time: int = int(time.time())
        last_time = self.endpoint_cooldowns[endpoint_name]
        on_cooldown = (current_time - last_time) < self.cooldown_interval
        self.logger.debug(
            msg=f"Endpoint '{endpoint_name}': current={current_time}, last={last_time}, interval={self.cooldown_interval}, on_cooldown={on_cooldown}"
        )

        return on_cooldown

    def update_last_request_time(self, endpoint_name: str) -> None:
        if self.endpoint_cooldowns.get(endpoint_name) is None:
            self.endpoint_cooldowns[endpoint_name] = 0
            return
        current_time: int = int(time.time())
        self.endpoint_cooldowns[endpoint_name] = current_time
        return None

    def return_remaining_time(self, endpoint_name: str) -> int:
        if self.endpoint_cooldowns.get(endpoint_name) is None:
            self.logger.warning(
                msg=f"Cooldown remaining time check for unknown endpoint '{endpoint_name}'."
            )
            return 0

        current_time: int = int(time.time())
        elapsed_time: int = current_time - self.endpoint_cooldowns[endpoint_name]
        remaining_time: int = self.cooldown_interval - elapsed_time
        return max(0, remaining_time)
