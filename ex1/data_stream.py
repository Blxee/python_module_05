from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return 
Sensor analysis: 3 readings processed, avg temp: 22.5°C


class SensorStream(DataStream):
    pass


class TransactionStream(DataStream):
    pass


class EventStream(DataStream):
    pass


class StreamProcessor:
    pass


def test_Sensor_stream() -> None:
    pass


def test_Transaction_stream() -> None:
    pass


def test_Event_stream() -> None:
    pass


if __name__ == "__main__":
    main()
