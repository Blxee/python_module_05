from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Set, Tuple
import sys


class DataStream(ABC):
    """Abstract class of all streams."""

    type: str
    unit: str
    data_type: str
    possible_fields: Set[str]

    def __init__(self, id: str) -> None:
        """Initialize DataStream with id."""
        self.id = id
        self.data: Dict[Any, Any] = {}

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Abstact method to process data with this stream."""
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter data using specific criteria."""
        if criteria is None:
            return data_batch
        if not isinstance(data_batch, list) or any(
            (not isinstance(i, str) for i in data_batch)
        ):
            return data_batch
        new_data = []
        for entry in data_batch.copy():
            if entry == criteria:
                new_data.append(entry)
        return new_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Get stats about the current stream."""
        return {"amount": len(self.data)}


class SensorStream(DataStream):
    """DataStream for processing and filtering sensor data."""

    type: str = "Environmental Data"
    unit: str = "reading"
    data_type: str = "Sensor data"
    possible_fields: Set[str] = {
        "temp",
        "humidity",
        "pressure",
    }

    def __init__(self, id: str) -> None:
        """Initialize SensorStream with id."""
        super().__init__(id)
        self.data: Dict[str, List[float]] = {}

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process sensor data batch."""
        batch_str: str = str(data_batch).replace("'", "")

        if not isinstance(data_batch, list) or any(
            (not isinstance(i, str) for i in data_batch)
        ):
            return "[Error]: the given data batch is not a list of strings."

        data_batch = [s.split(":") for s in data_batch]
        data_batch = [i for i in data_batch if len(i) == 2]

        try:
            for key, val in data_batch:
                if key not in self.possible_fields:
                    continue
                val = float(val)
                if key in self.data:
                    self.data[key].append(val)
                else:
                    self.data[key] = [val]
        except ValueError:
            return "[Error]: the values should be numbers"

        return f"Processing sensor batch: {batch_str}"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Get stats about the processed data."""
        temps = self.data.get("temp", [0])
        return {
            "amount": sum((len(i) for i in self.data.values())),
            "avg": sum(temps) / len(temps),
        }

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter data using specific criteria."""
        if (
            criteria is None
            or not isinstance(data_batch, list)
            or any((not isinstance(i, str) for i in data_batch))
        ):
            return data_batch

        split: List[List[str]] = [i.split(":") for i in data_batch]
        c_field, c_threshold = criteria.split(":", 1)

        try:
            c_threshold = float(c_threshold)
            new_data: List[Any] = []
            for entry, i in zip(split, data_batch, strict=False):
                if len(entry) != 2:
                    continue
                key, val = entry
                if key not in self.possible_fields:
                    continue
                val = float(val)
                if key == c_field and val > c_threshold:
                    new_data.append(i)
            return new_data
        except ValueError:
            return data_batch


class TransactionStream(DataStream):
    """DataStream for processing and filtering transaction data."""

    type: str = "Financial Data"
    unit: str = "operation"
    data_type: str = "Transaction data"
    possible_fields: Set[str] = {
        "buy",
        "sell",
    }
    critical_limit: int = 100

    def __init__(self, id: str) -> None:
        """Initialize TransactionStream with id."""
        super().__init__(id)
        self.data: Dict[str, List[int]] = {}

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process transaction data batch."""
        batch_str: str = str(data_batch).replace("'", "")

        if not isinstance(data_batch, list) or any(
            (not isinstance(i, str) for i in data_batch)
        ):
            return "[Error]: the given data batch is not a list of strings."

        data_batch = [s.split(":") for s in data_batch]
        data_batch = [i for i in data_batch if len(i) == 2]

        try:
            for key, val in data_batch:
                if key not in self.possible_fields:
                    continue
                val = int(val)
                if key in self.data:
                    self.data[key].append(val)
                else:
                    self.data[key] = [val]
        except ValueError:
            return "[Error]: the values should be integers"

        return f"Processing transaction batch: {batch_str}"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Get stats about the processed data."""
        buys = self.data.get("buy", [0])
        sells = self.data.get("sell", [0])
        return {
            "amount": sum((len(i) for i in self.data.values())),
            "flow": sum(buys) - sum(sells),
        }

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter data using specific criteria."""
        if (
            criteria is None
            or not isinstance(data_batch, list)
            or any((not isinstance(i, str) for i in data_batch))
        ):
            return data_batch

        split: List[List[str]] = [i.split(":") for i in data_batch]
        c_field, c_threshold = criteria.split(":", 1)

        try:
            c_threshold = int(c_threshold)
            new_data: List[Any] = []
            for entry, i in zip(split, data_batch, strict=False):
                if len(entry) != 2:
                    continue
                key, val = entry
                if key not in self.possible_fields:
                    continue
                val = int(val)
                if key == c_field and val > c_threshold:
                    new_data.append(i)
            return new_data
        except ValueError:
            return data_batch


class EventStream(DataStream):
    """DataStream for processing and filtering event data."""

    type: str = "System Events"
    unit: str = "event"
    data_type: str = "Event data"
    possible_fields: Set[str] = {
        "login",
        "logout",
        "register",
        "error",
    }

    def __init__(self, id: str) -> None:
        """Initialize EventStream with id."""
        super().__init__(id)
        self.data: Dict[str, int] = {}

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process event data batch."""
        batch_str: str = str(data_batch).replace("'", "")

        if not isinstance(data_batch, list) or any(
            (not isinstance(i, str) for i in data_batch)
        ):
            return "[Error]: the given data batch is not a list of strings."

        data_batch = [i for i in data_batch if ":" not in i]
        for key in data_batch:
            if key not in self.possible_fields:
                continue
            if key in self.data:
                self.data[key] += 1
            else:
                self.data[key] = 1

        return f"Processing event batch: {batch_str}"

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Get stats about the processed data."""
        return {
            "amount": sum((i for i in self.data.values())),
            "errors": self.data.get("error", 0),
        }

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter data using specific criteria."""
        return super().filter_data(data_batch, criteria)


class StreamProcessor:
    """Handles multiple streams polymorphically."""

    def __init__(self) -> None:
        """Initialize StreamProcessor."""
        self.data_streams: List[Tuple[DataStream, Optional[str]]] = []
        self.batches_processed: int = 0

    def add_stream(self, stream: DataStream, filter_criteria: Optional[str]):
        """Add a new stream to use for processing and filtering."""
        if not isinstance(stream, DataStream) or not isinstance(
            filter_criteria, str
        ):
            print("[Error]: this is not a valid stream", file=sys.stderr)
            return
        self.data_streams.append((stream, filter_criteria))

    def process(self, data_batch: List[Any]):
        """Process the same batch of data through all the streams."""
        self.batches_processed += 1
        for stream, _ in self.data_streams:
            stream.process_batch(data_batch)
            stats = stream.get_stats()
            print(
                f"- {stream.data_type}: "
                f"{stats['amount']} {stream.unit}s processed"
            )

    def filter(self, data_batch: List[Any]):
        """Filter this data batch through all the stream."""
        critical_sensor: int = 0
        critical_transaction: int = 0
        critical_event: int = 0

        for stream, criteria in self.data_streams:
            filtered: List[Any] = stream.filter_data(data_batch, criteria)
            if isinstance(stream, SensorStream):
                critical_sensor += len(filtered)
            if isinstance(stream, TransactionStream):
                critical_transaction += len(filtered)
            if isinstance(stream, EventStream):
                critical_event += len(filtered)

        print("Filtered results: ", end="")
        reports: List[str] = []
        if critical_sensor > 0:
            reports.append(f"{critical_sensor} critical sensor alerts")
        if critical_transaction > 0:
            reports.append(f"{critical_transaction} large transaction")
        if critical_event > 0:
            reports.append(f"{critical_event} spoopy event D:")
        print(", ".join(reports))


def test_sensor_stream() -> None:
    """Test SensorStream."""
    sensor_stream: SensorStream = SensorStream("SENSOR_001")
    print("\nInitializing Sensor Stream...")
    print(f"Stream ID: {sensor_stream.id}, Type: {sensor_stream.type}")
    processed: str = sensor_stream.process_batch(
        ["temp:22.5", "humidity:65", "pressure:1013"]
    )
    print(processed)
    stats = sensor_stream.get_stats()
    print(
        f"Sensor analysis: {stats['amount']} readings processed, "
        f"avg temp: {stats['avg']}°C"
    )


def test_transaction_stream() -> None:
    """Test TransactionStream."""
    transaction_stream: TransactionStream = TransactionStream("TRANS_001")
    print("\nInitializing Transaction Stream...")
    print(
        f"Stream ID: {transaction_stream.id}, Type: {transaction_stream.type}"
    )
    processed: str = transaction_stream.process_batch(
        ["buy:100", "sell:150", "buy:75"],
    )
    print(processed)
    stats = transaction_stream.get_stats()
    print(
        f"Transaction analysis: {stats['amount']} operations"
        f", net flow: {stats['flow']:+} units"
    )


def test_event_stream() -> None:
    """Test EventStream."""
    event_stream = EventStream("EVENT_001")
    print("\nInitializing Event Stream...")
    print(f"Stream ID: {event_stream.id}, Type: {event_stream.type}")
    processed: str = event_stream.process_batch(
        ["login", "error", "logout"],
    )
    print(processed)
    stats = event_stream.get_stats()
    print(
        f"Event analysis: {stats['amount']}"
        f" events, {stats['errors']} error detected"
    )


def test_poly_stream() -> None:
    """Test a bunch of stream polymorphically."""
    stream_processor: StreamProcessor = StreamProcessor()

    stream_processor.add_stream(SensorStream("SENSOR_002"), "temp:40")
    stream_processor.add_stream(TransactionStream("TRANS_002"), "buy:100")
    stream_processor.add_stream(EventStream("EVENT_002"), "error")

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print("\nBatch 1 Results:")
    data_batch = [
        "temp:45.3",
        "temp:41.0",
        "buy:200",
        "sell:10",
        "sell:13",
        "buy:40",
        "login",
        "register",
        "logout",
    ]
    stream_processor.process(data_batch)
    print("\nStream filtering active: High-priority data only")
    stream_processor.filter(data_batch)


def main() -> None:
    """Program entry function."""
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    try:
        test_sensor_stream()
        test_transaction_stream()
        test_event_stream()
        test_poly_stream()
    except Exception as error:
        print(error, file=sys.stderr)

    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
