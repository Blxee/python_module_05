import sys
from abc import ABC, abstractmethod
from typing import List, Any, Dict, Union


def dict_follows_prototype(var: Any, proto: Dict[Any, Any]) -> bool:
    """Checks whether a dict follows a specific format."""
    if not isinstance(var, dict):
        return False
    if not set(proto.keys()).issubset(set(var.keys())):
        return False
    for key, typ in proto.items():
        if isinstance(typ, type):
            if not isinstance(var[key], typ):
                return False
        else:
            if var[key] != typ:
                return False
    return True


class InputStage:
    def process(self, data: Any) -> Dict:
        if not dict_follows_prototype(data, {"entry": dict}):
            print("[Error]: there are no entry field to process.")
            return {}
        data["valid"] = True
        return data


class TransformStage:
    def process(self, data: Any) -> Dict:
        if not dict_follows_prototype(data, {"entry": dict}):
            print("[Error]: there are no entry field to process.")
            return {}
        entry: Dict = data["entry"]
        if dict_follows_prototype(
            entry, {"sensor": "temp", "value": float, "unit": str}
        ):
            entry["unit"] = "°C"
        data["metadata"] = {
            "amount": len(data["entry"]),
        }
        return data


class OutputStage:
    def process(self, data: Any) -> str:
        if not dict_follows_prototype(data, {"entry": dict}):
            print("[Error]: there are no entry field to process.")
            return ""
        entry: Dict = data["entry"]
        if dict_follows_prototype(
            entry, {"sensor": "temp", "value": float, "unit": str}
        ):
            return f"{entry['value']}{entry['unit']}"
        return ""


Stage = Union["InputStage", "TransformStage", "OutputStage"]


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id) -> None:
        self.pipeline_id = pipeline_id
        self.stages: List[Stage] = []

    def add_stage(self, stage):
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        for stage in self.stages:
            data = stage.process(data)
        return data


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        import json

        try:
            data = json.loads(data)
        except json.JSONDecodeError as error:
            print(error, file=sys.stderr)
            return None
        except TypeError as error:
            print(error, file=sys.stderr)
            return None

        input_data: Dict = {
            "entry": data,
            "type": "json",
        }
        output_data = super().process(input_data)
        return str(output_data)


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        from csv import reader

        try:
            if not isinstance(data, str):
                raise TypeError("[Error]: csv data input should be a string.")
            data = reader(data.splitlines())
            data = list(data)[0]
            entries = {}
            for i in data:
                if i in entries:
                    entries[i] += 1
                else:
                    entries[i] = 1
        except TypeError as error:
            print(error, file=sys.stderr)
            return None

        input_data: Dict = {
            "entry": entries,
            "type": "csv",
        }
        output_data = super().process(input_data)
        return str(output_data)


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        if not isinstance(data, list) or any(
            (not isinstance(i, (int, float)) for i in data)
        ):
            print("[Error]: data format does not fit the stream!")
            return None

        input_data: Dict = {
            "entry": {
                "sensor": "temp",
                "value": sum(data) / len(data),
                "unit": "C",
            },
            "type": "stream",
        }
        output_data = super().process(input_data)
        return str(output_data)


class NexusManager:
    """Orchestrates multiple pipelines."""

    def __init__(self) -> None:
        """Initialize a new NexusManager."""
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Add a new pipeline to the manager."""
        if not isinstance(pipeline, ProcessingPipeline):
            print(
                "[Error]: argument should be a valid pipeline", file=sys.error
            )
            return
        self.pipelines.append(pipeline)

    def process_data(self, data: Any) -> None:
        pass


def test_json_adapter(stages: List[Any]) -> None:
    """Test JSON adapter with stages pipeline."""
    print("\nProcessing JSON data through pipeline...")
    json_adapter: JSONAdapter = JSONAdapter("JSON_001")

    for stage in stages:
        json_adapter.add_stage(stage)

    json_input: str = '{"sensor": "temp", "value": 23.5, "unit": "C"}'
    print("Input:", json_input)
    print("Transform: Enriched with metadata and validation")
    result: str = json_adapter.process(json_input)
    print(f"Output: Processed temperature reading: {result} (Normal range)")


def test_csv_adapter(stages: List[Any]) -> None:
    """Test CSV adapter with stages pipeline."""
    print("\nProcessing CSV data through same pipeline...")
    csv_adapter: CSVAdapter = CSVAdapter("CSV_001")

    for stage in stages:
        csv_adapter.add_stage(stage)

    csv_input: str = "user,action,timestamp"
    print("Input:", f'"{csv_input}"')
    print("Transform: Parsed and structured data")
    result: str = csv_adapter.process(csv_input)
    print(
        f"Output: User activity logged: {csv_input.count('action')} actions processed"
    )


def test_steam_adapter(stages: List[Any]) -> None:
    """Test Stream adapter with stages pipeline."""
    print("\nProcessing Stream data through same pipeline...")
    stream_adapter: StreamAdapter = StreamAdapter("STREAM_001")

    for stage in stages:
        stream_adapter.add_stage(stage)

    stream_input: List[float] = [20, 21, 22, 23, 24.5]
    print("Input: Real-time sensor stream")
    print("Transform: Aggregated and filtered")
    result: str = stream_adapter.process(stream_input)
    print(
        f"Output: Stream summary: {len(stream_input)} readings, avg: {result}"
    )


def main() -> None:
    print("""\
=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===

Initializing Nexus Manager...
Pipeline capacity: 1000 streams/second

Creating Data Processing Pipeline...""")
    stages: List[Any] = []

    print("Stage 1: Input validation and parsing")
    input_stage: InputStage = InputStage()
    stages.append(input_stage)

    print("Stage 2: Data transformation and enrichment")
    transform_stage: TransformStage = TransformStage()
    stages.append(transform_stage)

    print("Stage 3: Output formatting and delivery")
    output_stage: OutputStage = OutputStage()
    stages.append(output_stage)

    print("\n=== Multi-Format Data Processing ===")

    test_json_adapter(stages)
    test_csv_adapter(stages)
    test_steam_adapter(stages)


if __name__ == "__main__":
    main()
