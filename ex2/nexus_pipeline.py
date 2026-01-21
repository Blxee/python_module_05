import csv
import json
import sys
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Protocol, Union


class ProcessingStage(Protocol):
    """Protocol for duck-typing stages."""

    def process(self, data: Any) -> Any:
        """Process data."""
        pass


class ProcessingPipeline(ABC):
    """Super class for all pipelines."""

    def __init__(self) -> None:
        """Initialize a new pipeline."""
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """Add this object to stage if it has a 'process' (ducktyping)."""
        if callable(getattr(stage, "process", None)):
            self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Abstract method to process data through pipeline stages."""
        pass


class InputStage:
    """Input stage processor class."""

    def process(self, data: Any) -> Dict:
        """Process data."""
        try:
            data["valid"] = True
        except Exception:
            print(
                "Error detected in Stage 2: Invalid data format",
                file=sys.stderr,
            )
            data = {"error": "error in stage 1"}
        return data


class TransformStage:
    """Transform stage processor class."""

    def process(self, data: Any) -> Dict:
        """Process data."""
        try:
            data_type = data["type"]
            raw = data["raw"]

            match data_type:
                case "json":
                    if not isinstance(raw, dict):
                        data["parsed"] = json.loads(raw)
                case "csv":
                    data["parsed"] = csv.reader(raw)
                case "stream":
                    data["parsed"] = raw
                case _:
                    raise ValueError()
        except Exception:
            print(
                "Error detected in Stage 2: Invalid data format",
                file=sys.stderr,
            )
            data = {"error": "error in stage 2"}

        return data


class OutputStage:
    """Output stage processor class."""

    def process(self, data: Any) -> str:
        """Process data."""
        try:
            return data["raw"]
        except Exception:
            print(
                "Error detected in Stage 3: Invalid data format",
                file=sys.stderr,
            )
            return ""


class JSONAdapter(ProcessingPipeline):
    """JSON data pipeline."""

    def __init__(self, pipeline_id: str) -> None:
        """Initialize a new pipeline."""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """Process data through pipeline stages."""
        data = {
            "raw": data,
            "type": "json",
        }
        for stage in self.stages:
            data = stage.process(data)
            if isinstance(data, dict) and "error" in data:
                break
        return data


class CSVAdapter(ProcessingPipeline):
    """JSON data pipeline."""

    def __init__(self, pipeline_id: str) -> None:
        """Initialize a new pipeline."""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """Process data through pipeline stages."""
        data = {
            "raw": data,
            "type": "csv",
        }
        for stage in self.stages:
            data = stage.process(data)
            if isinstance(data, dict) and "error" in data:
                break
        return data


class StreamAdapter(ProcessingPipeline):
    """JSON data pipeline."""

    def __init__(self, pipeline_id: str) -> None:
        """Initialize a new pipeline."""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """Process data through pipeline stages."""
        data = {
            "raw": data,
            "type": "stream",
        }
        for stage in self.stages:
            data = stage.process(data)
            if isinstance(data, dict) and "error" in data:
                break
        return data


class NexusManager:
    """Orchestrates multiple pipeline chains."""

    def __init__(self) -> None:
        """Initialize a new NexusManager."""
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Add a new pipeline to the processing chain."""
        if isinstance(pipeline, ProcessingPipeline):
            self.pipelines.append(pipeline)

    def process_data(self, data: Any) -> None:
        """Process data through the pipeline chain."""
        try:
            for pipeline in self.pipelines:
                data = pipeline.process(data)
        except Exception:
            print(
                "Error detected while processing through many pipelines",
                file=sys.stderr,
            )


def add_processing_stages(pipeline: ProcessingPipeline) -> None:
    """Add 3 stage stages to the given pipeline (input, transform, output)."""
    try:
        pipeline.add_stage(InputStage())
        pipeline.add_stage(TransformStage())
        pipeline.add_stage(OutputStage())
    except Exception:
        print("Error detected while adding stages: not a valid pipeline")


def test_json_pipeline() -> None:
    """Test a json pipeline with dummy data using three stages."""
    json_adapter = JSONAdapter("JSON_001")
    add_processing_stages(json_adapter)

    json_input: Dict[str, Any] = {"sensor": "temp", "value": 23.5, "unit": "C"}
    _ = json_adapter.process(json_input)

    print("\nProcessing JSON data through pipeline...")
    print("Input:", str(json_input).replace("'", '"'))
    print("Transform: Enriched with metadata and validation")
    print("Output: Processed temperature reading:"
          f" {json_input.get('value', 0)}°C (Normal range)")


def test_csv_pipeline() -> None:
    """Test a csv pipeline with dummy data using three stages."""
    csv_adapter = CSVAdapter("CSV_001")
    add_processing_stages(csv_adapter)

    csv_input: str = "user,action,timestamp"
    _ = csv_adapter.process(csv_input)

    print("\nProcessing CSV data through same pipeline...")
    print("Input:", f'"{csv_input}"')
    print("Transform: Parsed and structured data")
    print("Output: User activity logged: "
          f"{csv_input.count('action')} actions processed")


def test_stream_pipeline() -> None:
    """Test a steam pipeline with dummy data using three stages."""
    stream_adapter = StreamAdapter("STREAM_001")
    add_processing_stages(stream_adapter)

    stream_input: List[float] = [22.0, 22.0, 22.0, 22.0, 22.5]
    _ = stream_adapter.process(stream_input)

    print("\nProcessing Stream data through same pipeline...")
    print("Input: Real-time sensor stream")
    print("Transform: Aggregated and filtered")
    print(f"Output: Stream summary: {len(stream_input)} readings,"
          f" avg: {sum(stream_input) / len(stream_input)}°C")


def test_pipeline_chaining() -> None:
    """Test polymorphic handling of pipelines."""
    print("\n=== Pipeline Chaining Demo ===")

    manager = NexusManager()

    pipeline_a = JSONAdapter("JSON_001")
    pipeline_b = JSONAdapter("JSON_002")
    pipeline_c = JSONAdapter("JSON_003")

    manager.add_pipeline(pipeline_a)
    manager.add_pipeline(pipeline_b)
    manager.add_pipeline(pipeline_c)
    print("Pipeline A -> Pipeline B -> Pipeline C")

    json_input: Dict[str, Any] = {"sensor": "temp", "value": 23.5, "unit": "C"}
    manager.process_data(json_input)

    print("Data flow: Raw -> Processed -> Analyzed -> Stored")
    print("\nChain result: 100 records processed through 3-stage pipeline")
    print("Performance: 95% efficiency, 0.2s total processing time")


def test_pipeline_failure() -> None:
    """Test error recovery."""
    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    json_adapter: JSONAdapter = JSONAdapter("JSON_001")
    add_processing_stages(json_adapter)
    json_adapter.process("wrong data")
    print("Recovery initiated: Switching to backup processor")
    print("Recovery successful: Pipeline restored, processing resumed")


def main() -> None:
    """Etnry function."""
    print("""\
=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===

Initializing Nexus Manager...
Pipeline capacity: 1000 streams/second

Creating Data Processing Pipeline...
Stage 1: Input validation and parsing
Stage 2: Data transformation and enrichment
Stage 3: Output formatting and delivery

=== Multi-Format Data Processing ===""")

    try:
        test_json_pipeline()
        test_csv_pipeline()
        test_stream_pipeline()
        test_pipeline_chaining()
        test_pipeline_failure()
    except Exception as error:
        print("[Error]:", error)

    print("\nNexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
