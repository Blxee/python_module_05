from abc import ABC, abstractmethod
from typing import Any, List, Dict
import sys


class DataProcessor(ABC):
    """Abstract parent class for data processors."""

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the data into str."""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate whether this data is of the required type."""
        pass

    def format_output(self, result: str) -> str:
        """Format the processed data."""
        return "Processed"


class NumericProcessor(DataProcessor):
    """Processor for numeric data."""

    def process(self, data: Any) -> str:
        """Process the data into str."""
        if type(data) is not list:
            print("[Error]: expected a list", file=sys.stderr)
            return ""
        for i in data:
            if type(i) is not int:
                print("[Error]: expected list of ints", file=sys.stderr)
                return ""
        return " ".join(map(str, data))

    def validate(self, data: Any) -> bool:
        """Validate whether this data is of the required type."""
        if type(data) is not list:
            return False
        for i in data:
            if type(i) is not int:
                return False
        return True

    def format_output(self, result: str) -> str:
        """Format the processed data."""
        if not isinstance(result, str):
            print("[Error]: result should be a string", file=sys.stderr)
            return ""
        data: List[int] = [int(num) for num in result.split()]
        summary: int = sum(data)
        avg = summary / len(data)
        return (
            super().format_output(result) + f" {len(data)} numeric values"
            f", sum={summary}, avg={avg:.1f}"
        )


class TextProcessor(DataProcessor):
    """Processor for text data."""

    def process(self, data: Any) -> str:
        """Process the data into str."""
        if type(data) is not str:
            print("[Error]: expected text data", file=sys.stderr)
            return ""
        else:
            return data

    def validate(self, data: Any) -> bool:
        """Validate whether this data is of the required type."""
        return type(data) is str

    def format_output(self, result: str) -> str:
        """Return a formatted string."""
        if not isinstance(result, str):
            print("[Error]: result should be a string", file=sys.stderr)
            return ""
        return (
            super().format_output(result) + f" text: {len(result)} characters"
            f", {len(result.split())} words"
        )


class LogProcessor(DataProcessor):
    """Processor for log data."""

    level: tuple[str, str, str, str] = ("ERROR:", "WARN:", "INFO:", "DEBUG:")

    def process(self, data: Any) -> str:
        """Process the data into str."""
        if type(data) is not str or not data.startswith(self.level):
            print("[Error]: expected log data", file=sys.stderr)
            return ""
        else:
            return data

    def validate(self, data: Any) -> bool:
        """Validate whether this data is of the required type."""
        return type(data) is str and data.startswith(self.level)

    def format_output(self, result: str) -> str:
        """Format the processed data."""
        label: Dict[str, str] = {
            "ERROR": "[ALERT]",
            "WARN": "[ALERT]",
            "DEBUG": "[INFO]",
            "INFO": "[INFO]",
        }
        split = result.split(": ")
        if len(split) != 2:
            print("[Error]: wrong format", file=sys.stderr)
            return ""
        (level, message) = split
        return f"{label[level]} {level} level detected: {message}"


def test_numeric_processor() -> None:
    """Test numeric processor."""
    print("\nInitializing Numeric Processor...")
    numeric_processor: NumericProcessor = NumericProcessor()

    numeric_data: List[int] = [1, 2, 3, 4, 5]
    print("Processing data:", numeric_data)
    result: str = numeric_processor.process(numeric_data)

    print("Validation: ", end="")
    validation: bool = numeric_processor.validate(numeric_data)
    if validation:
        print("Numeric data verified")
    else:
        print("Numeric data was not verified")

    print("Output:", numeric_processor.format_output(result))


def test_text_processor() -> None:
    """Test text processor."""
    print("\nInitializing Text Processor...")
    text_processor: TextProcessor = TextProcessor()

    text_data: str = "Hello Nexus World"
    print(f'Processing data: "{text_data}"')
    result: str = text_processor.process(text_data)

    print("Validation: ", end="")
    validation: bool = text_processor.validate(text_data)
    if validation:
        print("Text data verified")
    else:
        print("Text data was not verified")

    print("Output:", text_processor.format_output(result))


def test_log_processor() -> None:
    """Test log processor."""
    print("\nInitializing Log Processor...")
    log_processor: LogProcessor = LogProcessor()

    log_data: str = "ERROR: Connection timeout"
    print(f'Processing data: "{log_data}"')
    result: str = log_processor.process(log_data)

    print("Validation: ", end="")
    validation: bool = log_processor.validate(log_data)
    if validation:
        print("Log entry verified")
    else:
        print("Log entry was not verified")

    print("Output:", log_processor.format_output(result))


def test_polymorphic_processing() -> None:
    """Test polymorphic proccessing with common interface."""
    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    data_processor_pairs: List[tuple[DataProcessor, Any]] = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello World!"),
        (LogProcessor(), "INFO: System ready"),
    ]

    def get_processing_result(processor: DataProcessor, data: Any) -> str:
        """Use the processor given on the data and return the output."""
        valid: bool = processor.validate(data)
        if not valid:
            print("Error: Invalid data for the processor")
            return ""
        processed: str = processor.process(data)
        result = processor.format_output(processed)
        return result

    i = 1
    for processor, data in data_processor_pairs:
        result = get_processing_result(processor, data)
        print(f"Result {i}: {result}")
        i += 1


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    try:
        test_numeric_processor()
        test_text_processor()
        test_log_processor()
        test_polymorphic_processing()
    except Exception as error:
        print(error, file=sys.stderr)

    print("\nFoundation systems online. Nexus ready for advanced streams.")
