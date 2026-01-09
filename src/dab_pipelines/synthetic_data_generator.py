"""Synthetic test data generator for ETL pipelines.

This module provides a configurable way to generate synthetic JSON test data
for PySpark ETL pipelines. Data is generated based on schema definitions that
specify field names, types, and generation strategies.
"""

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Literal

from faker import Faker


@dataclass
class FieldSchema:
    """Schema definition for a single field.

    Parameters
    ----------
    name : str
        Field name in the output JSON.
    field_type : str
        Data type: 'int', 'float', 'string', 'bool', 'datetime', 'uuid', 'choice', 'reference'.
    generator : Callable | None
        Custom function to generate values. If None, uses default for field_type.
    min_value : int | float | None
        Minimum value for numeric types or datetime.
    max_value : int | float | None
        Maximum value for numeric types or datetime.
    choices : list[Any] | None
        List of possible values for 'choice' type.
    reference_pool : list[Any] | None
        Pool of values to randomly select from for 'reference' type.
    reference_unique : bool
        If True, each value from reference_pool is used only once (for dimension tables).
    faker_method : str | None
        Name of Faker method to use (e.g., 'company', 'address', 'email').
    nullable : bool
        Whether field can be None.
    null_probability : float
        Probability of generating None (0.0-1.0).
    """

    name: str
    field_type: Literal["int", "float", "string", "bool", "datetime", "uuid", "choice", "reference", "faker"]
    generator: Callable[[], Any] | None = None
    min_value: int | float | None = None
    max_value: int | float | None = None
    choices: list[Any] | None = None
    reference_pool: list[Any] | None = None
    reference_unique: bool = False
    faker_method: str | None = None
    nullable: bool = False
    null_probability: float = 0.1


@dataclass
class DatasetSchema:
    """Schema definition for a dataset.

    Parameters
    ----------
    name : str
        Name of the dataset (used for filename).
    fields : list[FieldSchema]
        List of field schemas defining the structure.
    num_records : int
        Number of records to generate.
    """

    name: str
    fields: list[FieldSchema]
    num_records: int = 100


class SyntheticDataGenerator:
    """Generate synthetic test data based on schema definitions.

    Parameters
    ----------
    seed : int | None
        Random seed for reproducibility.
    """

    def __init__(self, seed: int | None = None) -> None:
        """Initialize the generator."""
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)

    def _generate_value(self, field_schema: FieldSchema) -> Any:
        """Generate a single value based on field schema.

        Parameters
        ----------
        field_schema : FieldSchema
            Schema definition for the field.

        Returns
        -------
        Any
            Generated value.
        """
        # Handle nullable fields
        if field_schema.nullable and random.random() < field_schema.null_probability:
            return None

        # Use custom generator if provided
        if field_schema.generator is not None:
            return field_schema.generator()

        # Generate based on field type
        if field_schema.field_type == "int":
            min_val = field_schema.min_value if field_schema.min_value is not None else 0
            max_val = field_schema.max_value if field_schema.max_value is not None else 1000
            return random.randint(int(min_val), int(max_val))

        elif field_schema.field_type == "float":
            min_val = field_schema.min_value if field_schema.min_value is not None else 0.0
            max_val = field_schema.max_value if field_schema.max_value is not None else 1000.0
            return round(random.uniform(float(min_val), float(max_val)), 2)

        elif field_schema.field_type == "string":
            return self.faker.word()

        elif field_schema.field_type == "bool":
            return random.choice([True, False])

        elif field_schema.field_type == "datetime":
            if field_schema.min_value is not None and field_schema.max_value is not None:
                start = datetime.fromisoformat(str(field_schema.min_value))
                end = datetime.fromisoformat(str(field_schema.max_value))
                delta = end - start
                random_delta = timedelta(seconds=random.randint(0, int(delta.total_seconds())))
                return (start + random_delta).isoformat()
            else:
                # Default: last 30 days
                days_ago = random.randint(0, 30)
                return (datetime.now() - timedelta(days=days_ago)).isoformat()

        elif field_schema.field_type == "uuid":
            return str(uuid.uuid4())

        elif field_schema.field_type == "choice":
            if field_schema.choices is None:
                raise ValueError(f"Field '{field_schema.name}' of type 'choice' requires 'choices' parameter")
            return random.choice(field_schema.choices)

        elif field_schema.field_type == "reference":
            if field_schema.reference_pool is None:
                raise ValueError(f"Field '{field_schema.name}' of type 'reference' requires 'reference_pool' parameter")
            return random.choice(field_schema.reference_pool)

        elif field_schema.field_type == "faker":
            if field_schema.faker_method is None:
                raise ValueError(f"Field '{field_schema.name}' of type 'faker' requires 'faker_method' parameter")
            method = getattr(self.faker, field_schema.faker_method)
            return method()

        else:
            raise ValueError(f"Unsupported field type: {field_schema.field_type}")

    def generate_dataset(self, schema: DatasetSchema) -> list[dict[str, Any]]:
        """Generate a dataset based on schema.

        Parameters
        ----------
        schema : DatasetSchema
            Schema definition for the dataset.

        Returns
        -------
        list[dict[str, Any]]
            List of generated records.
        """
        # Track remaining values for unique reference fields
        unique_references = {}
        for field in schema.fields:
            if field.field_type == "reference" and field.reference_unique:
                if field.reference_pool is None:
                    raise ValueError(f"Field '{field.name}' of type 'reference' requires 'reference_pool' parameter")
                # Create a copy and shuffle
                unique_references[field.name] = field.reference_pool.copy()
                random.shuffle(unique_references[field.name])

        records = []
        for i in range(schema.num_records):
            record = {}
            for field in schema.fields:
                # Handle unique references
                if field.field_type == "reference" and field.reference_unique:
                    if i >= len(unique_references[field.name]):
                        raise ValueError(
                            f"Not enough unique values in reference_pool for field '{field.name}'. "
                            f"Pool has {len(field.reference_pool)} values but {schema.num_records} records requested."
                        )
                    record[field.name] = unique_references[field.name][i]
                else:
                    record[field.name] = self._generate_value(field)
            records.append(record)
        return records

    def generate_and_save(
        self,
        schemas: list[DatasetSchema],
        output_dir: Path | str,
        indent: int = 2,
    ) -> dict[str, Path]:
        """Generate datasets and save to JSON files.

        Parameters
        ----------
        schemas : list[DatasetSchema]
            List of dataset schemas to generate.
        output_dir : Path | str
            Directory to save JSON files.
        indent : int
            JSON indentation for readability.

        Returns
        -------
        dict[str, Path]
            Mapping of dataset names to file paths.
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        file_paths = {}
        for schema in schemas:
            data = self.generate_dataset(schema)
            file_path = output_path / f"{schema.name}.json"

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=indent, ensure_ascii=False)

            file_paths[schema.name] = file_path
            print(f"Generated {len(data)} records -> {file_path}")

        return file_paths


def create_machine_example_schemas(num_machines: int = 10, num_sensor_readings: int = 1000) -> list[DatasetSchema]:
    """Create example schemas for machine data (dimensions and facts).

    Parameters
    ----------
    num_machines : int
        Number of machines to generate.
    num_sensor_readings : int
        Number of sensor readings to generate.

    Returns
    -------
    list[DatasetSchema]
        List of dataset schemas (machine_dim, sensor_facts).
    """
    # Generate machine IDs to be shared across datasets
    machine_ids = [f"MACH-{i:04d}" for i in range(1, num_machines + 1)]

    # Machine dimension schema
    machine_dim_schema = DatasetSchema(
        name="machine_dim",
        num_records=num_machines,
        fields=[
            FieldSchema(name="machine_id", field_type="reference", reference_pool=machine_ids, reference_unique=True),
            FieldSchema(name="machine_name", field_type="faker", faker_method="company"),
            FieldSchema(name="location", field_type="faker", faker_method="city"),
            FieldSchema(
                name="machine_type",
                field_type="choice",
                choices=["CNC Mill", "Lathe", "3D Printer", "Laser Cutter", "Welding Robot"],
            ),
            FieldSchema(name="manufacturer", field_type="faker", faker_method="company"),
            FieldSchema(
                name="installation_date", field_type="datetime", min_value="2020-01-01", max_value="2025-12-31"
            ),
            FieldSchema(
                name="status",
                field_type="choice",
                choices=["operational", "maintenance", "offline"],
            ),
            FieldSchema(name="max_temperature", field_type="float", min_value=50.0, max_value=200.0),
            FieldSchema(name="max_pressure", field_type="float", min_value=5.0, max_value=50.0),
        ],
    )

    # Sensor facts schema
    sensor_facts_schema = DatasetSchema(
        name="sensor_facts",
        num_records=num_sensor_readings,
        fields=[
            FieldSchema(name="reading_id", field_type="uuid"),
            FieldSchema(name="machine_id", field_type="reference", reference_pool=machine_ids),
            FieldSchema(name="timestamp", field_type="datetime", min_value="2025-01-01", max_value="2025-01-09"),
            FieldSchema(name="temperature", field_type="float", min_value=20.0, max_value=180.0),
            FieldSchema(name="pressure", field_type="float", min_value=1.0, max_value=45.0),
            FieldSchema(name="vibration", field_type="float", min_value=0.0, max_value=10.0),
            FieldSchema(name="power_consumption", field_type="float", min_value=0.5, max_value=50.0),
            FieldSchema(
                name="error_code",
                field_type="choice",
                choices=[None, "E001", "E002", "E003", "W001", "W002"],
                nullable=True,
                null_probability=0.8,
            ),
            FieldSchema(name="is_anomaly", field_type="bool"),
        ],
    )

    return [machine_dim_schema, sensor_facts_schema]


def main() -> None:
    """Example usage of the synthetic data generator."""
    # Create generator with seed for reproducibility
    generator = SyntheticDataGenerator(seed=42)

    # Generate machine example data
    schemas = create_machine_example_schemas(num_machines=10, num_sensor_readings=1000)

    # Save to fixtures directory
    output_dir = Path(__file__).parent.parent.parent / "data_output" / "synthetic_data"
    generator.generate_and_save(schemas, output_dir)

    print(f"\nSynthetic data generated successfully in: {output_dir}")


if __name__ == "__main__":
    main()
