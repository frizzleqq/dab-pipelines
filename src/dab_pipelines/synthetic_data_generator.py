"""Synthetic test data generator for ETL pipelines.

This module provides a configurable way to generate synthetic JSON test data
for PySpark ETL pipelines. Data is generated based on schema definitions that
specify field names, types, and generation strategies.
"""

import json
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from faker import Faker

from dab_pipelines.logging_config import get_logger
from dab_pipelines.synthetic_data_schemas import DatasetSchema, FieldSchema, create_machine_example_schemas

logger = get_logger(__name__)


class SyntheticDataGenerator:
    """Generate synthetic test data based on schema definitions.

    Parameters
    ----------
    seed : int | None
        Random seed for reproducibility.
    timestamp : datetime | None
        Optional datetime to append to filenames as suffix.
    """

    def __init__(self, seed: int | None = None, timestamp: datetime | None = None) -> None:
        """Initialize the generator."""
        self.seed = seed
        self.timestamp = timestamp
        if seed is not None:
            random.seed(seed)
            logger.debug(f"Random seed set to {seed}")
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)
            logger.debug(f"Faker seed set to {seed}")

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
        logger.debug(f"Generating dataset '{schema.name}' with {schema.num_records} records")
        
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
        
        logger.debug(f"Successfully generated {len(records)} records for '{schema.name}'")
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
        logger.info(f"Saving datasets to {output_path}")

        file_paths = {}
        for schema in schemas:
            data = self.generate_dataset(schema)
            # Add timestamp suffix if provided
            if self.timestamp:
                timestamp_str = self.timestamp.strftime("%Y%m%d_%H%M%S")
                filename = f"{schema.name}_{timestamp_str}.json"
            else:
                filename = f"{schema.name}.json"
            file_path = output_path / schema.name / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=indent, ensure_ascii=False)

            file_paths[schema.name] = file_path
            logger.info(f"Generated {len(data)} records -> {file_path}")

        return file_paths


def main() -> None:
    """Example usage of the synthetic data generator."""
    from dab_pipelines.logging_config import setup_logging
    
    # Initialize logging for standalone execution
    setup_logging(verbose=False)

    # Create generator with seed for reproducibility
    generator = SyntheticDataGenerator(seed=42, timestamp=datetime.now(tz=UTC))

    # Generate machine example data
    schemas = create_machine_example_schemas(num_machines=10, num_sensor_readings=1000)

    # Save to fixtures directory
    output_dir = Path(__file__).parent.parent.parent / "data_output" / "synthetic_data"
    generator.generate_and_save(schemas, output_dir)

    logger.info(f"Synthetic data generated successfully in: {output_dir}")


if __name__ == "__main__":
    main()
