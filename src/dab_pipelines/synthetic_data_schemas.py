"""Schema definitions for synthetic data generation.

This module defines the schema classes and predefined schema generators
used to configure synthetic data generation.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Literal


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
    now_utc = datetime.now(tz=UTC)
    min_timestamp = (now_utc - timedelta(days=7)).isoformat()
    max_timestamp = now_utc.isoformat()

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
            FieldSchema(name="timestamp", field_type="datetime", min_value=min_timestamp, max_value=max_timestamp),
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
            FieldSchema(name="timestamp", field_type="datetime", min_value=min_timestamp, max_value=max_timestamp),
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
