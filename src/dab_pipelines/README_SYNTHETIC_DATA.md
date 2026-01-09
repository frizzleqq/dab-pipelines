# Synthetic Data Generator

A flexible, schema-based synthetic test data generator for ETL pipelines.

## Features

- **Schema-based configuration**: Define data structures with field types, constraints, and relationships
- **Multiple data types**: Support for int, float, string, bool, datetime, UUID, choices, references, and Faker integration
- **Cross-dataset relationships**: Share IDs and references across multiple datasets
- **Reproducible**: Use seeds for consistent test data generation
- **JSON output**: Generate data in JSON format for easy consumption by PySpark

## Quick Start

### Basic Usage

```python
from pathlib import Path
from dab_pipelines.synthetic_data_generator import (
    SyntheticDataGenerator,
    DatasetSchema,
    FieldSchema,
)

# Create generator with seed for reproducibility
generator = SyntheticDataGenerator(seed=42)

# Define schema
schema = DatasetSchema(
    name="users",
    num_records=100,
    fields=[
        FieldSchema(name="user_id", field_type="uuid"),
        FieldSchema(name="age", field_type="int", min_value=18, max_value=80),
        FieldSchema(name="email", field_type="faker", faker_method="email"),
        FieldSchema(
            name="status",
            field_type="choice",
            choices=["active", "inactive", "suspended"]
        ),
    ],
)

# Generate and save
generator.generate_and_save([schema], "fixtures/data")
```

### Machine Data Example

Generate machine dimension and sensor facts data:

```python
from dab_pipelines.synthetic_data_generator import (
    SyntheticDataGenerator,
    create_machine_example_schemas,
)

generator = SyntheticDataGenerator(seed=42)
schemas = create_machine_example_schemas(
    num_machines=10,
    num_sensor_readings=1000
)

generator.generate_and_save(schemas, "fixtures/synthetic_data")
```

This generates two files:
- `machine_dim.json`: Machine metadata (dimension table)
- `sensor_facts.json`: Sensor readings (fact table)

### Command Line

Run the built-in example:

```bash
python -m dab_pipelines.synthetic_data_generator
```

## Field Types

### Numeric Types

```python
# Integer with range
FieldSchema(name="age", field_type="int", min_value=0, max_value=100)

# Float with range
FieldSchema(name="price", field_type="float", min_value=10.0, max_value=1000.0)
```

### String Types

```python
# Random word
FieldSchema(name="name", field_type="string")

# UUID
FieldSchema(name="id", field_type="uuid")

# Faker method
FieldSchema(name="email", field_type="faker", faker_method="email")
FieldSchema(name="company", field_type="faker", faker_method="company")
FieldSchema(name="address", field_type="faker", faker_method="address")
```

### DateTime

```python
FieldSchema(
    name="created_at",
    field_type="datetime",
    min_value="2025-01-01T00:00:00",
    max_value="2025-12-31T23:59:59"
)
```

### Choice and Reference

```python
# Fixed choices
FieldSchema(
    name="status",
    field_type="choice",
    choices=["active", "inactive", "pending"]
)

# Reference to shared pool (for relationships)
machine_ids = [f"MACH-{i:04d}" for i in range(1, 11)]
FieldSchema(name="machine_id", field_type="reference", reference_pool=machine_ids)
```

### Nullable Fields

```python
FieldSchema(
    name="optional_field",
    field_type="string",
    nullable=True,
    null_probability=0.2  # 20% chance of None
)
```

### Custom Generator

```python
import random

FieldSchema(
    name="custom_value",
    field_type="string",
    generator=lambda: f"CUSTOM-{random.randint(1000, 9999)}"
)
```

## Creating Related Datasets

Generate multiple datasets with shared IDs:

```python
# Step 1: Generate shared ID pool
customer_ids = [f"CUST-{i:05d}" for i in range(1, 101)]

# Step 2: Customer dimension
customer_schema = DatasetSchema(
    name="customers",
    num_records=100,
    fields=[
        FieldSchema(name="customer_id", field_type="reference", reference_pool=customer_ids),
        FieldSchema(name="name", field_type="faker", faker_method="name"),
        FieldSchema(name="country", field_type="faker", faker_method="country"),
    ],
)

# Step 3: Orders fact table (references customers)
orders_schema = DatasetSchema(
    name="orders",
    num_records=500,
    fields=[
        FieldSchema(name="order_id", field_type="uuid"),
        FieldSchema(name="customer_id", field_type="reference", reference_pool=customer_ids),
        FieldSchema(name="amount", field_type="float", min_value=10.0, max_value=5000.0),
        FieldSchema(name="order_date", field_type="datetime"),
    ],
)

# Generate both datasets
generator = SyntheticDataGenerator(seed=42)
generator.generate_and_save([customer_schema, orders_schema], "fixtures/data")
```

## Available Faker Methods

Common Faker methods you can use with `faker_method`:

- `name`, `first_name`, `last_name`
- `email`, `phone_number`
- `company`, `job`
- `address`, `city`, `country`, `postcode`
- `text`, `sentence`, `paragraph`
- `url`, `ipv4`, `user_name`
- `date`, `date_time`
- `credit_card_number`, `currency_code`

See [Faker documentation](https://faker.readthedocs.io/) for complete list.

## Integration with PySpark

Load generated JSON data in PySpark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read JSON data
machines_df = spark.read.json("fixtures/synthetic_data/machine_dim.json")
sensors_df = spark.read.json("fixtures/synthetic_data/sensor_facts.json")

# Join dimension and facts
result = sensors_df.join(machines_df, on="machine_id", how="left")
result.show()
```

## Testing

Run tests for the generator:

```bash
uv run pytest tests/test_synthetic_data_generator.py -v
```

## Examples

See [create_machine_example_schemas](../src/dab_pipelines/synthetic_data_generator.py) function for a complete example of generating related datasets with dimensions and facts.
