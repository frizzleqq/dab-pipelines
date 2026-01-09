"""Tests for synthetic data generator."""

import json
import tempfile

from dab_pipelines.synthetic_data_generator import (
    DatasetSchema,
    FieldSchema,
    SyntheticDataGenerator,
    create_machine_example_schemas,
)


class TestFieldSchema:
    """Tests for FieldSchema."""

    def test_field_schema_creation(self):
        """Test creating a field schema."""
        field = FieldSchema(name="test_field", field_type="int", min_value=0, max_value=100)
        assert field.name == "test_field"
        assert field.field_type == "int"
        assert field.min_value == 0
        assert field.max_value == 100


class TestSyntheticDataGenerator:
    """Tests for SyntheticDataGenerator."""

    def test_generator_initialization(self):
        """Test generator initialization with seed."""
        generator = SyntheticDataGenerator(seed=42)
        assert generator.seed == 42

    def test_generate_int_field(self):
        """Test generating integer values."""
        generator = SyntheticDataGenerator(seed=42)
        field = FieldSchema(name="age", field_type="int", min_value=18, max_value=65)

        values = [generator._generate_value(field) for _ in range(100)]

        assert all(isinstance(v, int) for v in values)
        assert all(18 <= v <= 65 for v in values)

    def test_generate_float_field(self):
        """Test generating float values."""
        generator = SyntheticDataGenerator(seed=42)
        field = FieldSchema(name="price", field_type="float", min_value=10.0, max_value=100.0)

        values = [generator._generate_value(field) for _ in range(100)]

        assert all(isinstance(v, float) for v in values)
        assert all(10.0 <= v <= 100.0 for v in values)

    def test_generate_string_field(self):
        """Test generating string values."""
        generator = SyntheticDataGenerator(seed=42)
        field = FieldSchema(name="name", field_type="string")

        values = [generator._generate_value(field) for _ in range(10)]

        assert all(isinstance(v, str) for v in values)
        assert all(len(v) > 0 for v in values)

    def test_generate_bool_field(self):
        """Test generating boolean values."""
        generator = SyntheticDataGenerator(seed=42)
        field = FieldSchema(name="is_active", field_type="bool")

        values = [generator._generate_value(field) for _ in range(100)]

        assert all(isinstance(v, bool) for v in values)
        assert True in values
        assert False in values

    def test_generate_uuid_field(self):
        """Test generating UUID values."""
        generator = SyntheticDataGenerator(seed=42)
        field = FieldSchema(name="id", field_type="uuid")

        values = [generator._generate_value(field) for _ in range(10)]

        assert all(isinstance(v, str) for v in values)
        assert len(set(values)) == 10  # All unique

    def test_generate_datetime_field(self):
        """Test generating datetime values."""
        generator = SyntheticDataGenerator(seed=42)
        field = FieldSchema(
            name="created_at",
            field_type="datetime",
            min_value="2025-01-01T00:00:00",
            max_value="2025-01-31T23:59:59",
        )

        values = [generator._generate_value(field) for _ in range(10)]

        assert all(isinstance(v, str) for v in values)
        assert all("2025-01" in v for v in values)

    def test_generate_choice_field(self):
        """Test generating choice values."""
        generator = SyntheticDataGenerator(seed=42)
        choices = ["red", "green", "blue"]
        field = FieldSchema(name="color", field_type="choice", choices=choices)

        values = [generator._generate_value(field) for _ in range(100)]

        assert all(v in choices for v in values)
        assert len(set(values)) > 1  # Should have variety

    def test_generate_reference_field(self):
        """Test generating reference values."""
        generator = SyntheticDataGenerator(seed=42)
        reference_pool = ["ID-001", "ID-002", "ID-003"]
        field = FieldSchema(name="ref_id", field_type="reference", reference_pool=reference_pool)

        values = [generator._generate_value(field) for _ in range(50)]

        assert all(v in reference_pool for v in values)

    def test_generate_faker_field(self):
        """Test generating values using Faker."""
        generator = SyntheticDataGenerator(seed=42)
        field = FieldSchema(name="email", field_type="faker", faker_method="email")

        values = [generator._generate_value(field) for _ in range(10)]

        assert all(isinstance(v, str) for v in values)
        assert all("@" in v for v in values)

    def test_nullable_field(self):
        """Test nullable field generation."""
        generator = SyntheticDataGenerator(seed=42)
        field = FieldSchema(name="optional", field_type="int", nullable=True, null_probability=0.5)

        values = [generator._generate_value(field) for _ in range(100)]

        assert None in values
        assert any(v is not None for v in values)

    def test_custom_generator(self):
        """Test using custom generator function."""
        generator = SyntheticDataGenerator(seed=42)
        field = FieldSchema(name="custom", field_type="int", generator=lambda: 999)

        value = generator._generate_value(field)

        assert value == 999

    def test_generate_dataset(self):
        """Test generating a complete dataset."""
        generator = SyntheticDataGenerator(seed=42)
        schema = DatasetSchema(
            name="test_data",
            num_records=10,
            fields=[
                FieldSchema(name="id", field_type="int", min_value=1, max_value=100),
                FieldSchema(name="name", field_type="string"),
                FieldSchema(name="is_active", field_type="bool"),
            ],
        )

        dataset = generator.generate_dataset(schema)

        assert len(dataset) == 10
        assert all(isinstance(record, dict) for record in dataset)
        assert all("id" in record and "name" in record and "is_active" in record for record in dataset)

    def test_generate_and_save(self):
        """Test generating and saving datasets to files."""
        generator = SyntheticDataGenerator(seed=42)
        schemas = [
            DatasetSchema(
                name="test_dataset",
                num_records=5,
                fields=[
                    FieldSchema(name="id", field_type="int"),
                    FieldSchema(name="value", field_type="float"),
                ],
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            file_paths = generator.generate_and_save(schemas, tmpdir)

            assert "test_dataset" in file_paths
            assert file_paths["test_dataset"].exists()

            # Verify file contents
            with open(file_paths["test_dataset"]) as f:
                data = json.load(f)

            assert len(data) == 5
            assert all("id" in record and "value" in record for record in data)


class TestMachineExampleSchemas:
    """Tests for machine example schemas."""

    def test_create_machine_example_schemas(self):
        """Test creating machine example schemas."""
        schemas = create_machine_example_schemas(num_machines=5, num_sensor_readings=50)

        assert len(schemas) == 2
        assert schemas[0].name == "machine_dim"
        assert schemas[1].name == "sensor_facts"
        assert schemas[0].num_records == 5
        assert schemas[1].num_records == 50

    def test_machine_schemas_have_shared_ids(self):
        """Test that machine schemas share machine IDs."""
        generator = SyntheticDataGenerator(seed=42)
        schemas = create_machine_example_schemas(num_machines=3, num_sensor_readings=20)

        # Get the reference pool from the schema
        machine_id_field = next(f for f in schemas[0].fields if f.name == "machine_id")
        expected_machine_ids = set(machine_id_field.reference_pool)

        machine_dim_data = generator.generate_dataset(schemas[0])
        sensor_facts_data = generator.generate_dataset(schemas[1])

        machine_ids_dim = {record["machine_id"] for record in machine_dim_data}
        machine_ids_facts = {record["machine_id"] for record in sensor_facts_data}

        # Dimension table should have all machine IDs (unique references)
        assert machine_ids_dim == expected_machine_ids

        # All sensor fact machine IDs should be from the reference pool
        assert machine_ids_facts.issubset(expected_machine_ids)

    def test_machine_dim_structure(self):
        """Test machine dimension data structure."""
        generator = SyntheticDataGenerator(seed=42)
        schemas = create_machine_example_schemas(num_machines=3, num_sensor_readings=10)

        data = generator.generate_dataset(schemas[0])

        required_fields = [
            "machine_id",
            "machine_name",
            "location",
            "machine_type",
            "manufacturer",
            "installation_date",
            "status",
            "max_temperature",
            "max_pressure",
        ]

        assert all(all(field in record for field in required_fields) for record in data)

    def test_sensor_facts_structure(self):
        """Test sensor facts data structure."""
        generator = SyntheticDataGenerator(seed=42)
        schemas = create_machine_example_schemas(num_machines=3, num_sensor_readings=10)

        data = generator.generate_dataset(schemas[1])

        required_fields = [
            "reading_id",
            "machine_id",
            "timestamp",
            "temperature",
            "pressure",
            "vibration",
            "power_consumption",
            "is_anomaly",
        ]

        assert all(all(field in record for field in required_fields) for record in data)

    def test_reproducibility_with_seed(self):
        """Test that same seed produces same data."""
        schemas = create_machine_example_schemas(num_machines=5, num_sensor_readings=20)

        generator1 = SyntheticDataGenerator(seed=42)
        data1 = generator1.generate_dataset(schemas[0])

        generator2 = SyntheticDataGenerator(seed=42)
        data2 = generator2.generate_dataset(schemas[0])

        assert data1 == data2
