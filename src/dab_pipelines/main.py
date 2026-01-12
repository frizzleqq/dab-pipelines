import argparse
from datetime import UTC, datetime

from databricks.sdk.runtime import spark

from dab_pipelines import databricks_utils, taxis
from dab_pipelines.synthetic_data_generator import SyntheticDataGenerator, create_machine_example_schemas


def generate_data(args):
    """Generate synthetic test data to Unity Catalog Volumes.

    Parameters
    ----------
    args : argparse.Namespace
        Command-line arguments containing catalog, schema, volume, and optional parameters.
    """
    # Ensure the volume exists before writing data
    output_path = databricks_utils.create_volume_if_not_exists(
        catalog=args.catalog,
        schema=args.schema,
        volume_name=args.volume,
    )

    print(f"Generating synthetic data to: {output_path}")

    # Create generator with seed for reproducibility
    generator = SyntheticDataGenerator(seed=args.seed, timestamp=datetime.now(tz=UTC))

    # Generate machine data schemas
    schemas = create_machine_example_schemas(num_machines=args.num_machines, num_sensor_readings=args.num_readings)

    # Generate and save
    file_paths = generator.generate_and_save(schemas, output_path)

    print(f"\nSuccessfully generated {len(file_paths)} datasets:")
    for name, path in file_paths.items():
        print(f"  - {name}: {path}")


def run_job(args):
    """Run the main Databricks job.

    Parameters
    ----------
    args : argparse.Namespace
        Command-line arguments containing catalog and schema.
    """
    # Set the default catalog and schema
    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    # Example: just find all taxis from a sample catalog
    taxis.find_all_taxis().show(5)


def main():
    """Main CLI entry point with subcommands."""
    parser = argparse.ArgumentParser(
        description="Databricks pipeline utilities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subcommand: generate-data
    generate_parser = subparsers.add_parser(
        "generate-data", help="Generate synthetic test data to Unity Catalog Volumes"
    )
    generate_parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    generate_parser.add_argument("--schema", required=True, help="Schema name")
    generate_parser.add_argument("--volume", required=True, help="Volume name")
    generate_parser.add_argument(
        "--num-machines", type=int, default=10, help="Number of machines to generate (default: 10)"
    )
    generate_parser.add_argument(
        "--num-readings", type=int, default=1000, help="Number of sensor readings to generate (default: 1000)"
    )
    generate_parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    generate_parser.set_defaults(func=generate_data)

    # Subcommand: run
    run_parser = subparsers.add_parser("run", help="Run the main Databricks job")
    run_parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    run_parser.add_argument("--schema", required=True, help="Schema name")
    run_parser.set_defaults(func=run_job)

    # Parse arguments
    args = parser.parse_args()

    # Execute the appropriate function based on subcommand
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
