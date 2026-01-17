"""Utilities for Databricks operations using the Databricks SDK."""

import logging
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

logger = logging.getLogger(__name__)


def create_volume_if_not_exists(
    catalog: str,
    schema: str,
    volume_name: str,
    comment: Optional[str] = None,
    workspace_client: Optional[WorkspaceClient] = None,
) -> Path:
    """Create a Databricks Unity Catalog managed volume if it does not exist.

    This function checks if a managed volume exists in Unity Catalog and creates
    it if it doesn't. Returns the volume path as a Path object.

    Parameters
    ----------
    catalog : str
        The name of the Unity Catalog catalog.
    schema : str
        The name of the schema within the catalog.
    volume_name : str
        The name of the volume to create.
    comment : str, optional
        Optional comment to describe the volume.
    workspace_client : WorkspaceClient, optional
        Databricks workspace client. If None, a new client will be created
        using default authentication.

    Returns
    -------
    Path
        The full path to the volume (/Volumes/catalog/schema/volume_name).

    Examples
    --------
    >>> # Create a managed volume
    >>> volume_path = ensure_volume_exists(catalog="my_catalog", schema="my_schema", volume_name="my_volume")
    >>> print(volume_path)
    PosixPath('/Volumes/my_catalog/my_schema/my_volume')
    """
    # Initialize workspace client if not provided
    if workspace_client is None:
        workspace_client = WorkspaceClient()

    # Construct the full volume name
    full_volume_name = f"{catalog}.{schema}.{volume_name}"

    try:
        # Check if volume exists
        workspace_client.volumes.read(full_volume_name)
        logger.info(f"Volume '{full_volume_name}' already exists.")
    except Exception:
        # Volume doesn't exist, create it
        logger.info(f"Creating volume '{full_volume_name}'...")

        workspace_client.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=volume_name,
            volume_type=VolumeType.MANAGED,
            comment=comment,
        )

        logger.info(f"Successfully created volume '{full_volume_name}'.")

    # Return the path to the volume
    return Path(f"/Volumes/{catalog}/{schema}/{volume_name}")
