"""
Helper module to run the MLflow tracking server.

    python -m training.mlflow_ui

Starts ``mlflow server`` bound to 127.0.0.1:5000 backed by a local SQLite
database (training/mlflow.db) with artifacts in training/mlartifacts/.
"""

import os
import subprocess
import sys
from pathlib import Path

DEFAULT_BACKEND_URI = "sqlite:///training/mlflow.db"
DEFAULT_ARTIFACT_ROOT = "training/mlartifacts"


def run_mlflow_server(host: str = "127.0.0.1", port: int = 5000) -> None:
    """Start the MLflow tracking server and keep it attached to the current terminal."""

    backend_uri = os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_BACKEND_URI)

    artifact_root = os.environ.get(
        "MLFLOW_ARTIFACT_ROOT", DEFAULT_ARTIFACT_ROOT
    )

    if artifact_root and "://" not in artifact_root:
        Path(artifact_root).mkdir(parents=True, exist_ok=True)
        artifact_root = str(Path(artifact_root).resolve())

    cmd = [
        sys.executable,
        "-m",
        "mlflow",
        "server",
        "--backend-store-uri",
        backend_uri,
        "--default-artifact-root",
        artifact_root,
        "--host",
        host,
        "--port",
        str(port),
    ]

    print("Starting MLflow server with command:")
    print(" ", " ".join(cmd))
    print(f"Open http://{host}:{port} in your browser.")

    subprocess.run(cmd, check=False, shell=False)


if __name__ == "__main__":
    run_mlflow_server()
