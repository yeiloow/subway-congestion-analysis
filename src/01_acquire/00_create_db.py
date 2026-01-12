import logging
import time
import os
import sys
import importlib.util

# Add project root to sys.path to allow importing src.utils
# This file is in src/01_acquire/, so root is ../..
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.config import LOG_LEVEL, LOG_FORMAT

# Configure Logging Globally
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def import_module_from_path(module_name, file_path):
    """
    Dynamically import a module from a file path.
    This is necessary because the directory and file names start with numbers,
    which are not valid Python identifiers for standard imports.
    """
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None:
            logger.error(f"Could not load spec for {module_name} from {file_path}")
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        logger.error(f"Failed to import {module_name} from {file_path}: {e}")
        return None


def main():
    start_time = time.time()
    logger.info("Starting Database Creation and Population Process...")

    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Define steps
    steps = [
        ("01_init_db.py", "run_init_db", "Step 1: Initialize Database"),
        (
            "02_insert_subway.py",
            "run_insert_subway",
            "Step 2: Insert Subway Information",
        ),
        (
            "03_insert_congestion.py",
            "run_insert_congestion",
            "Step 3: Insert Congestion Data",
        ),
        (
            "04_insert_floating_population.py",
            "run_insert_floating_population",
            "Step 4: Insert Floating Population Data",
        ),
        (
            "04_insert_living_population.py",
            "run_insert_living_population",
            "Step 5: Insert Living Population Data",
        ),
        (
            "05_insert_estimated_revenue.py",
            "run_insert_estimated_revenue",
            "Step 6: Insert Estimated Revenue Data",
        ),
    ]

    for filename, func_name, step_desc in steps:
        logger.info(f">>> {step_desc}")
        file_path = os.path.join(current_dir, filename)

        module_name = filename.replace(".py", "")
        module = import_module_from_path(module_name, file_path)

        if module and hasattr(module, func_name):
            try:
                func = getattr(module, func_name)
                func()
            except Exception as e:
                logger.error(f"Error executing {func_name} in {filename}: {e}")
                # Decide whether to continue or stop. For now, we continue but mark failure.
        else:
            logger.error(f"Could not find function '{func_name}' in {filename}")

    elapsed_time = time.time() - start_time
    logger.info(
        f"All steps completed (check logs for errors) in {elapsed_time:.2f} seconds."
    )


if __name__ == "__main__":
    main()
