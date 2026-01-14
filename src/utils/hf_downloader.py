from pathlib import Path
from typing import Optional, List, Union
import os
from huggingface_hub import snapshot_download


def download_dataset(
    repo_id: str,
    local_dir: Union[str, Path],
    repo_type: str = "dataset",
    allow_patterns: Optional[Union[List[str], str]] = None,
    ignore_patterns: Optional[Union[List[str], str]] = None,
    token: Optional[str] = None,
) -> str:
    """
    Downloads a dataset from Hugging Face Hub.

    Args:
        repo_id (str): The ID of the repository (e.g., 'username/repo-name').
        local_dir (Union[str, Path]): Local directory to download the dataset to.
        repo_type (str): Type of the repository ('dataset', 'model', or 'space'). Defaults to 'dataset'.
        allow_patterns (Optional[Union[List[str], str]]): Patterns to allow (e.g., ["*.csv", "*.json"]).
        ignore_patterns (Optional[Union[List[str], str]]): Patterns to ignore.
        token (Optional[str]): Hugging Face API token.

    Returns:
        str: The local directory path where the dataset was downloaded.
    """

    # Default ignore patterns to avoid downloading git related files if not specified
    if ignore_patterns is None:
        ignore_patterns = [".gitattributes", ".git", ".gitignore"]

    print(f"Downloading {repo_id} to {local_dir}...")

    local_dir_path = snapshot_download(
        repo_id=repo_id,
        local_dir=local_dir,
        repo_type=repo_type,
        allow_patterns=allow_patterns,
        ignore_patterns=ignore_patterns,
        token=token,
        local_dir_use_symlinks=False,  # Download actual files, not symlinks for easier local usage
    )

    print(f"Successfully downloaded to: {local_dir_path}")
    return local_dir_path


if __name__ == "__main__":
    # Example usage: Download alrq/subway dataset
    # This matches the user request to create a utility to download this specific dataset

    # Define default paths relative to the project root
    # Assuming this script is in src/utils/, so project root is two levels up
    project_root = Path(__file__).resolve().parent.parent.parent
    target_dir = project_root / "data" / "subway"

    repo_id = "alrq/subway"

    try:
        download_dataset(repo_id=repo_id, local_dir=target_dir)
    except Exception as e:
        print(f"An error occurred: {e}")
