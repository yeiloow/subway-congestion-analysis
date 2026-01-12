from pathlib import Path
from typing import Optional, Union, List
import os
from huggingface_hub import HfApi, login


def upload_to_hf(
    repo_id: str,
    path_or_fileobj: Union[str, Path],
    path_in_repo: Optional[str] = None,
    repo_type: str = "dataset",
    token: Optional[str] = None,
    commit_message: Optional[str] = None,
    create_repo: bool = True,
) -> str:
    """
    Uploads a file or folder to the Hugging Face Hub.

    Args:
        repo_id (str): The ID of the repository (e.g., 'username/repo-name').
        path_or_fileobj (Union[str, Path]): Local path to the file or folder to upload.
        path_in_repo (Optional[str]): Path in the repository. Defaults to the filename if uploading a file.
            If uploading a folder, this is the directory in the repo where the folder contents will be uploaded.
        repo_type (str): Type of the repository ('dataset', 'model', or 'space'). Defaults to 'dataset'.
        token (Optional[str]): Hugging Face API token. If None, uses the 'HF_TOKEN' environment variable.
        commit_message (Optional[str]): Commit message.
        create_repo (bool): Whether to create the repository if it doesn't exist.

    Returns:
        str: The URL of the uploaded file or the commit URL.
    """

    hf_token = token or os.getenv("HF_TOKEN")
    if not hf_token:
        # Try to login if token is not explicit locally
        try:
            # This might interactively ask if not logged in, but we assume environment or local config
            # If hf_hub is already logged in via cli, this step might be skippable or handled by HfApi calls directly
            pass
        except Exception:
            raise ValueError(
                "No Hugging Face token provided and HF_TOKEN environment variable not set."
            )

    api = HfApi(token=hf_token)

    if create_repo:
        try:
            api.create_repo(repo_id=repo_id, repo_type=repo_type, exist_ok=True)
        except Exception as e:
            print(
                f"Note: Repo creation check failed or popped a warning (might be fine if it exists): {e}"
            )

    path = Path(path_or_fileobj)
    if not path.exists():
        raise FileNotFoundError(f"File or directory not found: {path}")

    if path.is_file():
        print(f"Uploading file: {path} to {repo_id}")
        return api.upload_file(
            path_or_fileobj=path,
            path_in_repo=path_in_repo or path.name,
            repo_id=repo_id,
            repo_type=repo_type,
            commit_message=commit_message or f"Upload {path.name}",
        )
    elif path.is_dir():
        print(f"Uploading directory: {path} to {repo_id}")
        return api.upload_folder(
            folder_path=path,
            path_in_repo=path_in_repo,
            repo_id=repo_id,
            repo_type=repo_type,
            commit_message=commit_message or f"Upload folder {path.name}",
        )
    else:
        raise ValueError(f"Invalid path type for: {path}")


if __name__ == "__main__":
    # Example usage (commented out to prevent accidental execution)
    # import dotenv
    # dotenv.load_dotenv()
    # upload_to_hf(repo_id="your-user/your-repo", path_or_fileobj="path/to/file.txt")
    pass
