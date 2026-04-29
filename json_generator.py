from pathlib import Path
import os
import tqdm 

def get_all_files_in_test_sql(root_dir: str = "test_sql") -> list[str]:
    """Return all file paths inside test_sql recursively."""
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(f"Directory not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {root}")

    return sorted(str(path) for path in root.rglob("*") if path.is_file())


if __name__ == "__main__":
    files = get_all_files_in_test_sql()
    for file_path in tqdm.tqdm(files):
       os.system(f"./submit.sh {file_path} {file_path.replace('test_sql', 'test_json').replace('.sql', '.json')}") 
