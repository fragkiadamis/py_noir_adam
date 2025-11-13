import os
import string
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

def rename_datasets(base_dir: str):
    """Rename subdirectories and their subdirectories as Dataset_A, Dataset_A_1, Dataset_A_2, ..."""
    dataset_letters = string.ascii_uppercase
    dataset_index = 0
    path = os.path.abspath(os.path.join(os.path.dirname(__file__))) + base_dir

    # First-level subdirectories
    for subdir_name in sorted(os.listdir(path)):
        subdir_path = os.path.join(path, subdir_name)
        if not os.path.isdir(subdir_path):
            continue

        # Assign Dataset_A, Dataset_B, ...
        dataset_name = f"Dataset_{dataset_letters[dataset_index]}"
        new_subdir_path = os.path.join(path, dataset_name)
        os.rename(subdir_path, new_subdir_path)
        print(f"Renamed {subdir_path} → {new_subdir_path}")

        # Rename sub-subdirectories
        subsub_index = 1
        for subsub_name in sorted(os.listdir(new_subdir_path)):
            subsub_path = os.path.join(new_subdir_path, subsub_name)
            if os.path.isdir(subsub_path):
                new_subsub_name = f"{dataset_name}_{subsub_index}"
                new_subsub_path = os.path.join(new_subdir_path, new_subsub_name)
                os.rename(subsub_path, new_subsub_path)
                print(f"Renamed {subsub_path} → {new_subsub_path}")
                subsub_index += 1

        dataset_index += 1

# Example usage
rename_datasets("/../data/input")
