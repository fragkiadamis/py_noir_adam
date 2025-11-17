from src.utils.file_utils import find_project_root, create_file_path


def create_tracking_file(project_name : str):
    tracking_file = get_tracking_file(project_name)
    return

def get_tracking_file(project_name : str):
    tracking_file_path = find_project_root(__file__) + "/py_noir_code/resources/tracking_files/"
    create_file_path(tracking_file_path)
    return tracking_file_path + project_name + ".csv"