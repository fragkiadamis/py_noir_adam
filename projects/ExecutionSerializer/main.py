import sys

from config.config import Config

sys.path.append(Config.rootPath)

from src.utils.config_utils import load_config
from src.utils.file_utils import get_working_file_paths
from src.utils.main_utils import get_args

if __name__ == '__main__':

    project_name, configs = get_args(sys.argv)

    load_config(configs)
    working_file, save_file= get_working_file_paths(project_name)

    print("All good")

    # if not os.path.exists(working_file):
    #     _ = init_executions(working_file, json_generator(project_name))
    # else:
    #     _ = resume_executions(working_file, save_file)


