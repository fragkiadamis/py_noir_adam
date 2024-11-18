from py_noir_code.projects.CometeMoelle.comete_moelle_json_generator import generate_comete_moelle_json
from py_noir_code.src.execution.execution_init_service import init_executions
from py_noir_code.src.utils.context_utils import load_context

if __name__ == '__main__':
    load_context()
    json_content = generate_comete_moelle_json()
    init_executions(json_content)
