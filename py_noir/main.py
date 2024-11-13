import sys

from src.API.shanoir_context import ShanoirContext

sys.path.append('../')
sys.path.append('../projects/shanoir_object/dataset')


from py_noir.src.execution.execution_init_service import init_executions

if __name__ == '__main__':
    context = ShanoirContext()
    context.domain = 'shanoir-ofsep-qualif.irisa.fr'
    context.username = 'jcdouteau'
    context.project = 'CometeMoelle'
    context.output_folder = './ressources/output'
    context.entry_file = 'ressources/entry_files/COMETE-M_exams_baseline.txt'
    init_executions(context)
