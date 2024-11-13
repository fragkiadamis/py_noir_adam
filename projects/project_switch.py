import string
import sys

from projects.CometeMoelle.comete_moelle_json_generator import generate_comete_moelle_json
from py_noir.src.API.shanoir_context import ShanoirContext

def generate_json(context: ShanoirContext):

    match context.project:
        case "CometeMoelle":
            return generate_comete_moelle_json(context)
        case "eCAN":
            return
        case _:
            print("Project described with context.project defined in the py_noir/src/main.py is not found in projects/project_switch.py. Please verify both main.py and project_switch.py.")
            sys.exit(1)