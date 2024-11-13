PyNoir
===

PyNoir is a Python library aiming at facilitate the use of Shanoir APIs through Python scripts.

# Summary

- Repository structure
- Project file constructions
- How to use PyNoir

# Repository structure
- `main.py` file contains parameters

- `projects` directory contains files relative to all project specific scripts
  - `project_switch.py` file contains switch that allows the choice of the project exection. It needs to be modified when a new project script is added to the directory
  - `shanoir_project` directory contains tools that help manipulate shanoir objects
  - `xxx` directory contains project specific scripts using PyNoir lib
- `py_noir` directory contains all ressources and generic code for VIP executions
  - `ressources` directory contains entry files for project VIP scripts (the entry file format must follow its respective specific script)
    - `entry_files` generic directory for entry files deposit
    - `output` generic directory for output files
    - `WIP_files` directory contains working files, it allows the script interruption management
  - `src` directory contains all generic code for VIP executions (as PyNoir user, you should not modify the methods in this directory)
    - `API` directory contains API methods for communication with Shanoir
    - `execution` directory contains queue script methods. Manage the queue, the exceptions and the user feedback
    - `security` directory contains methods managing keycloack authentification
    - `utils` directory contains tools used for generic code

# Project file constructions

Actually, execution management and user feedback are relative to examination, multiple acquisitions and datasets are considered as a unique ressource, regardless of the VIP script.
To create a working project file, it needs to return a list of dictionnary (similar to a json file shap). Care, one of the dictionnary `key/value` pair must follows that shape :

`examinationIdentifier: x`

The key have to be "examinationIdentifier", and it must be the examination id related to the object describes by the dictionnary.

Once project specific script is added to projects directory, it must be added to the projects/project_switch.py.

# How to use PyNoir

First, set up the ShanoirContext in the main.py file. Here is an example :

```python
...
context = ShanoirContext()
context.domain = 'shanoir-ofsep-qualif.irisa.fr'
context.username = 'ymerel'
context.project = projects.CometeMoelle
context.output_folder = 'ressources/output'
context.entry_file = 'ressources/entry_files/input_file.txt' 
...
```

When needed, authentication will be asked through console on runtime.

```shell
$ python3 main.py
Password for Shanoir user ymerel:
```
If there are output, they might be found in the output_folder.
If the script execution is interrupted, just start it again, it will resume where it stopped

