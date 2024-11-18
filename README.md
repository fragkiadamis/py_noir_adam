PyNoir
===

PyNoir is a Python library aiming at facilitate the use of Shanoir APIs through Python scripts.

# Summary

- Repository structure
- Project directory constructions
- How to use PyNoir

# Repository structure


- `py_noir_code` directory contains all resources, generic and specific code for projects executions
  - `projects` directory contains directories relative to all specific project code and resources
    - `project` directory contains specific code and resources for a project. Ensure that there is a main.py and a context.conf inside.
    - `main_example.py` file example of a main.py in the context of py_noir executions (entry point of executions)
    - `context_example.conf` file example of a context.conf in the context of py_noir executions (configuration file for executions)
  - `ressources` directory contains various resources directories
    - `logs` generic directory for project logs
    - `WIP_files` directory contains working files, it allows the script interruption management
  - `src` directory contains all generic code for projects executions
    - `API` directory contains API methods for communication with Shanoir
    - `execution` directory contains queue script methods. Manage the queue, the exceptions and the user feedback
    - `security` directory contains methods managing keycloack authentification
    - `shanoir_object` directory contains tools that help manipulate shanoir objects
    - `utils` directory contains tools used for generic code


# Project directory constructions

Below is a description of the generic procedure of py_noir usage. Feel free to adapt the code to your project :

Based on the project directory, it must contain :
  
  - A context.conf file containing some parameters for the project execution
  - A main.py file which has to call **init_executions(json_content)**, where json_content corresponds to the data sent to Shanoir.
  - *Optional* An entry file when needed, the open_project_file method makes the file management easier

In py_noir_code/projects, you can see examples of both main.py and context.conf files.

# How to use PyNoir

First, set up the context in the context.conf file. Here is an example :

```python
[API context]

scheme = https
domain = shanoir-ofsep-qualif.irisa.fr
verify = True
timeout = None
proxies = {}
username = ghubert
clientId = shanoir-uploader
access_token = None
refresh_token = None

[Execution context]

max_thread = 3

```

When needed, authentication will be asked through console on runtime.

```shell
$ python3 main.py
Password for Shanoir user ymerel:
```
If the script execution is interrupted, just start it again, it will resume where it stopped

