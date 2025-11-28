
# py_noir

`py_noir` is a Python CLI application for automating dataset execution and processing in **Shanoir-NG**, primarily designed for medical imaging workflows.
Generic Shanoir and executions tools through Shanoir API are available too in that project.

---

## Table of Contents
- [Features](#features)
- [Setup](#setup)
- [Project Structure](#project-structure)
- [Dependencies](#dependencies)

---

## Features
- Use the Shanoir-NG API.
- User the Orthanc API and DICOM webstore.
- Prepare and launch mass VIP executions -- resume incomplete runs.

---

## Setup

1. **Clone the repository**
```bash
git clone https://github.com/fli-iam/py_noir.git
cd py_noir
```

2. **Install UV python package manager and sync the project**
```bash
pip install uv
uv sync
```

2. **Config your root_path project**
Modify the rootPath in config/config.conf according to your project path 

4.
**Finally you can run the project pipeline you need**
```bash
uv run main.py [your_app] [your_feature]
```
An example : uv run main.py ecan execute  