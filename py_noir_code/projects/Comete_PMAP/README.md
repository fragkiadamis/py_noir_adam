Comete - Moelle integration
===

# VIP

## Sequence
![](https://notes.inria.fr/uploads/upload_b22721e6cfa8e12f1395e34664c0b622.png)

## Boutiques

```
{
  "name": "comete_moelle",
  "tool-version": "0.1",
  "author": "EMPENN",
  "description": "Spinal cord lesion segmentation from T2 and STIR images.<br />",
  "command-line": "unzip [INPUT_T2] -d [INPUT_T2]_input; [INPUT_STIR] -d [INPUT_STIR]_input; mkdir output; python3 inference_multimodal.py -t [INPUT_T2]_input -s [INPUT_STIR]_input \ -o /app_primus/data/outputs; tar -cvzf [OUTPUT_FILE] output_folder && rm -rf output_folder && chmod -R 777 *",
  "schema-version": "0.5",
   "container-image":{
	  "type": "docker",
	  "image": "comete_moelle:latest"
  },
  "inputs": [{
      "id": "t2_archive",
      "name": "T2 archive",
      "type": "File",
      "description": "NIfTI ZIP of T2 image",
      "value-key": "[INPUT_T2]"
    },
    {
      "id": "stir_archive",
      "name": "STIR archive",
      "type": "File",
      "description": "NIfTI ZIP of STIR images",
      "value-key": "[INPUT_STIR]"
    }
  ],
  "output-files": [{
      "description" : "Tarball containing results. ",
      "id" : "outarchive",
      "name" : "Output archive",
      "path-template": "[INPUT_T2]_output.tgz",
      "value-key": "[OUTPUT_FILE]"
    }]
}
```
 
# Pipeline #1 : Segmentation

## Initial Shanoir tree

```=tree
.
└── 🩺 Examination 15/04/2024
    ├── 🛏️ Acquisition T2_SAG_1
    ├── 🛏️ Acquisition T2_SAG_2
    ├── 🛏️ Acquisition T2_STIR_1
    └── 🛏️ Acquisition T2_STIR_2
```

## Execution

- For each acquisition
    - Filter out non spinal cord acquisition
    - Identify T2 : dataset name contains "T2"
    - Identify STIR : dataset name contains "STIR"
    -  For each T2
        -  Send all STIR
            -  The pipeline will associate each T2 with the right STIR
                - And return a PMAP NIfTI

```=tree
.
├── 🚀 Execution #1.1
│   ├── 📂 INPUT
│   │   ├── T2_SAG_1.nii.gz
│   │   └── {T2_STIR_1.nii.gz, T2_STIR_2.nii.gz}
│   └── 📂 OUTPUT
│       └── PMAP_1.nii.gz
└── 🚀 Execution #1.2
    ├── 📂 INPUT
    │   ├── T2_SAG_2.nii.gz
    │   └── {T2_STIR_1.nii.gz, T2_STIR_2.nii.gz}
    └── 📂 OUTPUT
        └── PMAP_2.nii.gz
```

## Resulting Shanoir tree

- **1** processed dataset per execution
- Associated to **all** input acquisition/datasets

```=tree
.
└── 🩺 Examination 15/04/2024
    ├── 🛏️ Acquisition T2_SAG_1
    │   └── ⚙️ Processing Execution #1.1
    │       └── 📷 Processed Dataset PMAP_1
    ├── 🛏️ Acquisition T2_SAG_2
    │   └── ⚙️ Processing Execution #1.2
    │       └── 📷 Processed Dataset PMAP_2
    ├── 🛏️ Acquisition T2_STIR_1
    │   ├── ⚙️ Processing Execution #1.1
    │   │   └── 📷 Processed Dataset PMAP_1
    │   └── ⚙️ Processing Execution #1.2
    │       └── 📷 Processed Dataset PMAP_2
    └── 🛏️ Acquisition T2_STIR_2
        ├── ⚙️ Processing Execution #1.1
        │   └── 📷 Processed Dataset PMAP_1
        └── ⚙️ Processing Execution #1.2
            └── 📷 Processed Dataset PMAP_2
```

# Pipeline #2 : Fusion

- For each examination
    - Send all processed datasets
        - The pipeline will merge the segmented NIfTI
            - And return a structured file (JSON ?)

## Initial shanoir tree

```=tree
.
└── 🩺 Examination 15/04/2024
    ├── 🛏️ Acquisition T2_SAG_1
    │   └── ⚙️ Processing Execution #1.1
    │       └── 📷 Processed Dataset PMAP_1
    ├── 🛏️ Acquisition T2_SAG_2
    │   └── ⚙️ Processing Execution #1.2
    │       └── 📷 Processed Dataset PMAP_2
    ├── 🛏️ Acquisition T2_STIR_1
    │   ├── ⚙️ Processing Execution #1.1
    │   │   └── 📷 Processed Dataset PMAP_1
    │   └── ⚙️ Processing Execution #1.2
    │       └── 📷 Processed Dataset PMAP_2
    └── 🛏️ Acquisition T2_STIR_2
        ├── ⚙️ Processing Execution #1.1
        │   └── 📷 Processed Dataset PMAP_1
        └── ⚙️ Processing Execution #1.2
            └── 📷 Processed Dataset PMAP_2
```

## Execution

- For each examination, send all processed datasets

```=tree
.
└── 🚀 Execution #3
    ├── 📂 INPUT
    │   └── { PMAP_1.nii.gz, PMAP_2.nii.gz }
    └── 📂 OUTPUT
        └── RESULT_1.zip
```

## Resulting Shanoir tree

- Produce a processed dataset
- Associated to what ? Input processed datasets ?

```=tree
.
└── 🩺 Examination 15/04/2024
    ├── 🛏️ Acquisition T2_SAG_1
    │   └── ⚙️ Processing Execution #1.1
    │       └── 📷 Processed Dataset PMAP_1
    │           └── ⚙️ Processing Execution #2
    │               └── 📷 Processed Dataset RESULT_1
    ├── 🛏️ Acquisition T2_SAG_2
    │   └── ⚙️ Processing Execution #1.2
    │       └── 📷 Processed Dataset PMAP_2
    │           └── ⚙️ Processing Execution #2
    │               └── 📷 Processed Dataset RESULT_1
    ├── 🛏️ Acquisition T2_STIR_1
    │   ├── ⚙️ Processing Execution #1.1
    │   │   └── 📷 Processed Dataset PMAP_1
    │   │       └── ⚙️ Processing Execution #2
    │   │            └── 📷 Processed Dataset RESULT_1
    │   └── ⚙️ Processing Execution #1.2
    │       └── 📷 Processed Dataset PMAP_2
    │           └── ⚙️ Processing Execution #2
    │               └── 📷 Processed Dataset RESULT_1
    └── 🛏️ Acquisition T2_STIR_2
        ├── ⚙️ Processing Execution #1.1
        │   └── 📷 Processed Dataset PMAP_1
        │           └── ⚙️ Processing Execution #2
        │               └── 📷 Processed Dataset RESULT_1
        └── ⚙️ Processing Execution #1.2
            └── 📷 Processed Dataset PMAP_2
               └── ⚙️ Processing Execution #2
                   └── 📷 Processed Dataset RESULT_1
```




