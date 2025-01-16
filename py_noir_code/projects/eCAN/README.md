# RHU eCAN Shanoir Script

This python script aims at automatizing the preparation, filtering and download of Shanoir datasets to be used by segmentation tools in the eCAN project.

The script can take as input : 
    - a list of Shanoir subjectNames in one column of a .csv file if the argument -subjects is used
    - a whole study if the argument -study is used (in this case the name of the study is required and not its Shanoir id, e.g "UCAN" and not 178)

The script will filter the TOF mri with at least 50 instances and a slice thickness inferior or equal to 0.5 mm

How to execute:
```
python ./eCAN.py -lf /tmp/test.log -u {your_shanoir_username} -d shanoir.irisa.fr -out ./testEcan -subjects ./patientIDs.csv
```

