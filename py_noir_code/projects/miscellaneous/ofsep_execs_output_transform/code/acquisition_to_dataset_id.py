import os
import pandas as pd

# Load the CSV file
path = os.path.abspath(os.path.join(os.path.dirname(__file__))) + "/../data/input/tempo.csv"
output = os.path.abspath(os.path.join(os.path.dirname(__file__))) + "/../data/output"

df = pd.read_csv(path, header=None)

# Get columns A and B
columnA = df.iloc[:, 0]
columnB = df.iloc[:, 1]

# Drop rows with NaN in column D and convert to int
df = df.dropna(subset=[df.columns[3]])
df.iloc[:, 3] = df.iloc[:, 3].astype(int)
columnD = df.iloc[:, 3]

# Create mapping from A to B values
tempo = {}
for index, value in enumerate(columnA):
    tempo.setdefault(value, []).append(columnB[index])

# Apply mapping and convert to comma-separated strings
columnD = columnD.apply(lambda x: ",".join(map(str, tempo.get(x, [x]))))

# Assign cleaned column back
df[df.columns[3]] = columnD.astype(str)

# Save cleaned output
df.to_csv(os.path.join(output, "output.csv"), index=False)
