import base64 
import json
import glob 

globlist = sorted(glob.glob("job_backup/*"))
file = globlist[-1]
with open(file, "r") as f:
    auth = json.load(f)
    source = auth["source"]
    source = base64.b64decode(source)
    print(auth["args"])
    with open("test2", "wb") as f:
        f.write(source)

        