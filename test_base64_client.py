import json 
import base64
with open("client/rotate", "rb") as f:
    file_content = f.read()
    # print(file_content)
    base64_encoded = base64.b64encode(file_content).decode("utf-8")
    
    json_data = json.dumps(base64_encoded)
    
with open("test.json", "w") as f:
    f.write(json_data)

with open("test.json", "r") as f:
    data = json.load(f)
    print(data)
    base64_decoded = base64.b64decode(data)
    print(base64_decoded)
    assert(base64_decoded == file_content)
    
with open("test", "wb") as f:
    f.write(base64_decoded)
with open("test2", "wb") as f:
    f.write(file_content)