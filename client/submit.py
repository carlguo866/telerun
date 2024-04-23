import argparse
import urllib
import urllib.parse
import urllib.request
import ssl
import os
import json
import traceback
import time
import base64

timeout = 120 # seconds

server_cert = """
-----BEGIN CERTIFICATE-----
MIIFmjCCA4KgAwIBAgIUDqUoEkI/a8wLiGh+vZlMnLkpSdswDQYJKoZIhvcNAQEL
BQAwVDELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAk1BMRIwEAYDVQQHDAlDYW1icmlk
Z2UxDDAKBgNVBAoMA01JVDEWMBQGA1UEAwwNNTQuMjI3LjEzNi40MDAeFw0yNDA0
MTYxNTEyMzNaFw0yNTA0MTYxNTEyMzNaMFQxCzAJBgNVBAYTAlVTMQswCQYDVQQI
DAJNQTESMBAGA1UEBwwJQ2FtYnJpZGdlMQwwCgYDVQQKDANNSVQxFjAUBgNVBAMM
DTU0LjIyNy4xMzYuNDAwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQCd
20ew1JmkO5HQr/xzOkJTCEnSeg4E1xUEYLG4i4TKJMmo23ekOKeCzGvaZh1dZiNR
9pWMASAzmOr9Jz5bgKb6FDAT7vwidTLgX485rnNyAoMTD3tAckUezUiwFcX0ZgS4
z7e/T36ok5ViIDo+knVdG1IaeJOVYJKGcMbgimHcdEA7/VD1VddSVHDMQsX1fv5N
+3AtLGEEXBDxqqv24IQTBnrB2UshdBTSCNWjC0JVxHyikZBJbTmlzBOzDshsiuda
VmMeODmR+pQ1l0tDvbIlf+27qeHvhIJ0n/t44gdsgCtekgD8uU20EFWd0m//TNgo
1TmzZNK8i/kuyhV6VznETJWN+1zmAQt00Qtyv1b+plgmEqW19TMRx3e8WF3Ldl3+
L37FkjDRXsoDh00G11vS2JcCck0HECb526to+714uSB8bJternC4MxCmkzQTgrEn
q7v606CIFrzgGfljZIP7aZjy63YgCGDehs2vhQmIAljgABVUb4Zf4tSoyt0W5Pxs
0LGU2MoxMiar8HLIrqujC4asTonMl0Vf0kGgRNjBMD+qPhdAasKyBeOHv+tBALCB
um6vPHcVYAlG47VXtnaCYHcKolGZzBpDSOLWSAdsYFN1q4TyUcLZUkn6N5KVDrjE
jwckwX4KAUkzA9xqZAeM3/yXtyQCOWjfGR7y29giDwIDAQABo2QwYjAdBgNVHQ4E
FgQUEhgyGoMFpa//tHRQuxePJ+LgUkcwHwYDVR0jBBgwFoAUEhgyGoMFpa//tHRQ
uxePJ+LgUkcwDwYDVR0TAQH/BAUwAwEB/zAPBgNVHREECDAGhwQ244goMA0GCSqG
SIb3DQEBCwUAA4ICAQCXKb5CcngkfoIMETSTbR0rUV81DgM3DSd9KK12gRkQJ+Nf
IXT3Kh4nQnoe6ns0UaSXTpqUqcKIoODUIk1zA3BQra+yRLFpm4aaP84wzxNmv+zW
w+8PTb4GQzIx/GkdEiUcrgZkhHMcAJT9hNAyCVfB8/CZPhDranXf+gRYh6aCG7pl
yqinhhyfwf+5Uz0+6wUKUbHpWTGyLFF9jZmKyoTwt9/Sma+QL9fwwM5ApeQOSTia
+wjqMqKfgdh8iGatRCWDo1G0RQFildU0Y9uZT6+s0yNBpvkfkNc3/E/s6W9VbSWi
3stMPKVywI+xMPu49Vnc0GEPxBOctZ5q2SIlXhhuyH2iwE2F4lCsiaeflu2h2t0l
3suBSoHin8v5+x/n8O0UtVDEi9D3ggpMqXh329X35FZyGwKlfNgMASJsNqF1T/1b
WM2NYmioytRzNHuPLVdAa5YMwV3rTXw3q8A8xMxy/+HQzIbcKF7Fbg/yO/4tlC2v
ATCzmZaguPG1JHN22LT0CBFUdjZ2AUMIw1e8bjB1LXPBefBSStisBaUbmNx9spek
n4DTjHBOOm3OmcLo/47+Y1v14SPVSfp6IgBJLgnpW4HZ/QM/uWkEaRDsqTCYp8ug
kiLGufRDq91L4h7vufATRyVEFUCgcnpZhjpPfli9xSrE3ZrL8UM2ArEB/7/nWw==
-----END CERTIFICATE-----
"""

server_ip_port = "54.227.136.40:4443"

poll_interval = 0.25 # seconds

def submit_job(username, token, file_path, script_args, ssl_ctx, override_pending=False):
    query_params = {"username": username, "token": token}
    if override_pending:
        query_params["override_pending"] = "1"
    url_query = urllib.parse.urlencode(query_params)
    url = "https://" + server_ip_port + "/api/submit?" + url_query
    
    with open(file_path, 'rb') as file:
        file_content = file.read()
        base64_encoded = base64.b64encode(file_content).decode("utf-8")
        req_json = json.dumps({"source": base64_encoded, "args": script_args}).encode("utf-8")
    request = urllib.request.Request(url, data=req_json, method="POST")
    request.add_header("Content-Type", "application/json")
    
    try:
        response = urllib.request.urlopen(request, context=ssl_ctx)
        response_json = json.load(response)
        return response_json["job_id"]
    except urllib.error.HTTPError as e:
        if e.code == 400:
            response_json = json.load(e)
            if response_json["error"] == "pending_job":
                return None
        raise e

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--auth",
        help="Authentication token (defaults to ./auth.json in the same directory as this script)",
    )
    parser.add_argument("--override-pending", action="store_true", help="Allow overriding pending jobs")
    parser.add_argument("file", help="CUDA source file to submit")
    parser.add_argument('script_args', nargs=argparse.REMAINDER, help='Arguments for the script')
    args = parser.parse_args()
    
    
    remaining_args = []
    skip_next = False
    for arg in args.script_args:
        if arg == '--auth':
            skip_next = True
        elif skip_next:
            args.auth = arg
        elif arg.startswith('--override-pending'):
            args.override_pending = True
        else:
            remaining_args.append(arg)
    
    script_args = ' '.join(remaining_args)

    token_path = args.auth or os.path.join(os.path.dirname(__file__), "auth.json")
    with open(token_path, "r") as f:
        auth = json.load(f)
    username = auth["username"]
    token = auth["token"]

    source = args.file
    ssl_ctx = ssl.create_default_context(cadata=server_cert)
    job_id = submit_job(username, token, source, script_args, ssl_ctx, override_pending=args.override_pending)
    if job_id is None:
        print("You already have a pending job. Pass '--override-pending' if you want to replace it.")
        exit(1)
    
    print("Submitted job", job_id)

    already_claimed = False
    old_time = time.time()
    while True:
        time.sleep(poll_interval)
        
        if time.time() - old_time > timeout:
            raise TimeoutError
        try:
            url_query = urllib.parse.urlencode({"username": username, "token": token, "job_id": job_id})
            req = urllib.request.Request(
                "https://" + server_ip_port + "/api/status?" + url_query,
                method="GET",
            )
            with urllib.request.urlopen(req, context=ssl_ctx) as f:
                response = json.load(f)
            
            state = response["state"]
            if state == "pending":
                continue
            elif state == "claimed":
                if not already_claimed:
                    print("Compiling and running, took {:.2f} seconds to be claimed.".format(time.time() - old_time)) 
                    print("This may take a while, please do not close this window, as your job will be lost.")
                    already_claimed = True
                continue
            elif state == "complete":
                # TODO: Don't double-nest JSON!
                result = json.loads(response["result"])["result_json"]
                if result["success"]:
                    print("Job completed successfully.")
                else:
                    print("Job failed.")
                print()
                print("--- Execution log:")
                print()
                print(result["execute_log"])
                break
        except Exception as e:
            traceback.print_exc()
            continue

if __name__ == "__main__":
    main()
