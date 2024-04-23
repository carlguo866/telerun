import urllib
import urllib.parse
import urllib.request
import ssl
import os
import json
import time
import traceback
import multiprocessing
import subprocess
from dataclasses import dataclass
from typing import Optional
import argparse
import uuid
import base64
import shutil

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

compile_timeout = 60 # seconds
execute_timeout = 60 # seconds

max_log_length = 1 << 20

compute_capability = "86"

# @dataclass
# class CompileJob:
#     job_id: int
#     job_dir: str
#     source: str

@dataclass
class ExecuteJob:
    job_id: int
    job_dir: str
    source_dir: str
    job_args: str

@dataclass
class CompleteJob:
    job_id: int
    job_dir: str
    success: bool
    # compile_log: str
    execute_log: Optional[str] = None

def src_path(job_dir: str) -> str:
    return os.path.join(job_dir, "execute")

def bin_path(job_dir: str) -> str:
    return os.path.join(job_dir, "bin")

def claim_worker(execute_queue, auth, scratch_dir: str):
    ssl_ctx = ssl.create_default_context(cadata=server_cert)
    
    auth_name = auth["executor"]
    auth_token = auth["token"]

    while True:
        time.sleep(poll_interval)
        try:
            url_query = urllib.parse.urlencode({"executor": auth_name, "token": auth_token})

            req = urllib.request.Request(
                "https://" + server_ip_port + "/api/claim?" + url_query,
                method="POST",
            )
            with urllib.request.urlopen(req, context=ssl_ctx) as f:
                response = json.load(f)
            
            assert response["success"]

            job_id = response["job_id"]
            if job_id is None:
                continue
            print("Claimed job", job_id)
            json_data = json.loads(response["request_json"])
            source = base64.b64decode(json_data["source"])
            job_dir = os.path.join(scratch_dir, str(f"job-{job_id}"))
            if not os.path.exists(job_dir):
                os.makedirs(job_dir)
            source_dir = bin_path(job_dir)
            with open(source_dir, "wb") as f:
                f.write(source)

            job_args = json_data["args"]

            execute_queue.put(ExecuteJob(job_id, job_dir, source_dir, job_args))
        except Exception as e:
            traceback.print_exc()
            continue

# def compile_worker(compile_queue, complete_queue, execute_queue):
#     while True:
#         compile_job: CompileJob = compile_queue.get()
#         put_fail = (
#             lambda log: complete_queue.put(
#                 CompleteJob(compile_job.job_id, compile_job.job_dir, False, log, None)
#             )
#         )
#         try:
#             os.makedirs(compile_job.job_dir, exist_ok=True)
#             with open(src_path(compile_job.job_dir), "w") as f:
#                 f.write(compile_job.source)
#             out = subprocess.run(
#                 [
#                     "nvcc",
#                     "-O3",
#                     "-use_fast_math",
#                     f"-arch=compute_{compute_capability}",
#                     f"-code=sm_{compute_capability}",
#                     "-o", bin_path(compile_job.job_dir),
#                     src_path(compile_job.job_dir),
#                 ],
#                 timeout=compile_timeout,
#                 stdout=subprocess.PIPE,
#                 stderr=subprocess.STDOUT,
#                 text=True,
#             )
#             if out.returncode == 0:
#                 execute_queue.put(ExecuteJob(compile_job.job_id, compile_job.job_dir, out.stdout))
#             else:
#                 put_fail(out.stdout)
#         except subprocess.TimeoutExpired:
#             put_fail(
#                 f"Compilation timed out after {compile_timeout} seconds. Output log:\n\n" + out.stdout
#             )
#         except Exception as e:
#             put_fail("Compilation failed with exception:\n" + str(e))

def execute_worker(execute_queue, complete_queue, gpu_index: int):
    while True:
        execute_job: ExecuteJob = execute_queue.get()
        put_complete = (
            lambda success, log: complete_queue.put(
                CompleteJob(execute_job.job_id, execute_job.job_dir, success, log)
            )
        )
        try:
            executable = bin_path(execute_job.job_dir)
            os.chmod(executable, 0o755)
            args = execute_job.job_args.split()
            out = subprocess.run(
                [executable] + args,
                timeout=execute_timeout,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=execute_job.job_dir,
            )
            if out.returncode == 0:
                put_complete(True, out.stdout)
            else:
                put_complete(False, out.stdout)
        except subprocess.TimeoutExpired:
            put_complete(
                False,
                f"Execution timed out after {execute_timeout} seconds. Output log:\n\n" + out.stdout,
            )
        except Exception as e:
            put_complete(False, "Execution failed with exception:\n" + str(e))

def truncate_text(text, max_length):
    if len(text) > max_length:
        return text[:max_length // 2] + "\n--- truncated... ---\n" + text[-max_length // 2:]
    else:
        return text

def complete_worker(complete_queue, auth):
    ssl_ctx = ssl.create_default_context(cadata=server_cert)

    auth_name = auth["executor"]
    auth_token = auth["token"]

    while True:
        completion: CompleteJob = complete_queue.get()
        try:
            shutil.rmtree(completion.job_dir, ignore_errors=True)

            completion_req_query = urllib.parse.urlencode({"executor": auth_name, "token": auth_token, "job_id": completion.job_id})

            if completion.execute_log is None:
                completion.execute_log = ""

            completion_data = json.dumps({
                "result_json": {
                    "success": completion.success,
                    # "compile_log": truncate_text(completion.compile_log, max_log_length),
                    "execute_log": truncate_text(completion.execute_log, max_log_length),
                }
            }).encode("utf-8")

            completion_req = urllib.request.Request(
                "https://" + server_ip_port + "/api/complete?" + completion_req_query,
                data=completion_data,
                method="POST",
            )
            with urllib.request.urlopen(completion_req, context=ssl_ctx) as f:
                pass
        except Exception as e:
            traceback.print_exc()
            continue

def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--nproc-compile", type=int, required=True)
    parser.add_argument("--nproc-execute", type=int, required=True)
    parser.add_argument(
        "--auth",
        help="Authentication token (defaults to ./auth.json in the same directory as this script)",
    )
    parser.add_argument(
        "--scratch-dir",
        help="Directory to store temporary files (default: /tmp)",
        default="/tmp",
    )
    args = parser.parse_args()

    token_path = args.auth or os.path.join(os.path.dirname(__file__), "auth.json")
    with open(token_path, "r") as f:
        auth = json.load(f)
    
    scratch_uuid = str(uuid.uuid4())
    scratch_dir = os.path.join(args.scratch_dir, f"executor-{scratch_uuid}")
    os.makedirs(scratch_dir, exist_ok=True)

    # compile_queue = multiprocessing.Queue(1)
    execute_queue = multiprocessing.Queue(1)
    complete_queue = multiprocessing.Queue(1)

    claim_proc = multiprocessing.Process(target=claim_worker, args=(execute_queue, auth, scratch_dir))
    claim_proc.start()

    # compile_procs = [
    #     multiprocessing.Process(target=compile_worker, args=(compile_queue, complete_queue, execute_queue))
    #     for _ in range(args.nproc_compile)
    # ]
    # for proc in compile_procs:
    #     proc.start()
    
    execute_procs = [
        multiprocessing.Process(target=execute_worker, args=(execute_queue, complete_queue, i))
        for i in range(args.nproc_execute)
    ]
    for proc in execute_procs:
        proc.start()
    
    complete_proc = multiprocessing.Process(target=complete_worker, args=(complete_queue, auth))
    complete_proc.start()

    claim_proc.join()


if __name__ == "__main__":
    main()