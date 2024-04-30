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
MIIFtDCCA5ygAwIBAgIUXVML2juXWBLrghWPuEMqenRBXWowDQYJKoZIhvcNAQEL
BQAwYTELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAk1BMRIwEAYDVQQHDAlDYW1icmlk
Z2UxDDAKBgNVBAoMA01JVDEjMCEGA1UEAwwaNjEwNi10ZWxlcnVuLmNzYWlsLm1p
dC5lZHUwHhcNMjQwNDIzMTg0MDE3WhcNMjUwNDIzMTg0MDE3WjBhMQswCQYDVQQG
EwJVUzELMAkGA1UECAwCTUExEjAQBgNVBAcMCUNhbWJyaWRnZTEMMAoGA1UECgwD
TUlUMSMwIQYDVQQDDBo2MTA2LXRlbGVydW4uY3NhaWwubWl0LmVkdTCCAiIwDQYJ
KoZIhvcNAQEBBQADggIPADCCAgoCggIBANGVK25ZIl6mCs8tyEQntYD5vEGrgB9m
F3E30wjjxjkQ5cjzUmQrtmkkGQGiAtw3cznF3L7oTZvyTIOdsT9NKpVkiHOaO6pl
CLxRAFhUHJKzl6RrsQmCw2GTqIvXDiXFygqR9jYZF1FN9fCEJ7hpHfzT/wmrsPv1
N7m+1PCvFLbKGHFGS460NBdjRk5W2+cCsxfntIhBxnMXnABVZ/L4v75B/wfu+SFT
ChdgRXFnUaKcfBnErfmxVi6HmiAl0cU/ia2+bzaXKjsbkZDHMnlLM+jBwOduv/ST
a2QDgRLyTcnk/09kbLJkvqIuOrDKIMVjO8oBwnpwqZzBLfo0lG7scT1+Iw4vvXnT
WPKfPBoYdwMRzkyKdnVaWHP2se49nfcKZSfkIGl3xsBkgAWIjL2ELj6ZkvHxspF3
ZnjTsfmKfZCqU7OaGI4amzXxcdN3ohHkHD81yZJxl/86wUM1y7GvopYmOgQxRwx6
GI+RZOtsJzyuKaRE6DcmxL6xoXlxVYvzqwQkgsLXu/EKkesNCkfRcdOjQ352i+F1
GEW5fxwd0B130cNjnhtdhhGWuqTTR0j09Rnb4kD2VJ2C8g7QMc5eGgETDSLNDU0r
jlabU+IAv0hnVrO8ErgciHBNm0zFN6EhnFxSFs/3lYVlLB3yyF2ReDo8DqVHVr7X
WPOx6nrKSCATAgMBAAGjZDBiMB0GA1UdDgQWBBQDBhkWizBIhOPwuyRzF4GU1w3c
UzAfBgNVHSMEGDAWgBQDBhkWizBIhOPwuyRzF4GU1w3cUzAPBgNVHRMBAf8EBTAD
AQH/MA8GA1UdEQQIMAaHBIA0hLcwDQYJKoZIhvcNAQELBQADggIBAAMgNfJP7irm
LYWbsmIBYCAM6Bmw2jMuDf8wBalQyjrpAcOqAVk/QV/QlB9itUEVDGNfChLK7ARs
2aovDHJya9yLPODiqXJxdyYthktu8k7Kqz9V6gpsVlMLWQaZENlwnAas0SCPJFGa
rYqHal5fQgVa3k+POgA/FCYrKXPEVXe5mAFKJV8yUUry+1klLu0QVk/XANvkO8bQ
ZkbOrxlwloRLYo7IGYL3vLLYWMWu6KFdLdqltKIh3KITP46N3Cbft6BthFCRgX5D
k4nvGbI4USKv4to9hA9/OgpFh3nAOiCSLuurU90oZQOIbvocn93BuZzrhOpqrZd4
xu/cjxwUfYwtcYIL/2UA6d4tjjWvYOA/zHMRjgPzdkZTp0DC7uFO/zkvaQSXksUr
zs3w77l+3zv220oLm69mYIjdKVfeLi3DsMUfMAgRkSAzY565Qnxc0hT5T8eKqMv6
IAvolkIkKFMKjchMjLtRI1Ytl12ayDiEC0bQkx2UXmRiGSmLpp0W73cwQnllSsqf
TsrtD21bNVOt+CjvviRaNXzRB/HtXslxAAWQ5lOnPLG1mQ+s/uraF8NyZA8pipej
xg49pg/UeGV9BQpyt46Wlsyi3O1+pxNARQ9U9eo7PxrQ35Yu+dyjRWEEZH8F1H06
AFsopRSOsHcRT1hgqq8o/lR3hMKtgAt2
-----END CERTIFICATE-----
"""

server_ip_port = "6106-telerun.csail.mit.edu:4443"

poll_interval = 0.25 # seconds

execute_timeout = 60 # seconds

max_log_length = 1 << 20

compute_capability = "86"

@dataclass
class ExecuteJob:
    job_id: int
    job_dir: str
    source_path: str
    job_args: str

@dataclass
class CompleteJob:
    job_id: int
    job_dir: str
    success: bool
    execute_log: Optional[str] = None

def src_path(job_dir: str) -> str:
    return os.path.join(job_dir, "execute")

def bin_path(job_dir: str) -> str:
    return os.path.join(job_dir, "bin")

def execute_cmd(job_path: str, args: str, cpu_num: int ) -> str: 
    return f"taskset -c {cpu_num}-{cpu_num} numactl -i all {job_path} {args}"


def get_job(auth_name, auth_token, ssl_ctx):
    url_query = urllib.parse.urlencode({"executor": auth_name, "token": auth_token})

    req = urllib.request.Request(
        "https://" + server_ip_port + "/api/claim?" + url_query,
        method="POST",
    )
    with urllib.request.urlopen(req, context=ssl_ctx) as f:
        response = json.load(f)
    assert response["success"]
    return response

def load_job_data(response, job_id, scratch_dir: str):
    json_data = json.loads(response["request_json"])
    source = base64.b64decode(json_data["source"])
    job_dir = os.path.join(scratch_dir, str(f"job-{job_id}"))
    os.makedirs(job_dir, exist_ok=True)
    source_path = bin_path(job_dir)
    with open(source_path, "wb") as f:
        f.write(source)

    return job_dir, source_path, json_data["args"]

def main_runner(auth, scratch_dir: str):
    ssl_ctx = ssl.create_default_context(cadata=server_cert)
    
    auth_name = auth["executor"]
    auth_token = auth["token"]
    
    while True:
        time.sleep(poll_interval)
        # if execute_queue.empty():
        try:
            response = get_job(auth_name, auth_token, ssl_ctx)
            job_id = response["job_id"]
            if job_id is None:
                continue
            print("Claimed job", job_id)
            
            job_dir, source_path, job_args = load_job_data(response, job_id, scratch_dir)

            complete_job = execute_worker(ExecuteJob(job_id, job_dir, source_path, job_args), 1)
            complete_worker(complete_job, auth)
        except Exception as e:
            traceback.print_exc()
            continue

def execute_worker(execute_job: ExecuteJob, cpu_index: int):
    def put_complete(success, log):
        return CompleteJob(execute_job.job_id, execute_job.job_dir, success, log)

    try:   
        os.chmod(execute_job.source_path, 0o755)
        args = execute_cmd(execute_job.source_path, execute_job.job_args, cpu_index).split()
        out = subprocess.run(
            args,
            timeout=execute_timeout,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=execute_job.job_dir,
        )
        if out.returncode == 0:
            return put_complete(True, out.stdout)
        else:
            return put_complete(False, out.stdout)
    except subprocess.TimeoutExpired:
        return put_complete(
            False,
            f"Execution timed out after {execute_timeout} seconds. Output log:\n\n" + out.stdout,
        )
    except Exception as e:
        return put_complete(False, "Execution failed with exception:\n" + str(e))

def truncate_text(text, max_length):
    if len(text) > max_length:
        return text[:max_length // 2] + "\n--- truncated... ---\n" + text[-max_length // 2:]
    else:
        return text

def complete_worker(completion: CompleteJob, auth):
    ssl_ctx = ssl.create_default_context(cadata=server_cert)

    auth_name = auth["executor"]
    auth_token = auth["token"]


    try:
        shutil.rmtree(completion.job_dir, ignore_errors=True)

        completion_req_query = urllib.parse.urlencode({"executor": auth_name, "token": auth_token, "job_id": completion.job_id})

        if completion.execute_log is None:
            completion.execute_log = ""

        completion_data = json.dumps({
            "result_json": {
                "success": completion.success,
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


def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--nproc-execute", type=int, required=True)
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

    # execute_queue = multiprocessing.Queue(1)
    # complete_queue = multiprocessing.Queue(1)

    claim_proc = multiprocessing.Process(target=main_runner, args=(auth, scratch_dir))
    claim_proc.start()
    
    # execute_procs = [
    #     multiprocessing.Process(target=execute_worker, args=(execute_queue, complete_queue, i))
    #     for i in range(args.nproc_execute)
    # ]
    # for proc in execute_procs:
    #     proc.start()
    
    # complete_proc = multiprocessing.Process(target=complete_worker, args=(complete_queue, auth))
    # complete_proc.start()

    claim_proc.join()


if __name__ == "__main__":
    main()