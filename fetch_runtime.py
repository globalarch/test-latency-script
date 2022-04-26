import requests
import json
import time
import numpy as np
import pandas as pd
import sys

def fetch_runtime_all(kwargs):
    # runtime_manager -> pipeline_rt_db -> mongo
    # list -> search -> find
    host = kwargs["host"]
    org_id = kwargs["org_id"]
    token = kwargs["token"]
    endpoint = f'{host}/api/2/{org_id}/rest/pm/runtime'
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json; charset=UTF-8',
    }
    data = {
        "state": "Completed,Stopped,Failed",
        "offset": 0,
        "limit": 0,
    }
    response = requests.post(endpoint, headers=headers, data=json.dumps(data))
    runtime_list = response.json()["response_map"]["entries"]
    print('fetch_runtime_all',response.status_code)
    return runtime_list

def fetch_runtime_all_globalarch(kwargs):
    # runtime_manager -> pipeline_rt_db -> mongo
    # list -> search -> find
    host = kwargs["host"]
    org_id = kwargs["org_id"]
    token = kwargs["token"]
    endpoint = f'{host}/api/2/{org_id}/rest/pm/runtime/globalarch'
    headers = {
        'Content-Type': 'application/json; charset=UTF-8',
    }
    data = {
        "state": "Completed,Stopped,Failed",
        "offset": 0,
        "limit": 0,
        "is_globalarch": True,
    }
    response = requests.post(endpoint, headers=headers, data=json.dumps(data))
    print('fetch_runtime_all_globalarch',response.status_code)

def fetch_runtime_all_unauthorize(kwargs):
    # runtime_manager -> pipeline_rt_db -> mongo
    # list -> search -> find
    host = kwargs["host"]
    org_id = kwargs["org_id"]
    token = kwargs["token"]
    endpoint = f'{host}/api/2/{org_id}/rest/pm/runtime/globalarch'
    headers = {
        'Content-Type': 'application/json; charset=UTF-8',
    }
    data = {
        "state": "Completed,Stopped,Failed",
        "offset": 0,
        "limit": 0
    }
    response = requests.post(endpoint, headers=headers, data=json.dumps(data))
    print('fetch_runtime_all_globalarch',response.status_code)

def fetch_runtime_one(kwargs):
    # runtime_manager -> pipeline_rt_db -> mongo
    # get_runtime -> fetch_one -> find_one
    host = kwargs["host"]
    org_id = kwargs["org_id"]
    token = kwargs["token"]
    ruuid = kwargs["ruuid"]
    endpoint = f'{host}/api/2/{org_id}/rest/pm/runtime/{ruuid}'
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json; charset=UTF-8',
    }
    params = { "level": "details" }
    response = requests.get(endpoint, headers=headers, params=params)
    print('fetch_runtime_one',response.status_code)

def export_runtime(kwargs):
    # same as fetch one but return zip file
    # runtime_manager -> pipeline_rt_db -> mongo
    # list -> search -> find
    host = kwargs["host"]
    org_id = kwargs["org_id"]
    token = kwargs["token"]
    ruuid = kwargs["ruuid"]
    endpoint = f'{host}/api/2/{org_id}/rest/pm/runtime/export/{ruuid}'
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json; charset=UTF-8',
    }
    params = { "level": "details" }
    response = requests.get(endpoint, headers=headers, params=params)
    print('export_runtime',response.status_code)

def get_health_summary(kwargs):
    # runtime_manager -> pipeline_rt_db -> mongo
    # list_runtimes -> list -> find
    host = kwargs["host"]
    org_id = kwargs["org_id"]
    token = kwargs["token"]
    endpoint = f'{host}/api/2/{org_id}/rest/pm/runtime/health_summary'
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json; charset=UTF-8',
    }
    end_ts = int(round(time.time() * 1000))
    start_ts = int(round(time.time() * 1000)) - 3600000
    params = { "start_ts": start_ts, "end_ts": end_ts }
    response = requests.get(endpoint, headers=headers, params=params)
    print('get_health_summary',response.status_code)

def get_health_summary_for_pipe(kwargs):
    # runtime_manager -> runtime_manager -> pipeline_rt_db -> mongo
    # get_pipe_runtimes -> list_runtimes -> list -> find
    host = kwargs["host"]
    org_id = kwargs["org_id"]
    token = kwargs["token"]
    pipe_fqid = kwargs["pipe_fqid"]
    endpoint = f'{host}/api/2/{org_id}/rest/pm/runtime/health_summary/{pipe_fqid}'
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json; charset=UTF-8',
    }
    end_ts = int(round(time.time() * 1000))
    start_ts = int(round(time.time() * 1000)) - 3600000
    params = { "start_ts": start_ts, "end_ts": end_ts }
    response = requests.get(endpoint, headers=headers, params=params)
    print('get_health_summary_for_pipe',response.status_code)

def get_runtime_stats(kwargs):
    # runtime_manager -> pipeline_rt_db -> mongo
    # get_runtime_stats -> list -> find
    host = kwargs["host"]
    org_id = kwargs["org_id"]
    token = kwargs["token"]
    endpoint = f'{host}/api/2/{org_id}/rest/pm/runtime/stats'
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json; charset=UTF-8',
    }
    end_ts = int(round(time.time() * 1000))
    start_ts = int(round(time.time() * 1000)) - 3600000
    params = { "start_ts": start_ts, "end_ts": end_ts }
    response = requests.get(endpoint, headers=headers, params=params)
    print('get_runtime_stats',response.status_code)

def runtime_list_queued(kwargs):
    ### List the queued and unclaimed runtime IDs for a given Snaplex.
    # runtime_manager -> pipeline_rt_db -> mongo
    # list_queued -> list -> find
    host = kwargs["host"]
    org_id = kwargs["org_id"]
    token = kwargs["token"]
    endpoint = f'{host}/api/2/{org_id}/rest/pm/runtime/list_queued'
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json; charset=UTF-8',
    }
    runtime_path_id = 'oregon/rt/orgplex/dev' if org_name == 'oregon' else 'singapore/rt/sgpplex/dev'
    params = { "level": "details", "runtime_path_id": runtime_path_id }
    response = requests.get(endpoint, headers=headers, params=params)
    print('runtime_list_queued',response.status_code)

def login(host:str, username: str, password: str):
    path = '/api/1/rest/asset/session'
    params = {"caller": username}
    r = requests.get(host + path, params=params, auth=(username,password))
    return 'SLToken ' + r.json()["response_map"]["token"]

def get_orgsnid(host: str, username: str, token: str, org_name: str):
    path = '/api/1/rest/asset/user/{}'.format(username)
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json; charset=UTF-8',
    }
    r = requests.get(host + path, headers=headers)
    data = r.json()
    org_snodes = data["response_map"]["org_snodes"]
    for snode in org_snodes:
        if org_snodes[snode]["name"] == org_name: return snode

def evaluate(func, **kwargs):
    for i in range(N):
        result = []
        result.append(kwargs["endpoint"])
        result.append(kwargs["backend_server_location"])
        result.append(kwargs["organization_data"])
        start = time.perf_counter()
        func(kwargs)
        # milliseconds
        result.append(time.perf_counter() - start)
        data.append(result)   

def get_status(host: str):
    endpoint = f'{host}/status'
    response = requests.get(endpoint)
    print(response.json())

N = 10

username = 'admin@snaplogic.com'
password = 'Ephemeral$123'
host = sys.argv[1]
cross_region_host = sys.argv[2]
org_name = sys.argv[3]
if org_name == 'oregon': cross_org_name = 'singapore'
if org_name == 'singapore': cross_org_name = 'oregon'
token = login(host, username, password)
org_id = get_orgsnid(host, username, token, org_name)
data = []
columns = ['endpoint','backend server location', 'organization data', 'time (secs)']

print('backend server:', org_name)
token = login(host, username, password)
runtime_list = fetch_runtime_all({"host": host, "org_id":org_id, "token":token})
evaluate(fetch_runtime_all, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime')
evaluate(fetch_runtime_all_globalarch, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/globalarch')
evaluate(fetch_runtime_all_unauthorize, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/unauthorize')
evaluate(fetch_runtime_one, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/<ruuid>', ruuid=runtime_list[1]["instance_id"])
evaluate(get_health_summary, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/health_summary')
evaluate(get_health_summary_for_pipe, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/health_summary/<pipe_fqid>', pipe_fqid=runtime_list[1]["class_id"])
evaluate(get_runtime_stats, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/stats')
evaluate(runtime_list_queued, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/list_queued')

print('backend server:', cross_org_name)
token = login(cross_region_host, username, password)
runtime_list = fetch_runtime_all({"host": cross_region_host, "org_id":org_id, "token":token})
evaluate(fetch_runtime_all, host=cross_region_host, org_id=org_id, token=token, backend_server_location=cross_org_name, organization_data=org_name, endpoint='/runtime')
evaluate(fetch_runtime_all_globalarch, host=cross_region_host, org_id=org_id, token=token, backend_server_location=cross_org_name, organization_data=org_name, endpoint='/runtime/globalarch')
evaluate(fetch_runtime_all_unauthorize, host=cross_region_host, org_id=org_id, token=token, backend_server_location=cross_org_name, organization_data=org_name, endpoint='/runtime/unauthorize')
evaluate(fetch_runtime_one, host=cross_region_host, org_id=org_id, token=token, backend_server_location=cross_org_name, organization_data=org_name, endpoint='/runtime/<ruuid>', ruuid=runtime_list[1]["instance_id"])
evaluate(get_health_summary, host=cross_region_host, org_id=org_id, token=token, backend_server_location=cross_org_name, organization_data=org_name, endpoint='/runtime/health_summary')
evaluate(get_health_summary_for_pipe, host=cross_region_host, org_id=org_id, token=token, backend_server_location=cross_org_name, organization_data=org_name, endpoint='/runtime/health_summary/<pipe_fqid>', pipe_fqid=runtime_list[1]["class_id"])
evaluate(get_runtime_stats, host=cross_region_host, org_id=org_id, token=token, backend_server_location=cross_org_name, organization_data=org_name, endpoint='/runtime/stats')
evaluate(runtime_list_queued, host=cross_region_host, org_id=org_id, token=token, backend_server_location=cross_org_name, organization_data=org_name, endpoint='/runtime/list_queued')

df = pd.DataFrame(data=data, columns=columns)
df.to_csv('result_fetch.csv')

# username = 'admin@snaplogic.com'
# password = 'Adm1n@12z0'
# host = 'http://localhost:8888'
# token = login(host, username, password)
# org_name = 'snaplogic'
# org_id = get_orgsnid(host, username, token, org_name)
# runtime_list = fetch_runtime_all({"host": host, "org_id":org_id, "token":token})
# data = []
# columns = ['endpoint','backend server location', 'organization data', 'time (secs)']

# evaluate(fetch_runtime_all, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime')
# evaluate(fetch_runtime_all_globalarch, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/globalarch')
# evaluate(fetch_runtime_one, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/<ruuid>', ruuid=runtime_list[1]["instance_id"])
# evaluate(get_health_summary, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/health_summary')
# evaluate(get_health_summary_for_pipe, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/health_summary/<pipe_fqid>', pipe_fqid=runtime_list[1]["class_id"])
# evaluate(get_runtime_stats, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/stats')
# evaluate(runtime_list_queued, host=host, org_id=org_id, token=token, backend_server_location=org_name, organization_data=org_name, endpoint='/runtime/list_queued')

# df = pd.DataFrame(data=data, columns=columns)
# df.to_csv('result_fetch.csv')