from urllib import response
import requests
import os
import random
import json
from dotenv import load_dotenv

import re
import joblib


load_dotenv()

def get_cached_data(func, *args, **kwargs):
    # Replace special characters with underscores using regex
    func_str = re.sub(r"[^0-9a-zA-Z]+", "_", str(func.__name__))
    arg_str = re.sub(r"[^0-9a-zA-Z]+", "_", str(args))
    kwarg_str = re.sub(r"[^0-9a-zA-Z]+", "_", str(kwargs))
    cache_file = f"cached/{func_str}_{arg_str}_{kwarg_str}.joblib"
    if not os.path.exists("cached"):
        os.makedirs("cached")
    data = None
    try:
        print("Reading from cache")
        data = joblib.load(cache_file)
    except Exception as e:
        print("Cache miss", e)
        data = func(*args, **kwargs)
        if data is not None:
            joblib.dump(data, cache_file)
    return data


def getProxyList():
    if os.getenv("PROXY_USER2") == None:
        try:
            proxies = requests.get(
                f"https://actproxy.com/proxy-api/${os.environ['AIRFLOW_VAR_ACT_PROXY_API']}?format=json"
            )

            proxyIps = json.loads(proxies.text)
            proxyList = list(
                map(
                    (lambda x: "http://" + os.environ["AIRFLOW_VAR_PROXY_USER"] + "@" + x), proxyIps
                )
            )
        except Exception as e:
            raise (
                Exception(
                    "Please set the ACT_PROXY_API environment variable and connect to the internet"
                )
            )
    return proxyList


def getCachedProxyList():
    return get_cached_data(getProxyList)


def getProxy():
    # proxy = {"https": random.choice(getCachedProxyList())}
    proxy = {"https": os.environ["AIRFLOW_VAR_PROXY"]}
    return proxy