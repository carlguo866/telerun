import requests
import certifi

response = requests.get('https://54.227.136.40:4443', verify=certifi.where())
