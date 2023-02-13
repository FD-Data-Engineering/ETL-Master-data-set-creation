import requests
import json
from airflow.models import Variable

api_url = "https://iam.cloud.ibm.com/identity/token"
dt = "grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=CcZzswuM7FSjGEJ_fdmnL47OwD401XWpqfwC5jYtBnV8"
headers = {"Content-Type": "application/x-www-form-urlencoded"}

def set_variable(api_url, dt, headers)
    response = requests.post(api_url, data=dt, headers=headers)
    tokenJson = response.json()
    return(tokenJson)

Token_JSON = set_variable(api_url, dt, headers)
Access_Token = Token_JSON["access_token"]
Expiration = Token_JSON["expiration"]

Variable.set("Access_Token", Access_Token)
Variable.set("Expiration", Expiration)

print("Variable set is completed")