import requests
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')
mail = os.getenv('JQUANTS_MAIL')
password = os.getenv('JQUANTS_PASS')
print('mail:', mail)
r = requests.post('https://api.jquants.com/v1/token/auth_user', json={'mailaddress': mail, 'password': password})
print(r.status_code, r.text[:200])
