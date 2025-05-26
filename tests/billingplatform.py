import requests


bp_session = requests.Session()

bp_session.headers.update({
    "username": "my.username",
    "password": "password",
})

r = bp_session.get('https://my.billingplatform.com/demogm/rest/2.0/login')

print(r.json())

r = requests.get('https://my.billingplatform.com/demogm/rest/2.0/ACCOUNTS')

print(r.json())