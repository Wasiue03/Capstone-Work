import requests

response = requests.get('https://randomuser.me/api')

print(response.status_code)

data = response.json()

print(data)


gender = response.json()['results'][0]['gender']

print(gender)


name = response.json()['results'][0]['name']
print(name)