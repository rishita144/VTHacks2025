import requests


zip_code = 
url = "https://us-zip-code-to-income.p.rapidapi.com/"
querystring = {"zip": "24060"}  # example ZIP code: Blacksburg

headers = {
    "x-rapidapi-host": "us-zip-code-to-income.p.rapidapi.com",
    "x-rapidapi-key": "b09512ba34msh3255fd24a9cd3f6p151b94jsna691fc9e1c28"
}

response = requests.get(url, headers=headers, params=querystring)

if response.status_code == 200:
    data = response.json()
    print("Income Data for ZIP 24060:")
    print(data)
else:
    print("Error:", response.status_code, response.text)