import requests
API_KEY = '614d45c57c1537311a4a3d53121b0492'
url = f"http://api.nessieisreal.com/branches?key={API_KEY}"
#url = f"http://api.nessieisreal.com/accounts"
response = requests.get(url)
data = response.json()
if response.status_code == 200:
    print("Branches table:")
    print(data)
else:
    print("Error:", response.status_code, response.text)
    #http://api.nessieisreal.com/enterprise/customers?key=614d45c57c1537311a4a3d53121b0492

