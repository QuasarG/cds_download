import requests

response = requests.request(
    "POST",
    "https://datasets.wri.org/api/3/action/resource_patch",
    data={
            "id": "67b08651-b6a9-4765-be71-ebca1692c5f7",
            "title": "Global Power Plant DatabaseEdited",
            "description": "Global power plants by capacity in megawatts and fuel type. Includes coal, oil, gas, hydro, nuclear, solar, waste, wind, geothermal, and biomass. Edited"
        }, headers={
			"Content-Type": "application/json",
			"Authorization": "<API_TOKEN>"
		}
)

data = response.json()

print(data)
