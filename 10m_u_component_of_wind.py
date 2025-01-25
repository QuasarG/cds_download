import cdsapi

dataset = "reanalysis-era5-land"
request = {
    "variable": ["10m_u_component_of_wind"],
    "year": "1950",
    "month": "01",
    "day": ["01"],
    "time": ["01:00", "02:00"],
    "data_format": "grib",
    "download_format": "zip"
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()
