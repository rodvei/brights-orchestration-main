import requests

url = "https://rawg-video-games-database.p.rapidapi.com/genres"

headers = {
	"X-RapidAPI-Key": "5159d08578msha1641dfe82c9f26p1bd992jsn45e198531a33",
	"X-RapidAPI-Host": "rawg-video-games-database.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers)

print(response.text)