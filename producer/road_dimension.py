import os
import requests

from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("TOMTOM_API_KEY")

URL = "https://api.tomtom.com/search/2/reverseGeocode"


ROAD_POINTS = [
    {
        "road_id": 1,
        "road_name": "Rodovia Presidente Dutra",
        "road_code": "BR-116",
        "latitude": -23.4698,
        "longitude": -46.5191
    },
    {
        "road_id": 2,
        "road_name": "Rodovia dos Bandeirantes",
        "road_code": "SP-348",
        "latitude": -23.4542,
        "longitude": -46.8495
    },
    {
        "road_id": 3,
        "road_name": "Rodovia Anhanguera",
        "road_code": "SP-330",
        "latitude": -23.4475,
        "longitude": -46.8618
    },
    {
        "road_id": 4,
        "road_name": "Rodovia Presidente Castello Branco",
        "road_code": "SP-280",
        "latitude": -23.5200,
        "longitude": -46.9450
    },
    {
        "road_id": 5,
        "road_name": "Rodovia Fernão Dias",
        "road_code": "BR-381",
        "latitude": -23.4080,
        "longitude": -46.5500
    }
]


def get_road_info(latitude, longitude):

    url = f"{URL}/{latitude},{longitude}.json"

    params = {
        "key": API_KEY
    }

    response = requests.get(url, params=params)

    response.raise_for_status()

    return response.json()


def main():

    roads = []

    for road in ROAD_POINTS:

        print(
            f"Consultando: {road['road_name']}"
        )

        data = get_road_info(
            road["latitude"],
            road["longitude"]
        )

        addresses = data.get("addresses", [])

        if not addresses:
            print("Nenhum endereço encontrado.\n")
            continue

        address = addresses[0].get("address", {})

        result = {
            "road_id": road["road_id"],
            "road_name": road["road_name"],
            "road_code": road["road_code"],
            "tomtom_name": address.get("streetName"),
            "latitude": road["latitude"],
            "longitude": road["longitude"],
            "municipality": address.get("municipality"),
            "country": address.get("country")
        }

        roads.append(result)

        print(f"Nome informado: {road['road_name']}")
        print(f"Nome retornado pela TomTom: {result['tomtom_name']}")
        print()


    print("\n===== DIM ROAD =====\n")

    for road in roads:

        print(
            f"{road['road_id']} | "
            f"{road['road_name']} | "
            f"{road['road_code']} | "
            f"{road['tomtom_name']} | "
            f"{road['latitude']} | "
            f"{road['longitude']}"
        )


if __name__ == "__main__":
    main()