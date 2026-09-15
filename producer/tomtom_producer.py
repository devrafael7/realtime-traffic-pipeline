import os
import json
import time
import requests

from dotenv import load_dotenv
from confluent_kafka import Producer


load_dotenv()

API_KEY = os.getenv("TOMTOM_API_KEY")

TOMTOM_URL = (
    "https://api.tomtom.com/traffic/services/4/"
    "flowSegmentData/absolute/10/json"
)

KAFKA_SERVER = "13.223.234.68:9092"
KAFKA_TOPIC = "traffic-data"


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


producer = Producer({
    "bootstrap.servers": KAFKA_SERVER
})


def get_traffic_data(road):

    params = {
        "point": f"{road['latitude']},{road['longitude']}",
        "unit": "KMPH",
        "key": API_KEY
    }

    response = requests.get(
        TOMTOM_URL,
        params=params
    )

    response.raise_for_status()

    return response.json()


def delivery_report(error, message):

    if error is not None:

        print(
            f"Erro ao enviar para Kafka: {error}"
        )

    else:

        print(
            f"Enviado para Kafka → "
            f"{message.topic()} | "
            f"road_id={message.key().decode()}"
        )


def send_to_kafka(road, data):

    traffic = data["flowSegmentData"]

    event = {
        "road_id": road["road_id"],
        "road_name": road["road_name"],
        "road_code": road["road_code"],
        "latitude": road["latitude"],
        "longitude": road["longitude"],
        "current_speed": traffic["currentSpeed"],
        "free_flow_speed": traffic["freeFlowSpeed"],
        "current_travel_time": traffic["currentTravelTime"],
        "free_flow_travel_time": traffic["freeFlowTravelTime"],
        "confidence": traffic["confidence"],
        "road_closure": traffic["roadClosure"],
        "timestamp": time.time()
    }

    producer.produce(
        topic=KAFKA_TOPIC,
        key=str(road["road_id"]),
        value=json.dumps(event),
        callback=delivery_report
    )

    producer.poll(0)


def main():

    print("TrafficPulse Kafka Producer iniciado...\n")

    while True:

        for road in ROAD_POINTS:

            try:

                print(
                    f"Consultando TomTom → "
                    f"{road['road_name']}"
                )

                data = get_traffic_data(road)

                send_to_kafka(
                    road,
                    data
                )

            except Exception as error:

                print(
                    f"Erro em {road['road_name']}: "
                    f"{error}"
                )

        # Garante que as mensagens pendentes
        # sejam entregues ao Kafka
        producer.flush()

        print(
            "\nAguardando 30 segundos...\n"
        )

        time.sleep(30)


if __name__ == "__main__":
    main()