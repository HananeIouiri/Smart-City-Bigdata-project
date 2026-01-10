import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

# ================= CONFIGURATION =================
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9094"]
KAFKA_TOPIC = "traffic-events"

ZONES = [
    "Centre Ville",
    "Zone Industrielle",
    "Quartier Residentiel",
    "Aeroport"
]

ROAD_TYPES = {
    "Autoroute": {"max_speed": 130, "capacity": 100},
    "Avenue": {"max_speed": 60, "capacity": 50},
    "Rue": {"max_speed": 30, "capacity": 20}
}

ROADS = [
    {"id": "A1", "type": "Autoroute"},
    {"id": "A86", "type": "Autoroute"},
    {"id": "Av. des Champs", "type": "Avenue"},
    {"id": "Rue de la Republique", "type": "Rue"},
    {"id": "Bd de la Resistance", "type": "Avenue"}
]

# ================= CLASSE CAPTEUR =================
class TrafficSensor:
    def __init__(self, road_id, road_type, zone):
        self.sensor_id = str(uuid.uuid4())[:8]
        self.road_id = road_id
        self.road_type = road_type
        self.zone = zone
        self.max_speed = ROAD_TYPES[road_type]["max_speed"]
        self.capacity = ROAD_TYPES[road_type]["capacity"]

    def get_time_factor(self):
        hour = datetime.now().hour
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            return random.uniform(0.8, 1.0)
        elif 10 <= hour <= 16:
            return random.uniform(0.4, 0.7)
        else:
            return random.uniform(0.05, 0.3)

    def generate_event(self):
        traffic_load = self.get_time_factor()
        traffic_load += random.uniform(-0.1, 0.1)
        traffic_load = max(0, min(1, traffic_load))

        vehicle_count = int(self.capacity * traffic_load)
        occupancy_rate = round(traffic_load * 100, 2)

        if self.road_type == "Autoroute":
            speed = random.uniform(20, 50) if occupancy_rate >= 90 else \
                    random.uniform(50, 80) if occupancy_rate >= 70 else \
                    random.uniform(80, 140)
        elif self.road_type == "Avenue":
            speed = random.uniform(5, 25) if occupancy_rate >= 90 else \
                    random.uniform(20, 40) if occupancy_rate >= 70 else \
                    random.uniform(40, 60)
        else:
            speed = random.uniform(2, 10) if occupancy_rate >= 90 else \
                    random.uniform(10, 20) if occupancy_rate >= 70 else \
                    random.uniform(20, 30)

        event = {
            "sensor_id": self.sensor_id,
            "road_id": self.road_id,
            "road_type": self.road_type,
            "zone": self.zone,
            "vehicle_count": vehicle_count,
            "average_speed": round(speed, 1),
            "occupancy_rate": occupancy_rate,
            "event_time": datetime.now().isoformat()
        }

        return event

# ================= MAIN =================
def main():
    print("🚀 Initialisation du Producer Kafka Smart City...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("✅ Connexion Kafka établie")

    sensors = []
    for road in ROADS:
        zone = random.choice(ZONES)
        sensor = TrafficSensor(road["id"], road["type"], zone)
        sensors.append(sensor)
        print(f"📡 Capteur {sensor.sensor_id} installé sur {road['id']} ({zone})")

    print("\n📊 Simulation du trafic en cours... CTRL+C pour arrêter\n")

    try:
        while True:
            for sensor in sensors:
                event = sensor.generate_event()

                # Statut trafic visuel
                if event["occupancy_rate"] > 80:
                    status = "🔴 Congestion"
                elif event["occupancy_rate"] > 50:
                    status = "🟠 Dense"
                else:
                    status = "🟢 Fluide"

                producer.send(KAFKA_TOPIC, event)

                print(
                    f"🚦 EVENT | {status} | "
                    f"🆔 {event['sensor_id']} | "
                    f"🛣 {event['road_id']} ({event['road_type']}) | "
                    f"📍 {event['zone']} | "
                    f"🚗 {event['vehicle_count']} | "
                    f"⚡ {event['average_speed']} km/h | "
                    f"📊 {event['occupancy_rate']}% | "
                    f"⏰ {event['event_time']}"
                )

            producer.flush()
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n🛑 Simulation arrêtée par l'utilisateur.")
        producer.close()

# ================= LANCEMENT =================
if __name__ == "__main__":
    main()
