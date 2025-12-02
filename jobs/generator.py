# jobs/generator.py
import json, time, random, uuid, os
from datetime import datetime

OUT_DIR = "/host_incoming"   # смонтированная папка
if not os.path.exists(OUT_DIR):
    os.makedirs(OUT_DIR)

users = [f"user_{i}" for i in range(1, 51)]
products = [f"prod_{i}" for i in range(1, 101)]
events = ["view", "click", "add_to_cart", "purchase"]

def make_event():
    ts = int(time.time() * 1000)
    return {
        "user_id": random.choice(users),
        "session_id": str(uuid.uuid4()),
        "event_type": random.choices(events, weights=[0.7,0.15,0.1,0.05])[0],
        "product_id": random.choice(products),
        "price": round(random.uniform(5,200),2),
        "timestamp": ts,
        "page": f"/product/{random.randint(1,100)}",
        "category": random.choice(["electronics","books","home","fashion"])
    }

def write_file(batch_size=50):
    rows = [make_event() for _ in range(batch_size)]
    filename = f"clicks_{int(time.time())}_{random.randint(0,9999)}.json"
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print("Wrote", path)

if __name__ == "__main__":
    # пример: цикл, создаёт файл каждые N секунд
    for _ in range(20):
        write_file(100)
        time.sleep(3)
