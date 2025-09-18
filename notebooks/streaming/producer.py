from azure.eventhub import EventHubProducerClient, EventData
import json, time, random

# Usa el connection string que copiaste del portal
# connection_str = "Endpoint=sb://<tu-namespace>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXXXX"
# eventhub_name = "<tu-eventhub>"  # El nombre que diste al crear el Event Hub

producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)

for i in range(10):  # Envía 10 eventos de prueba
    event = {"device": "sensor1", "temp": random.randint(20, 35)}
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)
    print("Evento enviado:", event)
    time.sleep(1)
