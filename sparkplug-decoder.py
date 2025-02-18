import paho.mqtt.client as mqtt
from core.sparkplug_b_pb2 import Payload
import base64
import time

# Configuración del broker MQTT
BROKER = "localhost"
PORT = 1883
TOPIC = "spBv1.0/#"  # Escuchar todos los tópicos Sparkplug B

# Función de callback para cuando se recibe un mensaje MQTT
def on_message(client, userdata, message):
    print(f"Mensaje recibido en {message.topic}")
    try:
        payload = Payload()
        payload.ParseFromString(message.payload)
        print("--- Datos recibidos ---")
        for metric in payload.metrics:
            print(f"Métrica: {metric.name}, Tipo: {metric.datatype}, Valor: {get_value(metric)}")
        print("-----------------------\n")
    except Exception as e:
        print(f"Error al procesar mensaje: {str(e)}")

# Función para obtener valores de métricas
def get_value(metric):
    if metric.datatype == 10:  # Double
        return metric.double_value
    elif metric.datatype == 11:  # Float
        return metric.float_value
    elif metric.datatype in [7, 8]:  # Int32/Int64
        return metric.int_value
    elif metric.datatype == 12:  # Boolean
        return metric.boolean_value
    elif metric.datatype == 13:  # String
        return metric.string_value
    elif metric.datatype == 16:  # DateTime
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(metric.long_value / 1000))
    return "Tipo desconocido"

# Configuración y conexión al broker
client = mqtt.Client()
client.on_message = on_message
client.connect(BROKER, PORT, 60)
client.subscribe(TOPIC)

print("Escuchando mensajes MQTT...")
client.loop_forever()
