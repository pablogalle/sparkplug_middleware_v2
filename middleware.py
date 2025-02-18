import time
import datetime
from opcua import Client, ua
import paho.mqtt.client as mqtt
from core.sparkplug_b_pb2 import Payload
from opcua.common.subscription import SubHandler

# Configuración mejorada
OPC_UA_SERVER = "opc.tcp://localhost:4840/freeopcua/server/"
BROKER = "localhost"
PORT = 1883
GROUP_ID = "Production"
SCAN_INTERVAL = 10  # Segundos entre actualizaciones

# Mapeo manual de tipos de datos OPC UA a Sparkplug B
DATA_TYPE_MAP = {
    "Double": 10,    # DataType.Double
    "Float": 11,     # DataType.Float
    "Int32": 7,      # DataType.Int32
    "Int64": 8,      # DataType.Int64
    "Boolean": 12,   # DataType.Boolean
    "String": 13,    # DataType.String
    "DateTime": 16,  # DataType.DateTime
    "Byte": 2,       # DataType.Byte
    "UInt32": 9      # DataType.UInt32
}

class SparkplugConverter(SubHandler):
    def __init__(self):
        # -- MQTT y OPC UA --
        self.mqtt_client = mqtt.Client()
        self.opcua_client = Client(OPC_UA_SERVER)
        
        # Diccionario principal: { 
        #   "Device_1": {
        #       "metrics": { node_id: {...}, ... },
        #       "seq": 0  # Secuencia Sparkplug independiente
        #   }, 
        #   "Device_2": {...},
        #   ...
        # }
        self.devices = {}

        self.subscription = None
        self.subscription_handles = []

        # Conectar al broker MQTT
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        try:
            self.mqtt_client.connect(BROKER, PORT)
            self.mqtt_client.loop_start()  # Iniciar loop en background
            print(f"Cliente MQTT iniciado - Broker: {BROKER}:{PORT}")
        except Exception as e:
            print(f"Error conectando al broker MQTT: {str(e)}")

    # ---------------------------------------------------
    #   Callbacks MQTT
    # ---------------------------------------------------
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        print(f"Conectado al broker MQTT con código: {rc}")
        if rc != 0:
            print(f"Error conectando al broker MQTT. Código: {rc}")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        print(f"Desconectado del broker MQTT con código: {rc}")

    # ---------------------------------------------------
    #   Descubrimiento y Procesamiento de Nodos
    # ---------------------------------------------------
    def _map_data_type(self, ua_type):
        """Mapea tipos de OPC UA a tipos Sparkplug B"""
        type_mapping = {
            ua.VariantType.Double: 10,
            ua.VariantType.Float: 11,
            ua.VariantType.Int32: 7,
            ua.VariantType.Int64: 8,
            ua.VariantType.Boolean: 12,
            ua.VariantType.String: 13,
            ua.VariantType.DateTime: 16,
            ua.VariantType.Byte: 2,
            ua.VariantType.UInt32: 9
        }
        return type_mapping.get(ua_type, 0)

    def _process_device_node(self, device_name, device_node):
        """
        Procesa un nodo de tipo "Device_X" y registra sus variables en self.devices.
        """
        # Crear la entrada para este dispositivo si no existe
        if device_name not in self.devices:
            self.devices[device_name] = {
                "metrics": {},
                "seq": 0  # Secuencia independiente para cada dispositivo
            }
            print(f"🔧 Registrado nuevo dispositivo: {device_name}")

        # Obtener los hijos del dispositivo (variables)
        children = device_node.get_children()
        for child in children:
            try:
                if child.get_node_class() == ua.NodeClass.Variable:
                    self._process_variable_node(device_name, child)
                    self.subscribe_to_node(child)
                elif child.get_node_class() == ua.NodeClass.Object:
                    # Si hay más objetos anidados, podrías decidir si seguir explorando
                    # o tratarlo como "sub-objetos" del mismo dispositivo.
                    # Ejemplo recursivo:
                    self._process_device_node(device_name, child)
            except Exception as e:
                print(f"⚠️ Error procesando variable/objeto de {device_name}: {str(e)}")

        # Una vez procesado, publicar NBIRTH para este dispositivo
        self._publish_device_birth(device_name)

    def _process_variable_node(self, device_name, node):
        """
        Procesa una variable (nodo) y la registra dentro del dispositivo correspondiente.
        """
        try:
            display_name = node.get_display_name().Text
            node_id = str(node.nodeid)
            value = node.get_value()
            data_type = node.get_data_type_as_variant_type()
            sparkplug_type = self._map_data_type(data_type)

            if sparkplug_type == 0:
                print(f"⚠️ Tipo de dato no soportado para el nodo {display_name}")
                return

            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            self.devices[device_name]["metrics"][node_id] = {
                "name": display_name,
                "node_id": node_id,
                "data_type": sparkplug_type,
                "timestamp": timestamp,
                "value": value
            }
            print(f"✅ [{device_name}] Métrica procesada: {display_name} = {value}")

        except Exception as e:
            print(f"🚨 Error procesando variable: {str(e)}")

    def discover_nodes(self):
        """
        Descubre automáticamente los nodos de tipo 'Device_X' y sus variables
        en el servidor OPC UA, y los registra en self.devices.
        """
        try:
            print(f"Iniciando descubrimiento automático de nodos en {OPC_UA_SERVER}")
            root = self.opcua_client.get_root_node()
            objects = root.get_child(["0:Objects"])

            def browse_recursive(node, current_path=""):
                try:
                    node_name = node.get_browse_name().Name if node.get_browse_name() else "Desconocido"
                    node_id = str(node.nodeid)

                    # Evitar el nodo "Server"
                    if node_name == "Server":
                        return

                    new_path = f"{current_path}/{node_name}" if current_path else node_name
                    print(f"📌 Explorando nodo: {node_name} (ID: {node_id}) -> {new_path}")

                    # Si parece un dispositivo (ej. "Device_1", "Device_2", etc.)
                    # Puedes cambiar la lógica si tus nodos tienen otro naming
                    if node_name.startswith("Device_"):
                        # Procesar como un dispositivo
                        self._process_device_node(node_name, node)
                    else:
                        # Sino, solo explora sus hijos
                        children = node.get_children()
                        for child in children:
                            browse_recursive(child, new_path)

                except Exception as e:
                    print(f"🚨 Error en browse_recursive: {str(e)}")

            # Recorrer recursivamente el nodo Objects
            browse_recursive(objects)

            # Al final, imprime un resumen
            total_devices = len(self.devices)
            total_metrics = sum(len(d["metrics"]) for d in self.devices.values())
            print(f"🔍 Descubrimiento completado. Dispositivos: {total_devices}, Métricas totales: {total_metrics}")

        except Exception as e:
            print(f"Error en descubrimiento de nodos: {str(e)}")

    # ---------------------------------------------------
    #   Publicar BIRTH y DATA (por dispositivo)
    # ---------------------------------------------------
    def _publish_device_birth(self, device_name):
        """
        Publica un NBIRTH para el dispositivo especificado.
        """
        try:
            device_data = self.devices[device_name]
            payload = Payload()
            payload.timestamp = int(time.time() * 1000)
            payload.seq = device_data["seq"]

            # Añadir todas las métricas del dispositivo
            for node_id, metric_info in device_data["metrics"].items():
                metric = payload.metrics.add()
                metric.name = metric_info["name"]
                metric.timestamp = int(time.time() * 1000)
                metric.datatype = metric_info["data_type"]

                # Asignar valor según el tipo
                val = metric_info["value"]
                dt = metric_info["data_type"]
                if dt == 10:     # Double
                    metric.double_value = float(val)
                elif dt == 11:  # Float
                    metric.float_value = float(val)
                elif dt in [7, 8]:  # Int32/Int64
                    metric.int_value = int(val)
                elif dt == 12:  # Boolean
                    metric.boolean_value = bool(val)
                elif dt == 13:  # String
                    metric.string_value = str(val)
                elif dt == 16:  # DateTime
                    if isinstance(val, datetime.datetime):
                        metric.long_value = int(val.timestamp() * 1000)
                    else:
                        metric.long_value = int(time.time() * 1000)
                elif dt == 2:   # Byte
                    metric.int_value = int(val)

            # Publicar NBIRTH en spBv1.0/<GROUP_ID>/NBIRTH/<device_name>
            topic = f"spBv1.0/{GROUP_ID}/NBIRTH/{device_name}"
            self.mqtt_client.publish(topic, payload.SerializeToString())
            print(f"📡 NBIRTH publicado en {topic}")

        except Exception as e:
            print(f"Error publicando NBIRTH para {device_name}: {str(e)}")

    def _publish_device_data(self, device_name, changed_node_id=None):
        """
        Publica NDATA para un dispositivo concreto. 
        Si changed_node_id no es None, se publica solo esa métrica; 
        de lo contrario, se publican todas.
        """
        try:
            device_data = self.devices[device_name]
            device_data["seq"] = (device_data["seq"] + 1) % 256

            payload = Payload()
            payload.timestamp = int(time.time() * 1000)
            payload.seq = device_data["seq"]

            # Determinar si publicamos solo una métrica o todas
            if changed_node_id:
                metric_infos = {
                    changed_node_id: device_data["metrics"][changed_node_id]
                }
            else:
                metric_infos = device_data["metrics"]

            for node_id, metric_info in metric_infos.items():
                metric = payload.metrics.add()
                metric.name = metric_info["name"]
                metric.timestamp = int(time.time() * 1000)
                metric.datatype = metric_info["data_type"]

                val = metric_info["value"]
                dt = metric_info["data_type"]
                if dt == 10:     # Double
                    metric.double_value = float(val)
                elif dt == 11:  # Float
                    metric.float_value = float(val)
                elif dt in [7, 8]:  # Int32/Int64
                    metric.int_value = int(val)
                elif dt == 12:  # Boolean
                    metric.boolean_value = bool(val)
                elif dt == 13:  # String
                    metric.string_value = str(val)
                elif dt == 16:  # DateTime
                    if isinstance(val, datetime.datetime):
                        metric.long_value = int(val.timestamp() * 1000)
                    else:
                        metric.long_value = int(time.time() * 1000)
                elif dt == 2:   # Byte
                    metric.int_value = int(val)

            # Publicar NDATA en spBv1.0/<GROUP_ID>/NDATA/<device_name>
            topic = f"spBv1.0/{GROUP_ID}/NDATA/{device_name}"
            self.mqtt_client.publish(topic, payload.SerializeToString())
            print(f"📡 NDATA publicado en {topic} (device: {device_name})")

        except Exception as e:
            print(f"Error publicando NDATA para {device_name}: {str(e)}")

    # ---------------------------------------------------
    #   Callbacks de suscripción OPC UA
    # ---------------------------------------------------
    def datachange_notification(self, node, val, data):
        """
        Callback para notificaciones de cambio de valor en OPC UA.
        Identifica a qué dispositivo pertenece la variable y publica NDATA.
        """
        try:
            node_id = str(node.nodeid)
            device_name = None

            # Encontrar a qué dispositivo pertenece este node_id
            for d_name, d_data in self.devices.items():
                if node_id in d_data["metrics"]:
                    device_name = d_name
                    break

            if not device_name:
                print(f"⚠️ No se encontró dispositivo para nodeId {node_id}")
                return

            # Actualizar el valor
            self.devices[device_name]["metrics"][node_id]["value"] = val
            self.devices[device_name]["metrics"][node_id]["timestamp"] = \
                datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            display_name = self.devices[device_name]["metrics"][node_id]["name"]
            print(f"🔄 Cambio detectado: {device_name}.{display_name} = {val}")

            # Publicar NDATA SOLO para este dispositivo
            self._publish_device_data(device_name, changed_node_id=node_id)

        except Exception as e:
            print(f"🚨 Error en datachange_notification: {str(e)}")

    def event_notification(self, event):
        print("Evento recibido:", event)

    def status_change_notification(self, status):
        print("Cambio de estado:", status)

    # ---------------------------------------------------
    #   Suscripciones
    # ---------------------------------------------------
    def subscribe_to_node(self, node):
        """Suscribe a los cambios de un nodo específico"""
        try:
            print(f"Intentando suscribirse al nodo: {node}")
            if self.subscription is None:
                print("Creando nueva suscripción...")
                self.subscription = self.opcua_client.create_subscription(
                    period=500,
                    handler=self
                )
                print("Suscripción creada exitosamente")
            
            print(f"Suscribiendo a cambios de datos en: {node.get_display_name().Text}")
            handle = self.subscription.subscribe_data_change(node)
            self.subscription_handles.append(handle)
            print(f"Suscrito exitosamente a {node.get_display_name().Text}")

        except Exception as e:
            print(f"Error al suscribirse al nodo: {str(e)}")
            import traceback
            traceback.print_exc()

    # ---------------------------------------------------
    #   Ejecución Principal
    # ---------------------------------------------------
    def run(self):
        try:
            self.opcua_client.connect()
            print(f"Conectado a servidor OPC UA: {OPC_UA_SERVER}")

            # Descubrir todos los nodos (dispositivos y variables)
            self.discover_nodes()

            # Bucle principal
            while True:
                time.sleep(1)

        except Exception as e:
            print(f"Error crítico: {str(e)}")
        finally:
            if self.subscription:
                self.subscription.delete()
            self.opcua_client.disconnect()
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            print("Desconexión limpia completada")

# ---------------------------------------------------
#   Arranque
# ---------------------------------------------------
if __name__ == "__main__":
    converter = SparkplugConverter()
    converter.run()
