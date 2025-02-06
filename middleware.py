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
EDGE_NODE_ID = "Machine01"
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
        self.mqtt_client = mqtt.Client()
        self.opcua_client = Client(OPC_UA_SERVER)
        self.metrics_metadata = {}
        self.seq_number = 0
        self.subscription = None
        self.subscription_handles = []

    def _process_variable_node(self, node):
        try:
            display_name = node.get_display_name().Text
            node_id = str(node.nodeid)
            
            # Obtener valor y tipo de dato
            value = node.get_value()
            data_type = node.get_data_type_as_variant_type()
            
            # Mapear el tipo de dato de OPC UA a Sparkplug B
            sparkplug_type = self._map_data_type(data_type)

            if sparkplug_type == 0:
                print(f"Tipo de dato no soportado para el nodo {display_name}")
                return

            # Timestamp actual
            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
            self.metrics_metadata[display_name] = {
                "node_id": node_id,
                "data_type": sparkplug_type,
                "timestamp": timestamp,
                "value": value
            }
            
            print(f"Métrica procesada: {display_name} = {value} (Tipo: {data_type})")
        except Exception as e:
            print(f"Error procesando variable: {str(e)}")

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

    def provision_specific_node(self, node_path):
        """
        Autoaprovisiona un nodo específico dado su ruta.
        :param node_path: Lista de pasos para llegar al nodo, e.g., ["Objects", "Boilers", "Boiler #1", "CC1001"]
        """
        try:
            # Navegar hasta el nodo específico
            node = self.opcua_client.get_root_node()
            for step in node_path:
                node = node.get_child(f"2:{step}")  # Ajusta el índice '2' según tu namespace

            # Verificar el tipo del nodo
            node_class = node.get_node_class().name
            if node_class != "Object":
                print(f"El nodo {node_path[-1]} no es del tipo 'Object'.")
                return

            # Descubrir referencias y propiedades
            print(f"Descubriendo propiedades del nodo {node_path[-1]}...")
            references = node.get_references()
            for ref in references:
                if ref.ReferenceTypeName == "HasProperty":
                    prop = self.opcua_client.get_node(ref.NodeId)
                    self._process_variable_node(prop)

            print(f"Propiedades del nodo {node_path[-1]} autoaprovisionadas.")
        except Exception as e:
            print(f"Error autoaprovisionando el nodo: {str(e)}")

    def datachange_notification(self, node, val, data):
        """Callback para cambios en los valores de los nodos"""
        try:
            # Obtener el nodeId como string para identificación
            node_id = str(node.nodeid)
            
            # Buscar el display_name en metrics_metadata usando el node_id
            display_name = None
            for name, metadata in self.metrics_metadata.items():
                if metadata['node_id'] == node_id:
                    display_name = name
                    break
            
            if display_name is None:
                print(f"Warning: No se encontró display_name para el nodeId {node_id}")
                return
                
            print(f"Cambio detectado:")
            print(f"  Nodo: {display_name}")
            print(f"  Valor nuevo: {val}")
            print(f"  Data: {data}")
            
            # Actualizar el valor en metrics_metadata
            self.metrics_metadata[display_name]["value"] = val
            self.metrics_metadata[display_name]["timestamp"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
            # Crear y mostrar el payload actualizado
            payload = self.create_sparkplug_payload()
            self._print_payload(payload)
            
        except Exception as e:
            print(f"Error en el callback de cambio de datos: {str(e)}")
            import traceback
            traceback.print_exc()

    def event_notification(self, event):
        """Callback para notificaciones de eventos"""
        print("Evento recibido:", event)

    def status_change_notification(self, status):
        """Callback para cambios de estado"""
        print("Cambio de estado:", status)

    def subscribe_to_node(self, node):
        """Suscribe a los cambios de un nodo específico"""
        try:
            print(f"Intentando suscribirse al nodo: {node}")
            if self.subscription is None:
                print("Creando nueva suscripción...")
                self.subscription = self.opcua_client.create_subscription(
                    period=500,
                    handler=self,

                )
                print("Suscripción creada exitosamente")
            
            print(f"Suscribiendo a cambios de datos...")
            print(f"Tipo de nodo: {type(node)}")
            print(f"Nombre del nodo: {node.get_display_name().Text}")
            print(f"NodeId: {node.nodeid}")
            
            handle = self.subscription.subscribe_data_change(node)
            self.subscription_handles.append(handle)
            print(f"Suscrito exitosamente a cambios en {node.get_display_name().Text}")
        except Exception as e:
            print(f"Error al suscribirse al nodo: {str(e)}")
            print(f"Tipo de error: {type(e)}")
            import traceback
            traceback.print_exc()

    def add_sensor(self, sensor_path):
        """
        Añade un nuevo sensor para monitoreo
        :param sensor_path: Lista con la ruta al sensor, e.g., ["Sensor", "Temperature"]
        """
        try:
            # Navegar hasta el nodo del sensor
            node = self.opcua_client.get_root_node()
            objects = node.get_child(["0:Objects"])
            
            current_node = objects
            for step in sensor_path:
                current_node = current_node.get_child([f"2:{step}"])
            
            # Procesar el nodo
            self._process_variable_node(current_node)
            
            # Suscribirse a cambios
            self.subscribe_to_node(current_node)
            
            return True
            
        except Exception as e:
            print(f"Error añadiendo sensor {'/'.join(sensor_path)}: {str(e)}")
            return False

    def _print_payload(self, payload):
        """Imprime el payload de forma legible"""
        print("\nPayload Sparkplug generado:")
        for metric in payload.metrics:
            print(f"Nombre: {metric.name}")
            if metric.datatype == 10:  # Double
                print(f"Valor: {metric.double_value}")
            elif metric.datatype == 11:  # Float
                print(f"Valor: {metric.float_value}")
            elif metric.datatype in [7, 8]:  # Int32/Int64
                print(f"Valor: {metric.int_value}")
            elif metric.datatype == 12:  # Boolean
                print(f"Valor: {metric.boolean_value}")
            print(f"Timestamp: {metric.timestamp}")
            print("---")

    def test_specific_nodes(self):
        """Prueba la conexión con múltiples sensores"""
        # Lista de sensores a monitorear
        sensors = [
            ["Sensor", "Temperature"],
            # ["Sensor", "Pressure"],
            # Añade más sensores según necesites
        ]

        for sensor_path in sensors:
            print(f"\nConectando al sensor: {'/'.join(sensor_path)}")
            if self.add_sensor(sensor_path):
                print(f"Sensor {'/'.join(sensor_path)} añadido correctamente")

    def create_sparkplug_payload(self):
        payload = Payload()
        payload.timestamp = int(time.time() * 1000)
        
        for name, metadata in self.metrics_metadata.items():
            metric = payload.metrics.add()
            metric.name = name
            metric.datatype = metadata['data_type']
            
            # Asignar valor según el tipo
            if metadata['data_type'] == 10:  # Double
                metric.double_value = float(metadata['value'])
            elif metadata['data_type'] == 11:  # Float
                metric.float_value = float(metadata['value'])
            elif metadata['data_type'] in [7, 8]:  # Int32/Int64
                metric.int_value = int(metadata['value'])
            elif metadata['data_type'] == 12:  # Boolean
                metric.boolean_value = bool(metadata['value'])
            elif metadata['data_type'] == 13:  # String
                metric.string_value = str(metadata['value'])
            elif metadata['data_type'] == 16:  # DateTime
                metric.long_value = int(metadata['value'].timestamp() * 1000)
            
            metric.timestamp = int(time.mktime(
                datetime.datetime.strptime(
                    metadata['timestamp'], 
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ).timetuple()
            )) * 1000
        
        self.seq_number = (self.seq_number + 1) % 256
        payload.seq = self.seq_number
        
        return payload

    def run(self):
        """Bucle principal de ejecución"""
        try:
            # Conexión inicial
            self.opcua_client.connect()
            print(f"Connected to OPC UA: {OPC_UA_SERVER}")

            # Configurar y suscribirse a los sensores
            self.test_specific_nodes()

            # Mantener el programa en ejecución
            while True:
                time.sleep(1)

        except Exception as e:
            print(f"Critical error: {str(e)}")
        finally:
            if self.subscription:
                self.subscription.delete()
            self.opcua_client.disconnect()
            print("Cleanly disconnected")

if __name__ == "__main__":
    converter = SparkplugConverter()
    converter.run()
