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
        self.edge_node_id = None  # Inicializar EDGE_NODE_ID
        
        # Configurar callbacks MQTT
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        
        # Conectar al broker MQTT
        try:
            self.mqtt_client.connect(BROKER, PORT)
            self.mqtt_client.loop_start()  # Iniciar loop en background
            print(f"Cliente MQTT iniciado - Broker: {BROKER}:{PORT}")
        except Exception as e:
            print(f"Error conectando al broker MQTT: {str(e)}")

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """Callback cuando se conecta al broker MQTT"""
        print(f"Conectado al broker MQTT con código: {rc}")
        if rc == 0:
            # Publicar NBIRTH al conectar
            self._publish_birth()
        else:
            print(f"Error conectando al broker MQTT. Código: {rc}")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        """Callback cuando se desconecta del broker MQTT"""
        print(f"Desconectado del broker MQTT con código: {rc}")

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
            node_id = str(node.nodeid)
            
            # Buscar el display_name en metrics_metadata
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
            
            # Actualizar el valor en metrics_metadata
            self.metrics_metadata[display_name]["value"] = val
            self.metrics_metadata[display_name]["timestamp"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
            # Crear payload y publicar NDATA
            try:
                payload = self.create_sparkplug_payload()
                if payload and payload.metrics:
                    topic = self._get_topic("NDATA")
                    self.mqtt_client.publish(topic, payload.SerializeToString())
                    print(f"NDATA publicado en {topic}")
                else:
                    print("No hay métricas para publicar")
            except Exception as e:
                print(f"Error publicando NDATA: {str(e)}")
                import traceback
                traceback.print_exc()
            
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
        """Crea un payload Sparkplug B con los datos actuales"""
        payload = Payload()
        payload.timestamp = int(time.time() * 1000)  # Timestamp en milisegundos
        
        for name, metadata in self.metrics_metadata.items():
            try:
                metric = payload.metrics.add()
                metric.name = name
                metric.timestamp = int(time.mktime(
                    datetime.datetime.strptime(
                        metadata['timestamp'], 
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ).timetuple()
                )) * 1000  # Timestamp en milisegundos
                metric.datatype = metadata['data_type']
                
                # Verificar si el valor es None
                if metadata['value'] is None:
                    print(f"Warning: Valor None para la métrica {name}")
                    continue
                
                # Asignar valor según el tipo de dato
                match metadata['data_type']:
                    case 10:  # Double
                        metric.double_value = float(metadata['value'])
                    case 11:  # Float
                        metric.float_value = float(metadata['value'])
                    case 7 | 8:  # Int32/Int64
                        metric.int_value = int(metadata['value'])
                    case 12:  # Boolean
                        metric.boolean_value = bool(metadata['value'])
                    case 13:  # String
                        metric.string_value = str(metadata['value'])
                    case 16:  # DateTime
                        if isinstance(metadata['value'], datetime.datetime):
                            metric.long_value = int(metadata['value'].timestamp() * 1000)
                        else:
                            metric.long_value = int(time.time() * 1000)
                    case 2:  # Byte
                        metric.int_value = int(metadata['value'])
                    case _:
                        print(f"Tipo de dato no soportado: {metadata['data_type']}")
                        continue
            except Exception as e:
                print(f"Error procesando métrica {name}: {str(e)}")
                continue
        
        # Incrementar y establecer el número de secuencia
        self.seq_number = (self.seq_number + 1) % 256
        payload.seq = self.seq_number
        
        return payload

    def _publish_birth(self):
        """Publica el mensaje NBIRTH con la configuración inicial"""
        try:
            payload = Payload()
            payload.timestamp = int(time.time() * 1000)
            payload.seq = self.seq_number
            
            # Añadir todas las métricas conocidas
            for name, metadata in self.metrics_metadata.items():
                metric = payload.metrics.add()
                metric.name = name
                metric.timestamp = int(time.time() * 1000)
                metric.datatype = metadata['data_type']
                
                # Establecer el valor inicial
                match metadata['data_type']:
                    case 10:  # Double
                        metric.double_value = float(metadata['value'])
                    case 11:  # Float
                        metric.float_value = float(metadata['value'])
                    case 7 | 8:  # Int32/Int64
                        metric.int_value = int(metadata['value'])
                    case 12:  # Boolean
                        metric.boolean_value = bool(metadata['value'])
                    case 13:  # String
                        metric.string_value = str(metadata['value'])
                    case 16:  # DateTime
                        metric.long_value = int(metadata['value'].timestamp() * 1000)
                    case _:
                        print(f"Tipo de dato no soportado: {metadata['data_type']}")
                        continue

            # Publicar el mensaje NBIRTH
            topic = self._get_topic("NBIRTH")
            self.mqtt_client.publish(topic, payload.SerializeToString())
            print(f"NBIRTH publicado en {topic}")
            
        except Exception as e:
            print(f"Error publicando NBIRTH: {str(e)}")
            import traceback
            traceback.print_exc()

    def _get_topic(self, message_type):
        """
        Genera el topic Sparkplug B según el tipo de mensaje
        :param message_type: Tipo de mensaje (NBIRTH, NDATA, NDEATH)
        :return: Topic formateado
        """
        return f"spBv1.0/{GROUP_ID}/{message_type}/{self.edge_node_id}"  # Usar EDGE_NODE_ID dinámico

    def discover_nodes(self):
        """Descubre automáticamente los nodos variables relevantes del servidor"""
        try:
            print(f"Iniciando descubrimiento automático de nodos en {OPC_UA_SERVER}")
            root = self.opcua_client.get_root_node()
            objects = root.get_child(["0:Objects"])
            
            def browse_recursive(node, current_path=""):
                """Navega recursivamente por los nodos relevantes"""
                try:
                    # Obtener el nombre del nodo actual
                    node_name = node.get_display_name().Text
                    
                    # Saltar el nodo Server y sus descendientes
                    if node_name == "Server":
                        return
                    
                    # Asignar EDGE_NODE_ID basado en la ruta del nodo
                    self.edge_node_id = f"{current_path}/{node_name}"  # Usar la ruta completa como EDGE_NODE_ID

                    # Actualizar la ruta actual
                    new_path = f"{current_path}/{node_name}" if current_path else node_name
                    print(f"Explorando nodo: {new_path}")
                    
                    for child in node.get_children():
                        try:
                            node_class = child.get_node_class()
                            
                            # Si es una variable, procesarla
                            if node_class == ua.NodeClass.Variable:
                                print(f"Encontrada variable: {child.get_display_name().Text}")
                                self._process_variable_node(child)
                                self.subscribe_to_node(child)
                            
                            # Si es un objeto, explorar sus hijos
                            elif node_class == ua.NodeClass.Object:
                                browse_recursive(child, new_path)
                                
                        except Exception as e:
                            print(f"Error procesando nodo hijo: {str(e)}")
                            continue
                            
                except Exception as e:
                    print(f"Error en browse_recursive: {str(e)}")

            # Comenzar el descubrimiento desde el nodo Objects
            browse_recursive(objects)
            
            print("Descubrimiento de nodos completado")
            print(f"Nodos encontrados: {len(self.metrics_metadata)}")
            
        except Exception as e:
            print(f"Error en descubrimiento de nodos: {str(e)}")

    def run(self):
        """Bucle principal de ejecución"""
        try:
            # Conexión inicial OPC UA
            self.opcua_client.connect()
            print(f"Conectado a servidor OPC UA: {OPC_UA_SERVER}")

            # Descubrir automáticamente todos los nodos
            self.discover_nodes()

            # Mantener el programa en ejecución
            while True:
                time.sleep(1)

        except Exception as e:
            print(f"Error crítico: {str(e)}")
        finally:
            if self.subscription:
                self.subscription.delete()
            self.opcua_client.disconnect()
            self.mqtt_client.loop_stop()  # Detener el loop MQTT
            self.mqtt_client.disconnect()
            print("Desconexión limpia completada")

if __name__ == "__main__":
    converter = SparkplugConverter()
    converter.run()
