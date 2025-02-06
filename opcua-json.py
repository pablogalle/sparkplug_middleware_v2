from opcua import Client
import json

def discover_nodes(node, visited=None, depth=0):
    if visited is None:
        visited = set()

    node_id = str(node.nodeid)
    if node_id in visited:
        return None
    visited.add(node_id)

    indent = "  " * depth
    print(f"{indent}Discovering node: {node.get_browse_name().Name} (ID: {node_id})")

    node_structure = {
        "name": node.get_browse_name().Name,
        "node_id": node_id,
        "class": node.get_node_class().name,
        "children": []
    }

    try:
        # Try to get the value of the node if it is a variable
        if node.get_node_class().name == "Variable":
            try:
                node_structure["value"] = node.get_value()
                print(f"{indent}  Value: {node_structure['value']}")
            except:
                node_structure["value"] = "Unreadable Value"
                print(f"{indent}  Value: Unreadable")

        # Recursively explore children if the node has any
        children = node.get_children()
        for child in children:
            child_structure = discover_nodes(child, visited, depth + 1)
            if child_structure:
                node_structure["children"].append(child_structure)
    except Exception as e:
        node_structure["error"] = str(e)
        print(f"{indent}  Error: {str(e)}")

    return node_structure

def main():
    # Replace with your OPC UA server address
    opc_ua_server = input("Enter OPC UA Server Address: ")

    client = Client(opc_ua_server)

    try:
        client.connect()
        print(f"Connected to OPC UA server at {opc_ua_server}")

        # Start discovery from the Objects node
        root = client.get_root_node()
        objects_node = root.get_child(["0:Objects"])

        print("Starting discovery...")
        structure = discover_nodes(objects_node)

        # Convert the structure to JSON
        json_output = json.dumps(structure, indent=4)

        # Save the JSON to a file
        with open("opcua_structure.json", "w") as f:
            f.write(json_output)

        print("Discovery completed. Structure saved to opcua_structure.json")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()
        print("Disconnected from OPC UA server")

if __name__ == "__main__":
    main()