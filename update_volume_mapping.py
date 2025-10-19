# Requires: pip install kubernetes
import json
import time
import os
from kubernetes import client, config, watch

# The absolute path to the mapping file.
# It's constructed relative to the script's own location.
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MAPPING_FILE = os.path.join(SCRIPT_DIR, "longhorn-volume-mapping.json")

def load_kube_config():
    """Loads Kubernetes configuration."""
    try:
        config.load_kube_config()
        print("Successfully loaded kube config.")
        return True
    except config.ConfigException as e:
        print(f"Error loading Kubernetes configuration: {e}")
        print("This script must be run on a machine with a valid kubeconfig file.")
        return False

def get_pvc_mapping(core_v1_api):
    """
    Uses the Kubernetes API to get PVC data and returns it as a list of dicts.
    """
    try:
        ret = core_v1_api.list_persistent_volume_claim_for_all_namespaces()
        mapping = []
        for pvc in ret.items:
            # Ensure the volumeName exists, as it might not for unbound PVCs
            if pvc.spec.volume_name:
                mapping.append({
                    "namespace": pvc.metadata.namespace,
                    "pvcName": pvc.metadata.name,
                    "volumeName": pvc.spec.volume_name
                })
        return mapping
    except client.ApiException as e:
        print(f"Error calling Kubernetes API: {e}")
        return None

def update_mapping_file(core_v1_api):
    """
    Fetches the current PVC mapping and writes it to the JSON file.
    """
    print("Updating PVC mapping file...")
    mapping_data = get_pvc_mapping(core_v1_api)
    if mapping_data is not None:
        try:
            with open(MAPPING_FILE, 'w') as f:
                json.dump(mapping_data, f, indent=2)
            print(f"Successfully updated '{MAPPING_FILE}'")
        except IOError as e:
            print(f"Error writing to file '{MAPPING_FILE}': {e}")

def watch_for_changes(core_v1_api):
    """
    Watches for PVC changes in the cluster and triggers an update.
    """
    print("Starting to watch for PVC changes in the cluster...")
    w = watch.Watch()
    try:
        # The stream will time out periodically, so we loop to reconnect.
        for event in w.stream(core_v1_api.list_persistent_volume_claim_for_all_namespaces, timeout_seconds=60):
            event_type = event['type']
            pvc_name = event['object'].metadata.name
            pvc_namespace = event['object'].metadata.namespace
            
            print(f"\nChange detected: {event_type} on PVC '{pvc_name}' in namespace '{pvc_namespace}'")
            
            # Wait a moment for the API server to settle before fetching the new state
            time.sleep(2)
            update_mapping_file(core_v1_api)
            
    except client.ApiException as e:
        if e.status == 410: # "Gone" status means the resource version is too old
            print("Resource version is too old, restarting watch.")
        else:
            print(f"API Error during watch: {e}. Reconnecting in 10 seconds...")
            time.sleep(10)
    except Exception as e:
        print(f"An unexpected error occurred during watch: {e}. Reconnecting in 10 seconds...")
        time.sleep(10)

if __name__ == "__main__":
    if not load_kube_config():
        exit(1)

    # Create an API client instance
    v1 = client.CoreV1Api()

    # Perform an initial update when the script starts
    update_mapping_file(v1)
    
    # Start watching for subsequent changes in a loop to handle watch timeouts
    while True:
        try:
            watch_for_changes(v1)
        except KeyboardInterrupt:
            print("\nWatcher stopped by user. Exiting.")
            break
        except Exception as e:
            print(f"Main loop error: {e}. Restarting watch loop after 10 seconds.")
            time.sleep(10)