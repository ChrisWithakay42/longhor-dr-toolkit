# Requires: pip install boto3 pyyaml kubernetes
import os
import json
import sys
import glob
import shutil
import time
from datetime import datetime

import boto3
import yaml
from botocore.exceptions import ClientError, NoCredentialsError
from kubernetes import client, config, utils
from kubernetes.client.rest import ApiException

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MAPPING_FILE = os.path.join(SCRIPT_DIR, "longhorn-volume-mapping.json")
LONGHORN_BASE_PATH = "longhorn/backupstore"

# --- K8S & S3 CLIENTS ---
def load_kube_config():
    """Loads Kubernetes configuration."""
    try:
        config.load_kube_config()
        return True
    except config.ConfigException as e:
        print(f"Error: Could not load kubeconfig. {e}")
        return False

def get_s3_client(config_env):
    """Initializes and returns a boto3 S3 client."""
    try:
        return boto3.client(
            's3',
            endpoint_url=config_env['endpoint_url'],
            aws_access_key_id=config_env['access_key_id'],
            aws_secret_access_key=config_env['secret_access_key']
        )
    except NoCredentialsError:
        print("Error: Boto3 could not find credentials. Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")
        return None

# --- LONGHORN/S3 HELPERS ---
def get_backups_for_volume(s3_client, bucket, volume_name):
    """Lists all backups for a given Longhorn volume name from the S3 bucket."""
    prefix = f"{LONGHORN_BASE_PATH}/volumes/{volume_name[:2]}/{volume_name[2:4]}/{volume_name}/backups/"
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        backups = []
        for o in response.get('CommonPrefixes', []):
            backup_name = o.get('Prefix').split('/')[-2]
            cfg_key = f"{prefix}{backup_name}/backup.cfg"
            cfg_obj = s3_client.get_object(Bucket=bucket, Key=cfg_key)
            cfg_data = json.loads(cfg_obj['Body'].read().decode('utf-8'))
            backups.append({
                "name": cfg_data["Name"],
                "volume_name": cfg_data["VolumeName"],
                "created_at": cfg_data["Created"],
            })
        return sorted(backups, key=lambda b: b['created_at'], reverse=True)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return []
        print(f"An S3 client error occurred: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding backup metadata for {volume_name}: {e}")
        return None

# --- FILE & KUBECTL HELPERS ---
def find_pvc_manifest(pvc_name, namespace):
    """Finds the YAML file for a given PVC name and namespace."""
    search_patterns = [
        os.path.join(SCRIPT_DIR, "apps", "**", "*.yaml"),
        os.path.join(SCRIPT_DIR, "databases", "**", "*.yaml"),
    ]
    for pattern in search_patterns:
        for filepath in glob.glob(pattern, recursive=True):
            try:
                with open(filepath, 'r') as f:
                    docs = yaml.safe_load_all(f)
                    for doc in docs:
                        if doc and doc.get('kind') == 'PersistentVolumeClaim' and doc.get('metadata', {}).get('name') == pvc_name:
                            if namespace in filepath: # Rough namespace check
                                return filepath
            except (yaml.YAMLError, IOError):
                continue
    return None

def wait_for_pvc_bound(core_v1_api, pvc_name, namespace, timeout=300):
    """Waits for a PVC to enter the 'Bound' state."""
    print(" -> Waiting for PVC to become 'Bound'...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            pvc = core_v1_api.read_namespaced_persistent_volume_claim_status(pvc_name, namespace)
            if pvc.status.phase == 'Bound':
                return True
            time.sleep(5)
        except ApiException as e:
            print(f"API error while waiting for PVC: {e}")
            time.sleep(5)
    print(" -> Timeout reached while waiting for PVC to become Bound.")
    return False

# --- MAIN WORKFLOW ---
def main():
    """Main script execution."""
    print("--- Loading Configuration ---")
    if not load_kube_config(): sys.exit(1)
    
    k8s_client = client.ApiClient()
    core_v1 = client.CoreV1Api()

    config_env = {
        'endpoint_url': os.getenv('S3_ENDPOINT_URL'),
        'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'bucket_name': os.getenv('S3_BUCKET_NAME')
    }
    if not all(config_env.values()):
        print("Error: S3 environment variables not set.")
        print("Please set S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET_NAME.")
        sys.exit(1)

    try:
        with open(MAPPING_FILE, 'r') as f:
            pvc_mappings = json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error reading or parsing mapping file '{MAPPING_FILE}': {e}"); sys.exit(1)

    s3_client = get_s3_client(config_env)
    if not s3_client: sys.exit(1)

    print("\n--- Starting Guided Restoration ---")
    for pvc in pvc_mappings:
        pvc_name, namespace, volume_name = pvc['pvcName'], pvc['namespace'], pvc['volumeName']
        print(f"\nProcessing PVC '{pvc_name}' in namespace '{namespace}'...")
        
        backups = get_backups_for_volume(s3_client, config_env['bucket_name'], volume_name)
        if backups is None: 
            print(" -> Skipping due to an error fetching backups.")
            continue
        if not backups:
            print(" -> No backups found. Skipping.")
            continue

        latest_backup = backups[0]
        user_choice = input(f" -> Found latest backup from {datetime.strptime(latest_backup['created_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')}. Restore? (y/n/list): ").lower()

        chosen_backup = None
        if user_choice == 'y': chosen_backup = latest_backup
        elif user_choice == 'list':
            print(" -> Available backups:")
            for i, b in enumerate(backups):
                print(f"   [{i+1}] {b['name']} ({datetime.strptime(b['created_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')})")
            try:
                selection = int(input("Choose a backup number: ")) - 1
                if 0 <= selection < len(backups):
                    chosen_backup = backups[selection]
            except (ValueError, IndexError):
                print("Invalid selection. Skipping.")

        if not chosen_backup: 
            if user_choice != 'n': print("Skipping this PVC.")
            continue

        manifest_path = find_pvc_manifest(pvc_name, namespace)
        if not manifest_path: 
            print(f" -> Could not find manifest for '{pvc_name}'. Skipping."); continue
        
        print(f" -> Found manifest: {manifest_path}")
        backup_url = f"s3://{config_env['bucket_name']}@/longhorn/?backup={chosen_backup['name']}&volume={chosen_backup['volume_name']}"
        manifest_backup_path = manifest_path + ".bak"
        
        try:
            shutil.copy(manifest_path, manifest_backup_path)
            with open(manifest_path, 'r') as f:
                pvc_doc = yaml.safe_load(f)
            
            pvc_doc.setdefault('metadata', {}).setdefault('annotations', {})['longhorn.io/from-backup'] = backup_url

            with open(manifest_path, 'w') as f:
                yaml.dump(pvc_doc, f)

            print(" -> Applying annotated manifest...")
            utils.create_from_yaml(k8s_client, manifest_path, namespace=namespace)

            if not wait_for_pvc_bound(core_v1, pvc_name, namespace):
                raise IOError("PVC did not become Bound in time.")
            
            print(f" -> Successfully restored PVC '{pvc_name}'!")

        except (IOError, OSError, yaml.YAMLError, ApiException) as e:
            print(f"An error occurred during the restoration process: {e}")
            print("Restoring original manifest from backup.")
            if os.path.exists(manifest_backup_path):
                shutil.move(manifest_backup_path, manifest_path)
            continue
        finally:
            if os.path.exists(manifest_backup_path):
                cleanup_choice = input(" -> Restore original manifest (removes annotation)? (y/n): ").lower()
                if cleanup_choice == 'y':
                    shutil.move(manifest_backup_path, manifest_path)
                    print(" -> Original manifest restored.")
                else:
                    print(f" -> Please manually remove the annotation from {manifest_path} later.")

    print("\n--- Guided Restoration Complete ---")

if __name__ == "__main__":
    main()
