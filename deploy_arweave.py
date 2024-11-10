import os
import json
import time
import subprocess
from datetime import datetime
import arweave
from arweave.arweave_lib import Wallet, Transaction
import base64

class ArweaveUploader:
    def __init__(self, keyfile_path="arweave_keyfile.json"):
        self.keyfile_path = keyfile_path
        self.wallet = Wallet(keyfile_path)

    def get_wallet_info(self):
        """Get Arweave wallet information"""
        try:
            balance = self.wallet.balance
            last_tx = self.wallet.get_last_transaction_id()
            return {
                'balance': balance,
                'last_transaction': last_tx
            }
        except Exception as e:
            print(f"Error getting wallet info: {e}")
            return None

    def upload_data(self, data, tags=None):
        """Upload data to Arweave with optional tags"""
        try:
            # Convert binary files dictionary to JSON-serializable format
            json_data = {}
            for filepath, content in data.items():
                # Convert binary content to base64
                base64_content = base64.b64encode(content).decode('utf-8')
                json_data[filepath] = {
                    'content': base64_content,
                    'encoding': 'base64'
                }
            
            # Convert to JSON string
            json_string = json.dumps(json_data)
            
            # Create transaction with data
            transaction = Transaction(self.wallet, data=json_string)
            
            # Add tags if provided
            if tags:
                for tag in tags:
                    transaction.add_tag(tag['name'], tag['value'])
            
            # Add manifest tag for directory structure
            transaction.add_tag('Content-Type', 'application/x.arweave-manifest+json')
            
            # Sign transaction
            transaction.sign()
            
            # Send transaction
            transaction.send()
            
            return {
                'success': True,
                'transaction_id': transaction.id,
                'data_size': len(json_string)
            }
        except Exception as e:
            print(f"Error uploading to Arweave: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def monitor_transaction(self, tx_id, timeout=60, check_interval=5):
        """Monitor transaction status with timeout"""
        print("\nMonitoring transaction status...")
        start_time = time.time()
        transaction = Transaction(self.wallet, id=tx_id)
        
        while True:
            status = transaction.get_status()
            print(f"Status: {status}")
            
            # Check if confirmed
            if status == 'CONFIRMED':
                print(f"\nTransaction confirmed after {time.time() - start_time:.1f} seconds")
                return True
                
            # Check timeout
            if time.time() - start_time > timeout:
                print(f"\nTimeout reached ({timeout} seconds). Final status: {status}")
                return False
                
            # Wait before next check
            time.sleep(check_interval)

def deploy_to_arweave():
    # 1. First, export the app using shinylive
    print("Exporting application with shinylive...")
    subprocess.run(["shinylive", "export", ".", "site"])
    
    # 2. Initialize Arweave uploader
    uploader = ArweaveUploader()
    
    # 3. Prepare metadata tags
    tags = [
        {'name': 'App-Name', 'value': 'User-Metrics-Dashboard'},
        {'name': 'App-Version', 'value': '1.0.0'},
        {'name': 'Deployment-Date', 'value': datetime.now().isoformat()},
        {'name': 'Framework', 'value': 'Shinylive'},
        {'name': 'App-Type', 'value': 'Dashboard'}
    ]
    
    # 4. Read the exported site directory
    print("Reading exported site files...")
    site_files = {}
    for root, _, files in os.walk("site"):
        for file in files:
            filepath = os.path.join(root, file)
            with open(filepath, 'rb') as f:
                site_files[filepath] = f.read()
    
    # 5. Upload to Arweave
    print("Uploading to Arweave...")
    result = uploader.upload_data(site_files, tags)
    
    if result['success']:
        print(f"\nSuccessfully uploaded dashboard to Arweave!")
        print(f"Transaction ID: {result['transaction_id']}")
        print(f"View dashboard at: https://viewblock.io/arweave/tx/{result['transaction_id']}")
        
        # Monitor transaction status
        confirmed = uploader.monitor_transaction(
            result['transaction_id'],
            timeout=120,
            check_interval=10
        )
        
        if confirmed:
            print("\nDeployment confirmed on Arweave!")
        else:
            print("\nNote: Deployment is still pending but can be viewed once confirmed")
    else:
        print(f"\nDeployment failed: {result.get('error')}")

if __name__ == "__main__":
    deploy_to_arweave()