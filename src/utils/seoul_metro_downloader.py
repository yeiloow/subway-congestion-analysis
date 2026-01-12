import requests
import os
import ssl
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.util import ssl_

# Custom Adapter to handle "DH_KEY_TOO_SMALL" by lowering security level
class LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # Lower security level to allow smaller keys (often needed for older gov sites)
        ctx.set_ciphers('DEFAULT@SECLEVEL=1') 
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx
        )

def download_file(year, seq_no, file_extension):
    url = "https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?&useCache=false"
    
    # Form data based on inspection
    data = {
        'infId': 'OA-12033',
        'infSeq': '1',
        'seq': str(seq_no)
    }
    
    filename = f"seoul_metro_transfer_{year}.{file_extension}"
    print(f"Downloading data for {year}...", end="")
    
    session = requests.Session()
    session.mount('https://', LegacySSLAdapter())
    
    try:
        # verify=False is redundant with the adapter but good for documentation
        response = session.post(url, data=data, stream=True, verify=False)
        response.raise_for_status()
        
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f" Done! Saved as {filename}")
        return True
    except Exception as e:
        print(f" Failed! Error: {e}")
        return False

def main():
    # Sequence mapping found during research:
    # 2025: 9 (.xlsx)
    # 2024: 8 (.csv)
    # 2023: 7 (.csv)
    
    downloads = [
        (2025, 9, 'xlsx'),
        (2024, 8, 'csv'),
        (2023, 7, 'csv')
    ]
    
    print("Starting download of Seoul Metro Transfer Data (2023-2025)...")
    print("-" * 50)
    
    success_count = 0
    for year, seq, ext in downloads:
        if download_file(year, seq, ext):
            success_count += 1
            
    print("-" * 50)
    print(f"Completed: {success_count}/{len(downloads)} files downloaded.")

if __name__ == "__main__":
    main()
