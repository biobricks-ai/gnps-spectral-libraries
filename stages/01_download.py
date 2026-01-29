import os
import subprocess
import sys

url = "https://external.gnps2.org/gnpslibrary/ALL_GNPS.json"
output_path = "download/ALL_GNPS.json"

def download_file():
    # If file exists and is substantial, don't delete, let curl resume
    if os.path.exists(output_path):
        size = os.path.getsize(output_path)
        print(f"File exists, size: {size} bytes.")
        if size < 1000: # Arbitrary small size check
             os.remove(output_path)
    
    print(f"Downloading {url} to {output_path} using curl...")
    cmd = [
        "curl", 
        "-L", 
        "-C", "-", # Resume
        "--retry", "10", 
        "--retry-delay", "10",
        "--retry-max-time", "600",
        "-o", output_path,
        url
    ]
    
    result = subprocess.run(cmd)
    if result.returncode != 0:
        # Curl exit code 33 is 'resume failed' (e.g. file fully downloaded already sometimes?)
        # Standard curl success is 0.
        print(f"Download failed with code {result.returncode}.")
        sys.exit(result.returncode)
    print("Download complete.")

if __name__ == "__main__":
    download_file()
