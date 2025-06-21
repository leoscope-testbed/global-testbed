import subprocess
import datetime
import requests
import time
import threading
import random
import os
import glob
import logging

# Get environment variables
client_name = os.getenv('CLIENT_NAME', 'default_client_name')
upload_url = os.getenv('UPLOAD_URL', 'https://netsys.surrey.ac.uk/upload')
iperf_server = os.getenv('IPERF_SERVER', '35.177.249.56')

data_dir = "measurement_data"
uploaded_dir = os.path.join(data_dir, "uploaded")
script_dir = "starlink-grpc-tools/"

# Ensure data and uploaded directories exist
os.makedirs(data_dir, exist_ok=True)
os.makedirs(uploaded_dir, exist_ok=True)

# Configure logging
logging.basicConfig(filename='measurement_log.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

def run_command_for_6_hours():
    while True:
        date_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = os.path.join(data_dir, f'grpc_{client_name}_{date_str}.txt')
        temp_filename = filename + ".tmp"
        with open(temp_filename, 'w') as file:
            process = subprocess.Popen(['python3', 'dish_grpc_text.py', 'status', '-t', '1'],
                                       stdout=file, stderr=subprocess.STDOUT, cwd=script_dir)
            try:
                # Let the process run for 6 hours (21600 seconds)
                time.sleep(21600)
            except Exception as e:
                logging.error(f"An error occurred while running the command: {e}")
            finally:
                process.terminate()
                os.rename(temp_filename, filename)

def iperf_test(mode, duration=60, port=2025):
    command = ['iperf3', '-c', iperf_server, '-p', str(port), '-t', str(duration)]
    if mode == 'downlink':
        command.append('-R')
    result_file = f"iperf_{mode}_{client_name}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
    temp_result_file = result_file + ".tmp"
    result_path = os.path.join(data_dir, temp_result_file)
    final_result_path = os.path.join(data_dir, result_file)
    with open(result_path, 'w') as outfile:
        subprocess.run(command, stdout=outfile, stderr=subprocess.STDOUT)
    os.rename(result_path, final_result_path)

def schedule_iperf_tests():
    # Perform the first iperf test immediately
    iperf_test('downlink')
    time.sleep(10)  # Wait 10 seconds between downlink and uplink tests
    iperf_test('uplink')

    while True:
        # Random delay within the next hour (0 to 3600 seconds)
        delay = random.randint(0, 3600)
        time.sleep(delay)
        
        # Perform the iperf tests
        iperf_test('downlink')
        time.sleep(10)  # Wait 10 seconds between downlink and uplink tests
        iperf_test('uplink')
        
        # Sleep for the remaining time in the hour after the random delay
        remaining_time = 3600 - delay - 60 - 10  # 60 seconds for each iperf test and 10 seconds wait
        time.sleep(remaining_time)

def upload_file(url, filename):
    try:
        with open(filename, 'rb') as file:
            files = {'file': (os.path.basename(filename), file)}
            response = requests.post(url, files=files)
            logging.info(f"Response from server for {filename}: {response.text}")
            return response.status_code == 200
    except IOError as e:
        logging.error(f"Error uploading file {filename}: {e}")
        return False

def send_file(filename):
    while True:
        success = upload_file(upload_url, filename)
        if success:
            logging.info(f"File {filename} successfully sent.")
            # Move the file to the uploaded directory
            os.rename(filename, os.path.join(uploaded_dir, os.path.basename(filename)))
            break
        else:
            logging.warning(f"Failed to send file {filename}. Retrying in a random time between 5 and 10 minutes...")
            wait_time = random.randint(300, 600)  # Wait for a random time between 5 and 10 minutes
            time.sleep(wait_time)

def upload_files(data_dir, upload_url):
    for filename in glob.glob(os.path.join(data_dir, '*.txt')):
        if not filename.endswith('.tmp'):
            send_file(filename)

def cleanup_old_files(directory, hours=48):
    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(hours=hours)
    
    for filename in glob.glob(os.path.join(directory, '*.txt')):
        try:
            # Extract the date string from the filename and convert to datetime object
            date_str = '_'.join(filename.split('_')[-2:]).replace('.txt', '')
            file_date = datetime.datetime.strptime(date_str, '%Y-%m-%d_%H-%M-%S')
            
            if file_date < cutoff:
                os.remove(filename)
                logging.info(f"Removed old file: {filename}")
        except Exception as e:
            logging.error(f"Failed to remove file {filename}: {e}")

def main():
    # Start the cleanup function in a separate thread to run every 24 hours
    def cleanup_scheduler():
        while True:
            cleanup_old_files(data_dir)
            cleanup_old_files(uploaded_dir)
            time.sleep(86400)  # Sleep for 24 hours

    threading.Thread(target=cleanup_scheduler, daemon=True).start()

    # Start continuous background data collection
    threading.Thread(target=run_command_for_6_hours, daemon=True).start()

    # Start iperf tests every hour at random times, with the first one immediately
    threading.Thread(target=schedule_iperf_tests, daemon=True).start()

    while True:
        upload_files(data_dir, upload_url)
        time.sleep(3600)  # Check for new files to upload every hour

if __name__ == '__main__':
    main()
