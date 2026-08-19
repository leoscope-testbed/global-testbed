import asyncio
import aiohttp
import aiofiles
import subprocess
import datetime
import os
import glob
import random
import logging
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
import zipfile


# Configuration
data_dir = 'measurement_data'
uploaded_dir = os.path.join(data_dir, 'uploaded')
log_file = 'measurement_log.log'

# Ensure data directories exist
os.makedirs(data_dir, exist_ok=True)
os.makedirs(uploaded_dir, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Load environment variables
client_name = os.environ.get('CLIENT_NAME', 'Unknown')
upload_url = os.environ.get('UPLOAD_URL', 'http://example.com/upload')
# Starlink dish gRPC endpoint for embedded speed test
# Try 192.168.1.1:9000 if the default fails
STARLINK_GRPC_EP = os.environ.get('STARLINK_GRPC_EP', '192.168.100.1:9200')
STARLINK_GRPC_METHOD = "SpaceX.API.Device.Device/Handle"

# Log the environment variables at startup
logging.info(f"Client Name: {client_name}")
logging.info(f"Upload URL: {upload_url}")
logging.info(f"Starlink gRPC endpoint: {STARLINK_GRPC_EP}")


async def zip_file(file_path):
    """
    Compress the specified file into a .zip format, then delete the original file.
    Returns the path to the compressed file.
    """
    zip_path = file_path + '.zip'
    
    # Read the content of the file asynchronously
    async with aiofiles.open(file_path, 'rb') as f_in:
        content = await f_in.read()
    
    # Write to a zip file synchronously
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.writestr(os.path.basename(file_path), content)

    # Delete the original file after zipping
    os.remove(file_path)
    logging.info(f"Original file {file_path} deleted after compression.")
    #logging.info(f"New Zip dir file {zip_path} deleted after compression.")    
    return zip_path


async def run_continuous_grpc_measurement():
    """
    Run the gRPC measurement script continuously, saving and uploading data every hour.
    """
    # **Added logging to indicate the start of gRPC measurement**
    logging.info("Starting gRPC continuous measurement.")

    last_upload_time = datetime.datetime.now()

    # Generate the header once
    header_process = await asyncio.create_subprocess_exec(
        'python3', 'starlink-grpc-tools/dish_grpc_text.py', 'status', '-H', '-g', STARLINK_GRPC_EP,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )
    header_output = await header_process.stdout.read()
    await header_process.wait()

    header = header_output.decode()  # Store the header for reuse
    hourly_data = header  # Initialize `hourly_data` with the header

    # Start the continuous measurement process
    try:
        process = await asyncio.create_subprocess_exec(
            'python3', 'starlink-grpc-tools/dish_grpc_text.py', 'status', '-t', '1', '-g', STARLINK_GRPC_EP,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        logging.info("gRPC continuous measurement process started.")
    except Exception as e:
        logging.error(f"Failed to start gRPC measurement process: {e}")
        return

    while True:
        try:
            line = await process.stdout.readline()
            if not line:
                logging.info("gRPC measurement process ended (EOF reached)")
                break  # EOF reached
            
            try:
                decoded_line = line.decode()
                hourly_data += decoded_line
            except UnicodeDecodeError as e:
                logging.error(f"Failed to decode line from gRPC process: {e}")
                continue

            current_time = datetime.datetime.now()
            if (current_time - last_upload_time).total_seconds() >= 3600:  # Use total_seconds() for accuracy
                try:
                    # Save data to file
                    date_str = last_upload_time.strftime('%Y-%m-%d_%H-%M-%S')
                    filename = f"grpc_{client_name}_{date_str}.txt"
                    file_path = os.path.join(data_dir, filename)
                    async with aiofiles.open(file_path, 'w') as file:
                        await file.write(hourly_data)
                    logging.info(f"gRPC measurement data saved to {file_path}")
                    
                    # Upload the data
                    try:
                        await send_file(file_path)
                        logging.info(f"Successfully uploaded {file_path}")
                    except Exception as e:
                        logging.error(f"Failed to upload file {file_path}: {e}")
                    
                    # Reset `hourly_data` and last upload time
                    hourly_data = header  # Re-add the header
                    last_upload_time = current_time
                    
                except Exception as e:
                    logging.error(f"Failed to save or upload hourly data: {e}")
                    
        except Exception as e:
            logging.error(f"Error in gRPC measurement loop: {e}")
            break


async def schedule_starlink_speedtests(scheduler):
    """
    Schedule Starlink embedded speed tests once every hour at a random offset.
    The first test runs immediately on startup.
    """
    now = datetime.datetime.now()
    scheduler.add_job(run_starlink_speedtest, trigger=DateTrigger(run_date=now))
    logging.info(f"Scheduled initial Starlink speed test at {now.strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        now = datetime.datetime.now()
        next_hour = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        remaining_seconds_in_hour = int((next_hour - now).total_seconds())
        # Leave at least 3 minutes at end of hour to avoid overlap with gRPC poll window
        test_delay = random.randint(0, max(remaining_seconds_in_hour - 180, 0))
        speedtest_time = now + datetime.timedelta(seconds=test_delay)
        scheduler.add_job(run_starlink_speedtest, trigger=DateTrigger(run_date=speedtest_time))
        logging.info(f"Scheduled Starlink speed test at {speedtest_time.strftime('%Y-%m-%d %H:%M:%S')}")
        await asyncio.sleep(max((next_hour - now).total_seconds(), 60))


async def run_starlink_speedtest():
    """
    Run the Starlink embedded speed test via grpcurl against the dish gRPC API.
    Polls until the test finishes, then saves and uploads the JSON result.
    """
    ep = STARLINK_GRPC_EP
    method = STARLINK_GRPC_METHOD

    # Kick off the speed test
    start_cmd = ['grpcurl', '-plaintext', '-d', '{"start_speedtest":{}}', ep, method]
    rc, out = await run_subprocess(start_cmd)
    if rc != 0:
        logging.error(f"Failed to start Starlink speed test (rc={rc}): {out}")
        return

    # Poll for completion (up to 120 s)
    status_cmd = ['grpcurl', '-plaintext', '-d', '{"get_speedtest_status":{}}', ep, method]
    result = None
    for _ in range(120):
        await asyncio.sleep(1)
        rc, out = await run_subprocess(status_cmd)
        if rc != 0:
            logging.error(f"Speed test status poll failed (rc={rc}): {out}")
            break
        try:
            data = json.loads(out)
        except json.JSONDecodeError:
            logging.error(f"Failed to parse speed test status JSON: {out}")
            continue

        status = data.get('getSpeedtestStatus', {}).get('status', {})
        if status.get('running'):
            continue

        down_samples = status.get('down', {}).get('throughputsMbps', [])
        up_samples = status.get('up', {}).get('throughputsMbps', [])
        result = {
            'id': status.get('id'),
            'down_samples_mbps': down_samples,
            'up_samples_mbps': up_samples,
            'latest_down_mbps': down_samples[-1] if down_samples else None,
            'latest_up_mbps': up_samples[-1] if up_samples else None,
        }
        break

    if result is None:
        logging.error("Starlink speed test did not complete within 120 s timeout")
        return

    date_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"starlink_speedtest_{client_name}_{date_str}.json"
    file_path = os.path.join(data_dir, filename)
    try:
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(result))
        logging.info(f"Starlink speed test result saved to {file_path}")
        await send_file(file_path)
    except Exception as e:
        logging.error(f"An unexpected error occurred during Starlink speed test: {e}")

async def run_subprocess(command, cwd=None):
    """
    Run a subprocess command asynchronously and capture its output.
    """
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=cwd
    )
    stdout, _ = await process.communicate()
    return process.returncode, stdout.decode()


async def send_file(file_path):
    """
    Upload the specified file to the server with retries.
    """
    # Compress the file and get the path to the zip file
    zip_path = await zip_file(file_path)  # This returns the full path of the .zip file
    filename = os.path.basename(zip_path)  # Only the filename (not the path)
    url = upload_url
    max_retries = 5
    retry_delay = 60  # Start with a 1-minute delay
    retries = 0

    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                data = aiohttp.FormData()
                
                # Open the zip file (not the original file_path) for uploading
                async with aiofiles.open(zip_path, 'rb') as f:  
                    content = await f.read()
                    data.add_field('file', content, filename=filename)
                
                # Post the file to the upload URL
                async with session.post(url, data=data, timeout=30) as response:
                    if response.status == 200:
                        response_text = await response.text()
                        logging.info(f"Response from server for {filename}: {response_text}")
                        logging.info(f"File {filename} successfully sent.")
                        
                        # Move the zip file to the uploaded directory after successful upload
                        dest_path = os.path.join(uploaded_dir, filename)
                        os.replace(zip_path, dest_path)
                        return  # Exit the function if upload was successful
                    else:
                        response_text = await response.text()
                        logging.error(f"Failed to upload {filename}. Status: {response.status}, Response: {response_text}")
        except Exception as e:
            logging.error(f"Error uploading file {filename}: {e}")

        retries += 1
        logging.warning(f"Retry {retries}/{max_retries} for file {filename} in {retry_delay} seconds.")
        await asyncio.sleep(retry_delay)
        retry_delay *= 2  # Exponential backoff in case of failure

    logging.error(f"Max retries reached. Could not send file {filename}.")

async def cleanup_old_files(hours=48):
    """
    Remove files older than the specified number of hours.
    """
    while True:
        now = datetime.datetime.now()
        cutoff = now - datetime.timedelta(hours=hours)
        for directory in [data_dir, uploaded_dir]:
            for filepath in glob.glob(os.path.join(directory, '*.txt')):
                try:
                    file_mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))
                    if file_mod_time < cutoff:
                        os.remove(filepath)
                        logging.info(f"Removed old file: {filepath}")
                except Exception as e:
                    logging.error(f"Failed to remove file {filepath}: {e}")
        await asyncio.sleep(86400)  # Run once every 24 hours

async def main():
    """
    Main entry point for the script.
    """
    scheduler = AsyncIOScheduler()
    scheduler.start()

    # Start the continuous gRPC measurement
    asyncio.create_task(run_continuous_grpc_measurement())

    # Schedule Starlink embedded speed tests (replaces iperf3)
    asyncio.create_task(schedule_starlink_speedtests(scheduler))

    # Start the cleanup task
    asyncio.create_task(cleanup_old_files())

    try:
        # Keep the main thread alive
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logging.info("Program terminated by user.")
    finally:
        scheduler.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
