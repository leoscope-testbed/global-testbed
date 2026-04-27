import asyncio
import aiohttp
import aiofiles
import subprocess
import datetime
import os
import glob
import random
import logging
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
IPERF_SERVER = os.environ.get('IPERF_SERVER', 'iperf.example.com')
IPERF_PORT = os.environ.get('IPERF_PORT', '2025')
SPEEDTEST_ENABLED = os.environ.get('SPEEDTEST_ENABLED', 'true').lower() in {'1', 'true', 'yes', 'on'}

# Log the environment variables at startup
logging.info(f"Client Name: {client_name}")
logging.info(f"Upload URL: {upload_url}")
logging.info(f"Iperf Server: {IPERF_SERVER}")
logging.info(f"Iperf Port: {IPERF_PORT}")
logging.info(f"Speedtest Enabled: {SPEEDTEST_ENABLED}")


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
        'python3', 'starlink-grpc-tools/dish_grpc_text.py', 'status', '-H',
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
            'python3', 'starlink-grpc-tools/dish_grpc_text.py', 'status', '-t', '1',
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


async def schedule_iperf_tests(scheduler):
    """
    Schedule iperf tests (both uplink and downlink) once every hour.
    The first test runs immediately, and subsequent tests are scheduled
    at random times within each hour.
    """
    # First, schedule an immediate iperf test
    now = datetime.datetime.now()
    # Schedule downlink test immediately
    scheduler.add_job(
        run_iperf_test,
        args=['downlink'],
        trigger=DateTrigger(run_date=now)
    )
    logging.info(f"Scheduled initial downlink iperf test at {now.strftime('%Y-%m-%d %H:%M:%S')}")

    # Schedule uplink test 1 minute after downlink test
    uplink_time = now + datetime.timedelta(minutes=1)
    scheduler.add_job(
        run_iperf_test,
        args=['uplink'],
        trigger=DateTrigger(run_date=uplink_time)
    )
    logging.info(f"Scheduled initial uplink iperf test at {uplink_time.strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        now = datetime.datetime.now()
        next_hour = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)

        # Total time remaining in the hour
        remaining_seconds_in_hour = int((next_hour - now).total_seconds())

        iperf_duration = 60  # Duration of each iperf test in seconds
        min_gap = 120        # Minimum gap between tests in seconds

        # Minimum total duration required for both tests
        minimum_total_duration = iperf_duration + min_gap

        if remaining_seconds_in_hour <= minimum_total_duration:
            # Not enough time left in the hour, wait until next hour
            sleep_seconds = remaining_seconds_in_hour
            logging.info(f"Not enough time left in the hour to schedule tests. Sleeping for {sleep_seconds} seconds until next hour.")
            await asyncio.sleep(sleep_seconds)
            continue

        # Generate random delay between 0 and (remaining_seconds_in_hour - minimum_total_duration)
        max_start_time = remaining_seconds_in_hour - minimum_total_duration
        test_delay = random.randint(0, max_start_time)

        downlink_time = now + datetime.timedelta(seconds=test_delay)
        scheduler.add_job(
            run_iperf_test,
            args=['downlink'],
            trigger=DateTrigger(run_date=downlink_time)
        )
        logging.info(f"Scheduled downlink iperf test at {downlink_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Schedule uplink test after min_gap seconds
        uplink_time = downlink_time + datetime.timedelta(seconds=min_gap)
        scheduler.add_job(
            run_iperf_test,
            args=['uplink'],
            trigger=DateTrigger(run_date=uplink_time)
        )
        logging.info(f"Scheduled uplink iperf test at {uplink_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Wait until the next hour to schedule again
        sleep_seconds = (next_hour - now).total_seconds()
        if sleep_seconds > 0:
            logging.info(f"Sleeping for {sleep_seconds} seconds until next scheduling.")
            await asyncio.sleep(sleep_seconds)
        else:
            # In rare cases where sleep_seconds is negative or zero
            logging.warning("Sleep duration is non-positive. Correcting to 60 seconds.")
            await asyncio.sleep(60)


async def schedule_speedtest_tests(scheduler):
    """
    Schedule Ookla Speedtest once every hour. This becomes the default dashboard
    throughput source while the legacy iperf cadence stays in place.
    """
    if not SPEEDTEST_ENABLED:
        logging.info("Speedtest scheduling is disabled.")
        return

    now = datetime.datetime.now()
    scheduler.add_job(
        run_speedtest,
        trigger=DateTrigger(run_date=now)
    )
    logging.info(f"Scheduled initial Speedtest at {now.strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        now = datetime.datetime.now()
        next_hour = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        remaining_seconds_in_hour = int((next_hour - now).total_seconds())
        test_delay = random.randint(0, max(remaining_seconds_in_hour - 180, 0))
        speedtest_time = now + datetime.timedelta(seconds=test_delay)
        scheduler.add_job(
            run_speedtest,
            trigger=DateTrigger(run_date=speedtest_time)
        )
        logging.info(f"Scheduled Speedtest at {speedtest_time.strftime('%Y-%m-%d %H:%M:%S')}")
        await asyncio.sleep(max((next_hour - now).total_seconds(), 60))

async def run_iperf_test(mode):
    """
    Run an iperf test in the specified mode ('uplink' or 'downlink').
    """
    if mode == 'uplink':
        # For uplink test, use '-R'
        command = ['iperf3', '-c', IPERF_SERVER, '-p', IPERF_PORT, '-t', '60', '-R']
    elif mode == 'downlink':
        # For downlink test, do not use '-R'
        command = ['iperf3', '-c', IPERF_SERVER, '-p', IPERF_PORT, '-t', '60']
    else:
        logging.error(f"Invalid iperf mode: {mode}")
        return

    date_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"iperf_{mode}_{client_name}_{date_str}.txt"
    file_path = os.path.join(data_dir, filename)
    try:
        returncode, output = await run_subprocess(command)
        if returncode == 0:
            async with aiofiles.open(file_path, 'w') as file:
                await file.write(output)
            logging.info(f"Iperf {mode} test completed and saved to {file_path}")
            # Immediately upload the data
            await send_file(file_path)
        elif returncode == 1:
            logging.error(f"Iperf {mode} test failed: Server not reachable.")
            logging.error(f"Output: {output}")
        else:
            logging.error(f"Iperf {mode} test failed with return code {returncode}")
            logging.error(f"Output: {output}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during iperf {mode} test: {e}")


async def run_speedtest():
    """
    Run Ookla Speedtest CLI and upload its JSON result for dashboard ingestion.
    """
    command = [
        'speedtest',
        '--format=json',
        '--accept-license',
        '--accept-gdpr'
    ]
    date_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"speedtest_{client_name}_{date_str}.txt"
    file_path = os.path.join(data_dir, filename)
    try:
        returncode, output = await run_subprocess(command)
        if returncode == 0:
            async with aiofiles.open(file_path, 'w') as file:
                await file.write(output)
            logging.info(f"Speedtest completed and saved to {file_path}")
            await send_file(file_path)
        else:
            logging.error(f"Speedtest failed with return code {returncode}")
            logging.error(f"Output: {output}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during Speedtest: {e}")

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

    # Schedule iperf tests
    asyncio.create_task(schedule_iperf_tests(scheduler))

    # Schedule Speedtest throughput measurements for dashboard panels
    asyncio.create_task(schedule_speedtest_tests(scheduler))

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
