# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends git iperf3 procps tzdata curl gnupg ca-certificates

RUN curl -s https://packagecloud.io/install/repositories/ookla/speedtest-cli/script.deb.sh | bash && \
    apt-get install -y --no-install-recommends speedtest

# Set the timezone to Europe/London
ENV TZ=Europe/London

# Configure the system to use the Europe/London timezone
RUN ln -snf /usr/share/zoneinfo/Europe/London /etc/localtime && echo "Europe/London" > /etc/timezone


# Clone the required starlink-grpc-tools repository
RUN git clone https://github.com/sparky8512/starlink-grpc-tools.git

# Install grpc tools from the cloned repository
RUN pip install -r starlink-grpc-tools/requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Remove git and clean up apt cache to reduce image size
RUN apt-get purge -y git curl gnupg && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Make port 2025 available to the world outside this container
EXPOSE 2025

# Create necessary directories
RUN mkdir -p /app/measurement_data/uploaded

# Set environment variables
#ENV CLIENT_NAME star-wroughton
#ENV UPLOAD_URL https://leoscope.surrey.ac.uk/api/upload
#ENV IPERF_SERVER 35.177.249.56
#ENV IPERF_PORT 2025

# Run the script when the container launches
CMD ["python3", "main.py"]
