# Use python3.8 as the base image
FROM python:3.8

# Set the working directory and copy all the files to the container
WORKDIR /usr/src/
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Specify the port number that needs to be exposed
EXPOSE 1077

# Command for running the application
CMD ["python3", "./client.py", "--port", "2077", "--c_id", "0", "--ip", "127.0.0.1", "--path", "/usr/src/"]


