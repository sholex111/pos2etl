# Dockerfile

# Use a standard Python 3.10 slim base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first to leverage Docker layer caching
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code into the container
COPY ./app /app/
COPY ./data /app/data/

# Expose the port Streamlit runs on
EXPOSE 8501

# The default command (can be overridden by docker-compose)
# We set the server address to 0.0.0.0 to allow access from outside the container
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]