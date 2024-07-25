# Use the official Python image.
FROM python:3.9-slim

# Set the working directory in the container.
WORKDIR /app

# Copy the current directory contents into the container at /app.
COPY . /app

# Install any needed packages specified in requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the service account key file into the container.
COPY lloyds-hack-grp-43-e35386f27e3f.json /app/lloyds-hack-grp-43-e35386f27e3f.json

# Set environment variables.
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/lloyds-hack-grp-43-e35386f27e3f.json
ENV REDIS_HOST=localhost

# Run app.py when the container launches.
CMD ["python", "analytics-service.py"]
