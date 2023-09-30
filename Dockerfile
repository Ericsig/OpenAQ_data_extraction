# Use an official Python runtime as a parent image
FROM python:3.8

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

# Install any needed packages specified in requirements.txt
RUN python -m pip install -r requirements.txt

# Run script.py when the container launches
CMD ["python", "extract_openaq_data.py"]
