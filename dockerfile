FROM python:3.9-slim
LABEL maintainer="Joel Edwards <joel.edwards.372@gmail.com>"

# Set environment variables for non-interactive apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Set workdir and copy requirements
WORKDIR /app

# Install Java (required for PySpark)
# Using adoptopenjdk for a more recent and actively maintained JDK
RUN apt-get update && apt-get install -y \
    default-jdk \
    procps \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin
#ENV PYSPARK_PYTHON=python3
#ENV PYSPARK_DRIVER_PYTHON=python3

# Install Jupyter Notebook and other Python packages using pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create folders
RUN mkdir -p /app/data_quality_profiles && \
    mkdir -p /app/data_star_schema_prep && \
    mkdir -p /app/etl_development && \
	mkdir -p /app/src_data && \
	mkdir -p /app/dashboard && \
    mkdir -p /app/notebooks 

# Expose Jupyter and Streamlit ports
EXPOSE 8888 8501
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

#docker compose up --build -d