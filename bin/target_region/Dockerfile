FROM python:3.13.5-slim

# Install essential system updates only (pyzstd and tarfile handle compression/archiving)
RUN apt-get update && apt-get upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user and group with specific IDs for consistency
RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

# Create application and temp directories with proper ownership
RUN mkdir -p /app /tmp/app-temp /tmp/app-work && \
    chown -R appuser:appuser /app /tmp/app-temp /tmp/app-work && \
    chmod 755 /app && \
    chmod 1777 /tmp/app-temp /tmp/app-work

# Set working directory
WORKDIR /app

# Install Python dependencies as root (system-wide installation)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and set ownership
COPY . .
RUN chown -R appuser:appuser /app && \
    find /app -type d -exec chmod 755 {} \; && \
    find /app -type f -exec chmod 644 {} \; && \
    chmod 755 /app/server.py

# Switch to non-root user
USER appuser

# Set environment variables for temporary directories
ENV PYTHONUNBUFFERED=1
ENV TMPDIR=/tmp/app-temp
ENV TEMP=/tmp/app-temp
ENV TMP=/tmp/app-temp

# Run the application
CMD ["python", "server.py"]
