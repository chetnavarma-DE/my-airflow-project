FROM quay.io/astronomer/astro-runtime:3.0.8
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
