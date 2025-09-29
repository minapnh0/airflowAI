FROM quay.io/astronomer/astro-runtime:12.7.1
USER root
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
USER astro

