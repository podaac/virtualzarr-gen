from python:3.12
workdir /opt/cloud-optimized
COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pip install numcodecs==0.15.1
RUN apt-get update; apt-get install -y jq awscli

COPY generate_cloud_optimized_store.ipynb .
COPY generate_cloud_optimized_store_https.ipynb .

COPY wrapper.sh .
RUN chmod 755 wrapper.sh

ENTRYPOINT ["/opt/cloud-optimized/wrapper.sh"]
