FROM prefecthq/prefect:2-python3.11

RUN apt-get update && apt-get -y install git

COPY requirements.txt /tmp/pip-tmp/
RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
   && rm -rf /tmp/pip-tmp
   