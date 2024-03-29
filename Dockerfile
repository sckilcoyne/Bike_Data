# cd Documents\GitHub\Bike_Data
# docker build . -t tweet_bot

FROM python:3.10-slim

COPY utils /utils
COPY dataSources /dataSources
COPY log.conf .
COPY bot_poster.py .

COPY requirements.txt /tmp
RUN pip3 install -r /tmp/requirements.txt


CMD ["python3", "bot_poster.py"]