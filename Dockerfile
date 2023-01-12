# cd Documents\GitHub\Bike_Data
# docker build . -t tweet_bot

FROM python:3.10-slim

COPY utils /utils
COPY dataSources /dataSources
COPY log.conf .
COPY tweet_bot.py .

COPY requirements.txt /tmp
RUN pip3 install -r /tmp/requirements.txt


CMD ["python3", "tweet_bot.py"]