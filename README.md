# Dashboard with Twitter data related to Ukraine

This project has been done as the assignment in the course focused on real-time big data processing taught on Free University of Bozen-Bolzano.

The repository contains all files necessary to run the pipeline which processes data from Twitter in real time. The goal of the processing is to visualize some interesting information about current events on Ukraine based on data present in tweet hashtags and user's profile.

The pipeline contains Kafka, Kafka Connect, Apache Spark engine with processor, PostgreSQL database as sink for the processed data, and Grafana dashboard to visualize the output. The whole pipeline is also shown in this diagram:

![architecture diagram](./images/architecture_diagram.png)

## Running the pipeline

In order to run the pipeline, you have to fill in Twitter API authentication credentials (with permissions to access Streaming API) in `kafka_connect_entrypoint.sh` (on lines 11, 19, 20 and 21). After that, you have to run the following command in the CLI (assuming you have Docker engine up and running):

```
docker-compose up --build
```

The visualisations will be then available on Grafana dashboard at [localhost:23000](http://localhost:23000). By default you can access this dashboard under user 'user' with password 'user'.

Please mind that it will take some time (approximately 10 minutes since the pipeline has been started) before first results appear in the dashboard. This is caused by the set watermark in the Spark processor.
