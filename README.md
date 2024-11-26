# ELINK
## How to run
### set up environment on each node respectively
change the advertised_listeners at /kafka/Dockerfile to its ipv4 addr, then run:
```
docker compose build
```

### the following commands show an example to run an ELINK client for data processing
to start the data generator, run:
```
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic SYS
```

the generator will start to produce message data to local kafka breaker at _kafka:9092_ under the topic _SYS_ .

To start an ELINK client that consumes and processes the data on a node, run:
```
docker compose exec jobmanager ./bin/flink run -py /opt/pyflink-walkthrough/workdir/riot-benchmark/bolt/PRED_edge.py -d
```
this will commit a job to Flink CLI， which is expected to process data in PRED dataflow from Riot-benchmark
