Apache Kafka with GO - this is one of the best ways to use Kafka with GO - using Sarama by IBM. This library is very stable, provides a lot of functionality and can 100% be used in production (unlike other kafka libraries for GO).


## Setup

1. Install Docker and Docker Compose

2. Run docker-compose up -d in the root directory. This will start Apache Kafka and Zookeeper

3. Install dependencies

4. Run go run producer/producer.go to start the producer which is a REST API listening on port 3000

5. Run go run worker/worker.go to start the consumer


## Test it out!

Send a POST request to localhost:3000

This will produce a message in Apache Kafka and the consumer will process it.

```
curl --location --request POST '0.0.0.0:3000/api/v1/comments' \
--header 'Content-Type: application/json' \
--data-raw '{ "text":"message 1" }'

curl --location --request POST '0.0.0.0:3000/api/v1/comments' \
--header 'Content-Type: application/json' \
--data-raw '{ "text":"message 2" }'
```