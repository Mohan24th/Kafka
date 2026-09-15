# Apache Kafka Fundamentals & Implementation Guide

A hands-on Apache Kafka learning project demonstrating **topics, partitions, producers, consumers, consumer groups, offsets, and message routing** using **Node.js, KafkaJS, Docker, and ZooKeeper**.

The implementation focuses on understanding Kafka's core architecture through a simple **rider location update system**.

---

##  Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Core Concepts](#core-concepts)
* [Technologies](#technologies)
* [Project Structure](#project-structure)
* [Docker Setup](#docker-setup)
* [Node.js Setup](#nodejs-setup)
* [Running the Project](#running-the-project)
* [How It Works](#how-it-works)
* [Consumer Groups](#consumer-groups)
* [Partitioning](#partitioning)
* [Important Kafka Concepts](#important-kafka-concepts)
* [Key Takeaways](#key-takeaways)

---

# Overview

Apache Kafka is a distributed event streaming platform designed for building applications that need to **publish, process, and consume large streams of events**.

This project demonstrates Kafka using a simple rider-tracking example.

A producer publishes rider location updates:

```text
alex north
sam south
john north
mike east
```

These events are published to the:

```text
rider-updates
```

Kafka topic.

The topic contains **2 partitions**, allowing us to demonstrate partitioning and consumer-group behavior.

---

# Architecture

```text
                     ┌──────────────┐
                     │   Producer   │
                     │   Node.js    │
                     └──────┬───────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │  rider-updates  │
                   │      Topic      │
                   └────────┬────────┘
                            │
                  ┌─────────┴─────────┐
                  ▼                   ▼
           ┌─────────────┐     ┌─────────────┐
           │ Partition 0 │     │ Partition 1 │
           │    north    │     │ other areas │
           └──────┬──────┘     └──────┬──────┘
                  │                   │
                  └─────────┬─────────┘
                            ▼
                  ┌───────────────────┐
                  │     Consumers     │
                  │                   │
                  │ Group A / Group B │
                  └───────────────────┘
```

---

# Core Concepts

## Topics

A topic is a logical stream where Kafka stores messages.

This project uses:

```text
rider-updates
```

---

## Partitions

The `rider-updates` topic contains two partitions:

```text
rider-updates
│
├── Partition 0
└── Partition 1
```

Partitions allow Kafka to process messages in parallel and provide scalability.

Kafka guarantees ordering **within a partition**.

---

## Producers

The producer publishes rider location events to the Kafka topic.

Example event:

```json
{
  "name": "alex",
  "location": "north"
}
```

The producer also demonstrates explicit partition selection.

---

## Consumers

Consumers read events from Kafka topics.

This project uses KafkaJS to create Node.js consumers.

Each consumer belongs to a **consumer group**.

---

## Consumer Groups

Consumer groups allow multiple consumers to work together.

For example:

```text
group-user-tracking
```

and:

```text
group-notification-service
```

are independent consumer groups.

Each group can consume the same Kafka topic independently.

---

## Offsets

Kafka assigns every message an offset within its partition.

Example:

```text
Partition 0

Offset 0 → Message A
Offset 1 → Message B
Offset 2 → Message C
```

Consumers use offsets to track their progress through a partition.

---

## Replication

Replication creates multiple copies of partitions across Kafka brokers.

For this local setup, replication is:

```text
Replication Factor = 1
```

because only one Kafka broker is running.

In production environments, multiple brokers and higher replication factors are normally used.

---

# Technologies

| Technology       | Purpose                                   |
| ---------------- | ----------------------------------------- |
| **Apache Kafka** | Event streaming platform                  |
| **KafkaJS**      | Node.js Kafka client                      |
| **Node.js**      | Application runtime                       |
| **Docker**       | Local containerized environment           |
| **ZooKeeper**    | Kafka cluster coordination for this setup |

> This project uses the ZooKeeper-based Confluent Platform setup for learning. Modern Kafka deployments can also use **KRaft mode**, which removes the need for ZooKeeper.

---

# Project Structure

```text
kafka-node-demo/
│
├── client.js       # Kafka client configuration
├── admin.js        # Topic creation and administration
├── producer.js     # Publishes rider updates
├── consumer.js     # Consumes rider updates
│
├── package.json
└── package-lock.json
```

### File Responsibilities

**`client.js`**

Contains the reusable Kafka client configuration.

**`admin.js`**

Creates and manages the Kafka topic.

**`producer.js`**

Accepts rider information from the terminal and publishes messages.

**`consumer.js`**

Subscribes to the topic and processes incoming messages.

---

# Docker Setup

The project uses Docker to run Kafka locally.

## 1. Create Docker Network

```bash
docker network create kafka-net
```

The shared network allows the Kafka and ZooKeeper containers to communicate using container names.

---

## 2. Start ZooKeeper

```bash
docker run -d \
  --name zookeeper \
  --network kafka-net \
  -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_TICK_TIME=2000 \
  confluentinc/cp-zookeeper:7.5.0
```

---

## 3. Start Kafka

```bash
docker run -d \
  --name kafka \
  --network kafka-net \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.5.0
```

Verify the containers:

```bash
docker ps
```

You should see:

```text
kafka
zookeeper
```

---

# Node.js Setup

Initialize the Node.js project:

```bash
npm init -y
```

Install KafkaJS:

```bash
npm install kafkajs
```

The Kafka client connects to:

```text
localhost:9092
```

---

# Running the Project

## Step 1 — Start Kafka Infrastructure

Make sure the Kafka and ZooKeeper containers are running:

```bash
docker ps
```

---

## Step 2 — Create the Kafka Topic

Run:

```bash
node admin.js
```

This creates:

```text
Topic: rider-updates
Partitions: 2
```

---

## Step 3 — Start Consumers

Open two terminals.

### Terminal 1

```bash
node consumer.js group-user-tracking
```

### Terminal 2

```bash
node consumer.js group-notification-service
```

---

## Step 4 — Start Producer

Open another terminal:

```bash
node producer.js
```

Enter rider updates:

```text
alex north
sam south
john north
mike east
```

The producer publishes these events to the Kafka topic.

---

# How It Works

The complete flow is:

```text
User Input
    │
    ▼
Node.js Producer
    │
    ▼
Kafka Topic
rider-updates
    │
    ├───────────────┐
    ▼               ▼
Partition 0     Partition 1
    │               │
    └───────┬───────┘
            ▼
        Consumers
```

For example:

```text
alex north
```

is routed to:

```text
Partition 0
```

while:

```text
sam south
```

is routed to:

```text
Partition 1
```

The consumer then receives the event and prints the topic, partition, consumer group, and message value.

---

# Consumer Groups

## Same Consumer Group

If two consumers use the same group:

```bash
node consumer.js group-a
```

```bash
node consumer.js group-a
```

Kafka treats them as members of the same consumer group.

With two partitions:

```text
Partition 0 ─────► Consumer 1

Partition 1 ─────► Consumer 2
```

This provides **parallel processing and scalability**.

---

## Different Consumer Groups

If consumers use different groups:

```bash
node consumer.js group-user-tracking
```

```bash
node consumer.js group-notification-service
```

both groups independently consume the topic.

```text
                 rider-updates
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
      User Tracking        Notification
         Group                Group
```

This is useful when different services need the same events for different purposes.

---

# Partitioning

This project intentionally demonstrates manual partition selection.

The current logic is:

```text
north → Partition 0

everything else → Partition 1
```

Example:

| Rider | Location | Partition |
| ----- | -------- | --------: |
| Alex  | north    |         0 |
| Sam   | south    |         1 |
| John  | north    |         0 |
| Mike  | east     |         1 |

This demonstrates how producers can influence message routing.

---

# Important Kafka Concepts

| Concept                     | Meaning                                                   |
| --------------------------- | --------------------------------------------------------- |
| **Broker**                  | Kafka server responsible for storing and serving messages |
| **Topic**                   | Logical stream of messages                                |
| **Partition**               | Ordered log inside a topic                                |
| **Producer**                | Publishes messages                                        |
| **Consumer**                | Reads messages                                            |
| **Consumer Group**          | Group of consumers working together                       |
| **Offset**                  | Position of a message within a partition                  |
| **Replication Factor**      | Number of copies of a partition                           |
| **Retention**               | How long Kafka keeps messages                             |
| **Acknowledgment (`acks`)** | Producer confirmation level                               |
| **Admin Client**            | Used for Kafka administrative operations                  |

---

# Key Takeaways

### Topics

Topics organize Kafka events into logical streams.

### Partitions

Partitions provide scalability and parallel processing.

### Consumer Groups

Consumer groups allow multiple consumers to process partitions collaboratively.

### Different Consumer Groups

Different groups can independently consume the same topic.

### Offsets

Offsets allow consumers to track their position in a partition.

### Message Ordering

Kafka guarantees ordering within a partition, not across the entire topic.

### Retention

Kafka does not automatically delete a message just because a consumer has read it. Messages remain according to the configured retention policy.

### Replication

Replication improves fault tolerance by maintaining copies of partitions across brokers.

---

#  What This Project Demonstrates

This implementation provides hands-on experience with:

* Apache Kafka fundamentals
* Topics
* Partitions
* Producers
* Consumers
* Consumer groups
* Offsets
* Message routing
* Kafka Admin Client
* Docker-based Kafka setup
* Node.js Kafka integration
* Basic event-driven architecture
* Kafka scalability concepts

---

##  Future Improvements

Possible extensions to this project include:

* [ ] Add multiple Kafka brokers
* [ ] Demonstrate replication
* [ ] Implement automatic key-based partitioning
* [ ] Add producer acknowledgments (`acks`)
* [ ] Demonstrate retries and error handling
* [ ] Add dead-letter topics
* [ ] Implement Kafka-based microservices
* [ ] Add Kafka UI for monitoring
* [ ] Add Docker Compose
* [ ] Migrate the setup from ZooKeeper to KRaft
* [ ] Add schema validation using Avro or JSON Schema
* [ ] Implement real-time rider tracking

---

##  Learning Goal

The goal of this project is to build a strong practical understanding of **Apache Kafka and event-driven architecture** through a small, reproducible Node.js implementation rather than relying only on theoretical concepts.
