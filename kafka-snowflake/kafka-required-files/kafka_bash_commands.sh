# Kafka steps 

Terminal-1 - cd /Users/marc/documents/kafka_2.12-3.4.1
Step-1 - bin/zookeeper-server-start.sh config/zookeeper.properties

Terminal-2 - cd /Users/marc/documents/kafka_2.12-3.4.1
Step-2 - bin/kafka-server-start.sh config/server.properties

Terminal-3 - cd /Users/marc/documents/kafka_2.12-3.4.1
Step-3 - bin/kafka-topics.sh --create --topic sales-data --bootstrap-server localhost:9092
Step-4 - bin/kafka-console-consumer.sh --topic sales-data --from-beginning --bootstrap-server localhost:9092

Terminal-4 - cd /Users/marc/coding/public_repo/kafka-snowflake
Step-5 - python3 fake_data.py

Now, you will see the producer producing data and the consumer consuming data



Terminal-1 - cd /Users/marc/documents/kafka_2.12-3.4.1
Step-1 - bin/zookeeper-server-start.sh config/zookeeper.properties

Terminal-2 - cd /Users/marc/documents/kafka_2.12-3.4.1
Step-2 - bin/kafka-server-start.sh config/server.properties

Terminal-3 - cd /Users/marc/documents/kafka_2.12-3.4.1
Step-3 - bin/kafka-topics.sh --create --topic sales-data --bootstrap-server localhost:9092
Step-4 - cd /Users/marc/coding/public_repo/kafka-snowflake
Step-5 - python3 fake_data.py

Terminal-4 - cd /Users/marc/documents/kafka_2.12-3.4.1
Step-6  bin/connect-standalone.sh config/connect-standalone.properties config/SF_connect.properties
Now, you will see the consumer consuming data





#  Delete a topic 
 - bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic test-data

 #  list all topics 
 - bin/kafka-topics.sh --list --bootstrap-server localhost:9092






# Create an unencrypted private key 
openssl genrsa -out rsa_key.pem 2048

# Create a public key referencing the above private key
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub