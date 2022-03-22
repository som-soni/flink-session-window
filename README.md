# flink-session-window
Proof of concept for Flink session windowing (tumbling window) to aggregate user activity events and count the events by event 
types.

## Starting Services
Docker compose file will start kafka, job manager and task manager.

    docker-componse up --build app

## Building Flink Job
Build the flink job that aggregates the use activity events for 60 sec tumbling window and 
write the resulting events to a kafka topic.

    mvn clean package

## Sending the events

Create kafka topics (input/output) using the following commands.

    docker exec -it kafka kafka-topics --create --topic input --bootstrap-server localhost:9092
    docker exec -it kafka kafka-topics --create --topic output --bootstrap-server localhost:9092

Send the user activity events to 'input' kafka topic. 

    docker exec -ti kafka kafka-console-producer --topic input --bootstrap-server localhost:9092
    >{"userId":"1","eventType":"login"}
    >{"userId":"2","eventType":"login"}
    >{"userId":"3","eventType":"login"}
    >{"userId":"1","eventType":"add_to_cart"}
    >{"userId":"1","eventType":"add_to_cart"}
    >{"userId":"1","eventType":"place_order"}

Start a kafka consumer that reads messages from 'output'  topic.

    docker exec -ti kafka kafka-console-consumer --topic output --bootstrap-server localhost:9092

    {"eventType":"login","count":3,"timestamp":1647968810003}
    {"eventType":"place_order","count":1,"timestamp":1647968810043}
    {"eventType":"add_to_cart","count":2,"timestamp":1647968810044}
