package com.som.job;

import com.som.model.input.UserActivity;
import com.som.model.input.UserActivityDeserializationSchema;
import com.som.model.output.EventDetail;
import com.som.model.output.EventDetailSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;


public class SessionWindowJob {

    private static KafkaSource<UserActivity> createKafkaSource() {
        return KafkaSource.<UserActivity>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("input")
                .setGroupId("session-window")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new UserActivityDeserializationSchema())
                .build();
    }

    private static KafkaSink<EventDetail> createKafkaSink() {
        return KafkaSink.<EventDetail>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("output")
                        .setValueSerializationSchema(new EventDetailSerializationSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static void init() throws Exception {
        // set up the streaming execution environment
        Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "8081-8099");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(createKafkaSource(),
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),
                        "Kafka Source With Custom Watermark Strategy")
                .map(new MapFunction<UserActivity, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(final UserActivity userActivity) throws Exception {
                        return new Tuple2<>(userActivity.getEventType(), 1L);
                    }
                })
                .keyBy(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .reduce((t2, t1) ->
                {
                    t2.f1 += t1.f1;
                    return t2;
                })
                .map(tuple ->
                        EventDetail.builder()
                                .eventType(tuple.f0)
                                .count(tuple.f1)
                                .timestamp(System.currentTimeMillis())
                                .build())
                .sinkTo(createKafkaSink());
        // execute
        env.execute("Session-Window");
    }

    public static void main(String[] args) throws Exception {
        init();
    }
}
