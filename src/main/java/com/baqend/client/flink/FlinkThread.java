package com.baqend.client.flink;

import com.baqend.core.LatencyMeasurement;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.UUID;

public class FlinkThread extends Thread {

    private static final String EXCHANGE_NAME = "benchmark";
    private static final String QUEUE_NAME = "flinkTest";

    public FlinkThread(String query) {
    }

    public void run() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

//        DataStream<String> dataStream = env
//                .readTextFile("C:\\Users\\RüschenbaumPatrickIn\\IdeaProjects\\FlinkRabbitMQ\\src\\main\\java\\org\\example\\test.txt")
//                .flatMap(new Splitter());

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setPort(5672)
                .setVirtualHost("/")
                .setUserName("guest")
                .setPassword("guest")
                .build();

        try {
            ConnectionFactory factory = connectionConfig.getConnectionFactory();
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");

            final DataStream<String> rabbitMQStream = env
                    .addSource(new RMQSource<>(
                            connectionConfig,
                            QUEUE_NAME,
                            true,
                            new SimpleStringSchema()));


            tableEnv.createTemporaryView("myTable", rabbitMQStream, "Message");

            Table queryTable = tableEnv.sqlQuery("SELECT * FROM myTable WHERE Message LIKE 'Hello%'");

            // conversion of the queryTable to a retractStream
            // indicating inserts with a true boolean flag
            DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(queryTable, Row.class);
            retractStream.map(new Mapper());
//        retractStream.print();

//        TableSink sink = new CsvTableSink("C:\\Users\\RüschenbaumPatrickIn\\IdeaProjects\\rtdb-sp-benchmark\\src\\main\\java\\com\\baqend\\results\\flinkResults.txt", " ", 1, FileSystem.WriteMode.OVERWRITE);
//        String[] fieldNames = {"Message"};
//        TypeInformation[] fieldTypes = {Types.STRING};
//        tableEnv.registerTableSink("csvSinkTable", fieldNames, fieldTypes, sink);
//        queryTable.insertInto("csvSinkTable");

//        dataStream.addSink(new RMQSink<String>(
//                connectionConfig,
//                "queueName",
//                new SimpleStringSchema()));

            try {
                env.execute("Test Job");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class Mapper implements MapFunction<Tuple2<Boolean, Row>, String> {
        @Override
        public String map(Tuple2<Boolean, Row> booleanRowTuple2) {
            LatencyMeasurement.getInstance().tock(UUID.fromString(booleanRowTuple2.f1.toString().substring(13)));
            return booleanRowTuple2.f1.toString().substring(13);
        }
    }

//    public static class Splitter implements FlatMapFunction<String, String> {
//        @Override
//        public void flatMap(String s, Collector<String> collector) throws Exception {
//            for (String word : s.split("")) {
//                collector.collect(new String(word));
//            }
//            collector.collect(new String(s));
//        }
//    }
}
