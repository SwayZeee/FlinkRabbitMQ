package com.baqend.client;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

public class FlinkThread extends Thread {

    private String query;

    public FlinkThread(String query) {
        this.query = query;
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

        final DataStream<String> dataStream = env
                .addSource(new RMQSource<String>(
                        connectionConfig,
                        "hello",
                        true,
                        new SimpleStringSchema())).flatMap(new Splitter());

        // dataStream.print();

        tableEnv.createTemporaryView("myTable", dataStream, "Message");

        Table queryTable = tableEnv.sqlQuery("SELECT * FROM myTable WHERE Message LIKE 'Hello%'");

        TableSink sink = new CsvTableSink("C:\\Users\\RüschenbaumPatrickIn\\IdeaProjects\\FlinkRabbitMQ\\src\\main\\java\\com\\baqend\\results\\flinkResults.txt", " ", 1, FileSystem.WriteMode.OVERWRITE);
        String[] fieldNames = {"Message"};
        TypeInformation[] fieldTypes = {Types.STRING};
        tableEnv.registerTableSink("csvSinkTable", fieldNames, fieldTypes, sink);
        queryTable.insertInto("csvSinkTable");

//        dataStream.addSink(new RMQSink<String>(
//                connectionConfig,
//                "queueName",
//                new SimpleStringSchema()));

        try {
            env.execute("Test Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Splitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String s, org.apache.flink.util.Collector<String> collector) throws Exception {
//            for (String word : s.split("")) {
//                collector.collect(new String(word));
//            }
            collector.collect(new String(s));
        }
    }
}
