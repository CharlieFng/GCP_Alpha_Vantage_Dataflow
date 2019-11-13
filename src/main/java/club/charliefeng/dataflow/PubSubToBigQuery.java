package club.charliefeng.dataflow;
;
import club.charliefeng.dataflow.dto.StockData;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.api.client.util.DateTime;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;


import java.io.IOException;
import java.time.format.DateTimeFormatter;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.STREAMING_INSERTS;

public class PubSubToBigQuery {

    /*
     * Define your own configuration options. Add your own arguments to be processed
     * by the command-line parser, and specify default values for them.
     */
    public interface PubSubToBigQueryOption extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        String getInputTopic();
        void setInputTopic(String value);

        @Description("Output file's window size in number of minutes.")
        @Default.Integer(5)
        Integer getWindowSize();
        void setWindowSize(Integer value);

//        @Description("Path of the output file including its filename prefix.")
//        @Validation.Required
//        String getOutput();
//        void setOutput(String value);

        @Description("specify the fully-qualified BigQuery table name.")
        @Validation.Required
        String getTableSpec();
        void setTableSpec(String value);
    }

    public static void main(String[] args) throws IOException {
        // The maximum number of shards when writing output.
        int numShards = 1;

        TableSchema stockSchema = new TableSchema().setFields(ImmutableList.of(
                new TableFieldSchema()
                    .setName("dateTime")
                    .setType("STRING")
                    .setMode("REQUIRED"),
                new TableFieldSchema()
                    .setName("open")
                    .setType("NUMERIC")
                    .setMode("REQUIRED"),
                new TableFieldSchema()
                    .setName("high")
                    .setType("NUMERIC")
                    .setMode("REQUIRED"),
                new TableFieldSchema()
                    .setName("low")
                    .setType("NUMERIC")
                    .setMode("REQUIRED"),
                new TableFieldSchema()
                    .setName("close")
                    .setType("NUMERIC")
                    .setMode("REQUIRED"),
                new TableFieldSchema()
                    .setName("adjustedClose")
                    .setType("NUMERIC")
                    .setMode("REQUIRED"),
                new TableFieldSchema()
                    .setName("volume")
                    .setType("INTEGER")
                    .setMode("REQUIRED"),
                new TableFieldSchema()
                    .setName("dividendAmount")
                    .setType("NUMERIC")
                    .setMode("REQUIRED"),
                new TableFieldSchema()
                    .setName("splitCoefficient")
                    .setType("NUMERIC")
                    .setMode("REQUIRED")

        ));

        PubSubToBigQueryOption options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PubSubToBigQueryOption.class);

        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                // 1) Read string messages from a Pub/Sub topic.
                .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                // 2) Group the messages into fixed-sized minute intervals.
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))
                // 3) Write one file to GCS for every window of messages.
//                .apply("Write Files to GCS", new WriteOneFilePerWindow(options.getOutput(), numShards));
                .apply("Map Json to Dto", ParDo.of(new DoFn<String, StockData>() {
                            @ProcessElement
                            public void processElement(@Element String json, OutputReceiver<StockData> out) throws IOException {
                                System.out.println(json);

//                                Gson gson = new GsonBuilder().create();
//                                StockData data = gson.fromJson(json, StockData.class);

                                ObjectMapper objectMapper = new ObjectMapper();
                                objectMapper.registerModule(new JavaTimeModule());
                                StockData data = objectMapper.readValue(json, StockData.class);

                                System.out.println(data.getDateTime());
                                System.out.println(data.getOpen());
                                out.output(data);
                            }
                        }))
                .apply(MapElements.into(TypeDescriptor.of(TableRow.class))
                        .via((StockData ele) -> new TableRow()
                                .set("dateTime", ele.getDateTime().format(DateTimeFormatter.ISO_DATE_TIME))
                                .set("open", ele.getOpen())
                                .set("high", ele.getHigh())
                                .set("low", ele.getLow())
                                .set("close", ele.getClose())
                                .set("adjustedClose", ele.getAdjustedClose())
                                .set("volume", ele.getVolume())
                                .set("dividendAmount", ele.getDividendAmount())
                                .set("splitCoefficient", ele.getSplitCoefficient())))
                .apply(BigQueryIO.writeTableRows()
                    .to(options.getTableSpec())
                    .withSchema(stockSchema)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withMethod(STREAMING_INSERTS));

        //Execute the pipeline and wait until it finishes running. (for local testing)
//        pipeline.run().waitUntilFinish();

        //When staging job into gcs, need use this
        pipeline.run();
    }
}