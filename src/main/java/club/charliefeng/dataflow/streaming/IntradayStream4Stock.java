package club.charliefeng.dataflow.streaming;

import club.charliefeng.dataflow.util.BigQueryAvroUtils;
import club.charliefeng.dataflow.util.BigTableAvroUtils;
import club.charliefeng.stock.StockRecord;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.IOException;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.STREAMING_INSERTS;

@SuppressWarnings("unchecked")
public class IntradayStream4Stock {

    public interface MyOptions extends PipelineOptions, StreamingOptions {
        @Description("GCP Project ID")
        @Validation.Required
        String getProjectId();
        void setProjectId(String value);

        @Description("Pub/Sub topic to read from.")
        @Validation.Required
        String getInputTopic();
        void setInputTopic(String value);

        @Description("Processing window size in number of minutes.")
        @Default.Integer(5)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("BigQuery table to write to")
        @Validation.Required
        String getBqTableSpec();
        void setBqTableSpec(String value);

        @Description("BigTable instance ID")
        @Validation.Required
        String getBtInstanceId();
        void setBtInstanceId(String value);

        @Description("BigTable table ID")
        @Validation.Required
        String getBtTableId();
        void setBtTableId(String value);
    }

    public static void main(String[] args) throws IOException {

        TableSchema stockSchema = BigQueryAvroUtils.getTableSchema(StockRecord.SCHEMA$);

        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        BigQueryIO.Write bqWrite = BigQueryIO.write()
                .to(options.getBqTableSpec())
                .withSchema(stockSchema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withFormatFunction(TABLE_ROW_PARSER)
                .withMethod(STREAMING_INSERTS);

        BigtableIO.Write btWrite = BigtableIO.write()
                .withProjectId(options.getProjectId())
                .withInstanceId(options.getBtInstanceId())
                .withTableId(options.getBtTableId());





        PCollection<StockRecord> stockBundles = pipeline
                .apply("Read PubSub Messages", PubsubIO.readAvros(StockRecord.class).fromTopic(options.getInputTopic()))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))));

        stockBundles.apply("Write to BigQuery", bqWrite);

        stockBundles.apply("Transform Avro to BigTable Cell", MapElements.via(new AvroToBigtableFn()))
                    .apply("Write to BigTable", btWrite);

        //Execute the pipeline and wait until it finishes running. (for local testing)
//        pipeline.run().waitUntilFinish();

        //When staging job into gcs, need use this
        pipeline.run();
    }

    private static final SerializableFunction TABLE_ROW_PARSER =
            new SerializableFunction<SpecificRecord, TableRow>() {
                @Override
                public TableRow apply(SpecificRecord specificRecord) {
                    return BigQueryAvroUtils.convertSpecificRecordToTableRow(
                            specificRecord, BigQueryAvroUtils.getTableSchema(specificRecord.getSchema()));
                }
            };


    static class AvroToBigtableFn extends SimpleFunction<StockRecord,KV<ByteString, Iterable<Mutation>>> {

        @Override
        public KV<ByteString, Iterable<Mutation>> apply(StockRecord record) {
            ByteString rowKey = ByteString
                    .copyFromUtf8(record.getTimestamp() + "-" + record.getMetadata().getSymbol());

            Iterable<Mutation> iterator = BigTableAvroUtils.specificRecordToCell(record);
            return KV.of(rowKey, iterator);
        }
    }
}
