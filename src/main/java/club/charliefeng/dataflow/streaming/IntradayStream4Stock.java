package club.charliefeng.dataflow.streaming;

import club.charliefeng.dataflow.util.AvroCodec;
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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.STREAMING_INSERTS;

@SuppressWarnings("unchecked")
public class IntradayStream4Stock {

    private static Logger LOG = LoggerFactory.getLogger(IntradayStream4Stock.class);

    public interface MyOptions extends PipelineOptions, StreamingOptions {
        @Description("GCP Project ID")
        @Validation.Required
        String getProjectId();
        void setProjectId(String value);

        @Description("Stock names")
        @Validation.Required
        String getStocks();
        void setStocks(String value);

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
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withFormatFunction(TABLE_ROW_PARSER)
                .withMethod(STREAMING_INSERTS);


        // Need to creat column families [StockRecord, StockRecord-Metadata] ahead using cbt cli tool.
        BigtableIO.Write btWrite = BigtableIO.write()
                .withProjectId(options.getProjectId())
                .withInstanceId(options.getBtInstanceId())
                .withTableId(options.getBtTableId());


        String[] stockSymbols = options.getStocks().split(",");
        Map<String, TupleTag<StockRecord>> stockTags = new HashMap<>(stockSymbols.length);
        for (String symbol : stockSymbols) {
            stockTags.put(symbol, new TupleTag<StockRecord>(){});
        }

        Map<String, TupleTag<StockRecord>> copy = new HashMap(stockTags);
        TupleTag<StockRecord> mainTag = copy.remove(stockSymbols[0]);
        List<TupleTag<StockRecord>> restTags = new ArrayList(copy.values());
        TupleTagList rest = TupleTagList.empty();
        for(TupleTag<StockRecord> tag: restTags) {
            rest = rest.and(tag);
        }

        PCollection<StockRecord> stockBundles = pipeline
//                .apply("Read PubSub Messages", PubsubIO.readAvros(StockRecord.class).fromTopic(options.getInputTopic()))
                .apply("Read Pubsub Messages", PubsubIO.readMessagesWithAttributesAndMessageId()
                                                             .fromTopic(options.getInputTopic())
                                                             .withIdAttribute("uniqueId"))
                .apply("Split into fixed window", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))
                .apply("Deserialize bytes into Avro", ParDo.of(new PubsubMsgToAvro()));

        stockBundles.apply("Transform Avro to BigTable Cell", MapElements.via(new AvroToBigtableFn()))
                .apply("Write to BigTable", btWrite);

        PCollectionTuple brunchedRes = stockBundles.apply("Brunch into multiple collections",
                    ParDo.of(new DoFn<StockRecord, StockRecord>() {
                        @ProcessElement
                        public void processElement(@Element StockRecord record, MultiOutputReceiver out) {
                            String symbol = record.getMetadata().getSymbol();
                            if(symbol.equals(stockSymbols[0])) {
                                out.get(stockTags.get(stockSymbols[0])).output(record);
                            }else {
                                for (String stock : stockSymbols) {
                                    if (symbol.equals(stock)) {
                                        TupleTag<StockRecord> tag = stockTags.get(stock);
                                        out.get(tag).output(record);
                                    }
                                }
                            }
                        }
                    }).withOutputTags(mainTag,rest));


        for(Map.Entry<String, TupleTag<StockRecord>> item : stockTags.entrySet()) {
            brunchedRes.get(item.getValue()).apply(String.format("Write to BigQuery for %s", item.getKey()),
                        BigQueryIO.write()
                            .to(String.format("%s_%s",options.getBqTableSpec(),item.getKey()))
                            .withSchema(stockSchema)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                            .withFormatFunction(TABLE_ROW_PARSER)
                            .withMethod(STREAMING_INSERTS));
        }

//        stockBundles.apply("Write to BigQuery", bqWrite);

//        Execute the pipeline and wait until it finishes running. (for local testing)
//        pipeline.run().waitUntilFinish();

        //When staging job into gcs, need use this
        pipeline.run();
    }

    private static final SerializableFunction TABLE_ROW_PARSER =
            new SerializableFunction<SpecificRecord, TableRow>() {
                @Override
                public TableRow apply(SpecificRecord specificRecord) {
                    TableRow row = BigQueryAvroUtils.convertSpecificRecordToTableRow(
                            specificRecord, BigQueryAvroUtils.getTableSchema(specificRecord.getSchema()));
                    return row;

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

    static class PubsubMsgToAvro extends DoFn<PubsubMessage, StockRecord> {
        @ProcessElement
        public void processElement(@Element PubsubMessage message, OutputReceiver<StockRecord> out) {
            String messageId = message.getMessageId();
            String uniqueId = message.getAttribute("uniqueId");
            LOG.info("Get pubsub message with id: {} and uniqueId: {}", messageId, uniqueId);
            byte[] bytes = message.getPayload();
            StockRecord record = (StockRecord) AvroCodec.toSpecificRecord(StockRecord.SCHEMA$,bytes);
            LOG.info("Build Avro record: {}", record);
            out.output(record);
        }
    }

}
