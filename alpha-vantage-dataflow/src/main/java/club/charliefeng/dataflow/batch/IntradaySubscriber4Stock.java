package club.charliefeng.dataflow.batch;

import club.charliefeng.common.dto.Stock;
import club.charliefeng.common.mapper.StockMapper;
import club.charliefeng.common.service.AlphaVantageService;
import club.charliefeng.common.util.AvroCodec;
import club.charliefeng.dataflow.util.StockRecordCoder;
import club.charliefeng.stock.StockRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.patriques.output.timeseries.IntraDay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IntradaySubscriber4Stock {

    private static final String apiKey = "4VLYMUYXJE1BF1UU";
    private static Logger LOG = LoggerFactory.getLogger(IntradaySubscriber4Stock.class);

    public interface MyOptions extends PipelineOptions {
        @Description("The symbol of stock")
        @Validation.Required
        String getSymbol();
        void setSymbol(String value);

        @Description("Time interval between two consecutive data points")
        @Validation.Required
        String getInterval();
        void setInterval(String interval);

        @Description("The output topic of pubsub")
        @Validation.Required
        String getOutputTopic();
        void setOutputTopic(String value);

    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);


        Pipeline pipeline = Pipeline.create(options);

        IntraDay intraday = AlphaVantageService.fetchIntrayStock(options.getSymbol(), options.getInterval());
        Map<String, String> metadata = intraday.getMetaData();
        List<Stock> records = intraday.getStockData().stream()
                                    .map(data -> StockMapper.map(data, metadata)).collect(Collectors.toList());


        pipeline
                .apply("Fetch Stock Records", Create.of(records)).setCoder(new StockRecordCoder())
                .apply("Filter Current Day Only", ParDo.of(new FilterCurrentDay()))
                .apply("Map Pojo to Avro", ParDo.of(new PojoToAvroFn()))
                .apply("Map Avro to Pubsub message", ParDo.of(new AvroToPubsubMsg()))
                .apply("Publish to Pubsub", PubsubIO.writeMessages()
                                                          .withIdAttribute("uniqueId")
                                                          .to(options.getOutputTopic()));
//                .apply("Publish to Pubsub", PubsubIO.writeAvros(Stock.class)
//                                                          .withIdAttribute("timestamp")
//                                                          .to(options.getOutputTopic()));


        //Execute the pipeline and wait until it finishes running. (for local testing)
        pipeline.run().waitUntilFinish();

        //When staging job into gcs, need use this
//        pipeline.run();
    }

    static class PojoToAvroFn extends DoFn<Stock, StockRecord> {
        @ProcessElement
        public void processElement(@Element Stock pojo, OutputReceiver<StockRecord> out) {
            StockRecord avroRecord = StockMapper.mapIntraday(pojo);
//            LOG.info("Processed avro record: {}", avroRecord);
            out.output(avroRecord);
        }
    }

    static class FilterCurrentDay extends DoFn<Stock, Stock> {
        @ProcessElement
        public void processElement(@Element Stock in, OutputReceiver<Stock> out) {
            ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of("US/Eastern"));
            LocalDateTime localTime = zonedDateTime.toLocalDateTime();
            LocalDate localDate = zonedDateTime.toLocalDate();

            LocalDateTime recordTime = in.getDateTime();
            // Get data in the window of 4o mins, job get triggered every 30 mins
            if(recordTime.compareTo(localTime.minusMinutes(40))>0 && recordTime.compareTo(localTime) <=0) {
            // For development and testing, set date compare == 0, only filter current day data
//            if(recordTime.toLocalDate().compareTo(localDate)<=0) {
                out.output(in);
                LOG.info("Current US/Eastern local time is: {}, record date time is{}", localTime, recordTime);
                LOG.info("Record in local date {} has been added", localDate);
            }
        }
    }

    static class AvroToPubsubMsg extends DoFn<StockRecord, PubsubMessage> {
        @ProcessElement
        public void processElement(@Element StockRecord in, OutputReceiver<PubsubMessage> out) {
            byte[] bytes = AvroCodec.toByteArray((SpecificRecord) in);
            String messageId = String.format("%s@%s",in.getMetadata().getSymbol(), in.getTimestamp());
            Map<String,String> attributes = new HashMap<>();
            attributes.put("uniqueId", messageId);
            PubsubMessage message = new PubsubMessage(bytes,attributes,messageId);
            LOG.info("Build pubsub message with id: {}", messageId);
            out.output(message);
        }
    }
}
