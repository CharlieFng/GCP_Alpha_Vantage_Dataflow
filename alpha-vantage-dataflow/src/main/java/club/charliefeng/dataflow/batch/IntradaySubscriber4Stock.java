package club.charliefeng.dataflow.batch;

import club.charliefeng.dataflow.dto.StockRecord;
import club.charliefeng.dataflow.util.AvroCodec;
import club.charliefeng.dataflow.util.StockMapper;
import club.charliefeng.dataflow.util.StockRecordCoder;
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
import org.patriques.AlphaVantageConnector;
import org.patriques.TimeSeries;
import org.patriques.input.timeseries.Interval;
import org.patriques.input.timeseries.OutputSize;
import org.patriques.output.AlphaVantageException;
import org.patriques.output.timeseries.IntraDay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.patriques.input.timeseries.Interval.*;

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

    private static IntraDay fetchIntrayStock(String symbol, String value) {
        int timeout = 30000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        TimeSeries stockTimeSeries = new TimeSeries(apiConnector);
        IntraDay response = null;
        Interval interval;
        switch (value) {
            case "5min": interval = FIVE_MIN; break;
            case "10min": interval = TEN_MIN; break;
            case "15min": interval = FIFTEEN_MIN; break;
            case "30min": interval = THIRTY_MIN; break;
            case "60min": interval = SIXTY_MIN; break;
            default: interval = ONE_MIN;
        }

        while(response==null){
            try {
                response = stockTimeSeries.intraDay(symbol, interval, OutputSize.COMPACT);
            } catch (AlphaVantageException e) {
                LOG.error("Fetch intraday stock records failed due to {}, will retry", e.getMessage());
            }
        }
        return response;
    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);


        Pipeline pipeline = Pipeline.create(options);

        IntraDay intraday = fetchIntrayStock(options.getSymbol(), options.getInterval());
        Map<String, String> metadata = intraday.getMetaData();
        List<StockRecord> records = intraday.getStockData().stream()
                                    .map(data -> StockMapper.map(data, metadata)).collect(Collectors.toList());


        pipeline
                .apply("Fetch Stock Records", Create.of(records)).setCoder(new StockRecordCoder())
                .apply("Filter Current Day Only", ParDo.of(new FilterCurrentDay()))
                .apply("Map Pojo to Avro", ParDo.of(new PojoToAvroFn()))
                .apply("Map Avro to Pubsub message", ParDo.of(new AvroToPubsubMsg()))
                .apply("Publish to Pubsub", PubsubIO.writeMessages()
                                                          .withIdAttribute("uniqueId")
                                                          .to(options.getOutputTopic()));
//                .apply("Publish to Pubsub", PubsubIO.writeAvros(club.charliefeng.stock.StockRecord.class)
//                                                          .withIdAttribute("timestamp")
//                                                          .to(options.getOutputTopic()));


        //Execute the pipeline and wait until it finishes running. (for local testing)
        pipeline.run().waitUntilFinish();

        //When staging job into gcs, need use this
//        pipeline.run();
    }

    static class PojoToAvroFn extends DoFn<StockRecord, club.charliefeng.stock.StockRecord> {
        @ProcessElement
        public void processElement(@Element StockRecord pojo, OutputReceiver<club.charliefeng.stock.StockRecord> out) {
            club.charliefeng.stock.StockRecord avroRecord = StockMapper.mapIntraday(pojo);
//            LOG.info("Processed avro record: {}", avroRecord);
            out.output(avroRecord);
        }
    }

    static class FilterCurrentDay extends DoFn<StockRecord, StockRecord> {
        @ProcessElement
        public void processElement(@Element StockRecord in, OutputReceiver<StockRecord> out) {
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

    static class AvroToPubsubMsg extends DoFn<club.charliefeng.stock.StockRecord, PubsubMessage> {
        @ProcessElement
        public void processElement(@Element club.charliefeng.stock.StockRecord in, OutputReceiver<PubsubMessage> out) {
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
