package club.charliefeng.dataflow.batch;

import club.charliefeng.dataflow.dto.StockRecord;
import club.charliefeng.dataflow.util.StockMapper;
import club.charliefeng.dataflow.util.StockRecordCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.patriques.AlphaVantageConnector;
import org.patriques.TimeSeries;
import org.patriques.input.timeseries.OutputSize;
import org.patriques.output.AlphaVantageException;
import org.patriques.output.timeseries.IntraDay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.patriques.input.timeseries.Interval.ONE_MIN;

public class IntradaySubscriber4Stock {

    private static final String apiKey = "4VLYMUYXJE1BF1UU";
    private static Logger LOG = LoggerFactory.getLogger(IntradaySubscriber4Stock.class);

    public interface MyOptions extends PipelineOptions {
        @Description("The symbol of stock")
        @Validation.Required
        String getSymbol();
        void setSymbol(String value);

        @Description("The output topic of pubsub")
        @Validation.Required
        String getOutputTopic();
        void setOutputTopic(String value);

    }

    private static IntraDay fetchIntrayStock(String symbol) {
        int timeout = 30000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        TimeSeries stockTimeSeries = new TimeSeries(apiConnector);
        IntraDay response = null;

        while(response==null){
            try {
                response = stockTimeSeries.intraDay(symbol, ONE_MIN, OutputSize.FULL);
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

        IntraDay intraday = fetchIntrayStock(options.getSymbol());
        Map<String, String> metadata = intraday.getMetaData();
        List<StockRecord> records = intraday.getStockData().stream()
                                    .map(data -> StockMapper.map(data, metadata)).collect(Collectors.toList());


        pipeline
                .apply("Fetch Stock Records", Create.of(records)).setCoder(new StockRecordCoder())
                .apply("Filter Current Day Only", ParDo.of(new FilterCurrentDay()))
                .apply("Map Pojo to Avro", ParDo.of(new PojoToAvroFn()))
                .apply("Publish to Pubsub", PubsubIO.writeAvros(club.charliefeng.stock.StockRecord.class)
                                                          .to(options.getOutputTopic()));


        //Execute the pipeline and wait until it finishes running. (for local testing)
//        pipeline.run().waitUntilFinish();

        //When staging job into gcs, need use this
        pipeline.run();
    }

    static class PojoToAvroFn extends DoFn<StockRecord, club.charliefeng.stock.StockRecord> {
        @ProcessElement
        public void processElement(@Element StockRecord pojo, OutputReceiver<club.charliefeng.stock.StockRecord> out) {
            club.charliefeng.stock.StockRecord avroRecord = StockMapper.mapIntraday(pojo);
            System.out.println(avroRecord);
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
            // Production should set compare == 0, only filter current day data
            if(recordTime.toLocalDate().compareTo(localDate)<=0) {
                out.output(in);
                LOG.info("Current US/Eastern local time is: {}, record date time is{}", localTime, recordTime);
                LOG.info("Record in local date {} has been added", localDate);
            }
        }
    }
}
