package club.charliefeng.dataflow.batch;

import club.charliefeng.dataflow.dto.StockRecord;
import club.charliefeng.dataflow.util.StockMapper;
import club.charliefeng.dataflow.util.StockRecordCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.patriques.AlphaVantageConnector;
import org.patriques.TimeSeries;
import org.patriques.input.timeseries.OutputSize;
import org.patriques.output.AlphaVantageException;
import org.patriques.output.timeseries.Daily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DaillySubscriber4Stock {

    private static final String apiKey = "4VLYMUYXJE1BF1UU";
    private static Logger LOG = LoggerFactory.getLogger(DaillySubscriber4Stock.class);

    public interface MyOptions extends PipelineOptions {
        @Description("The symbol of stock")
        @Validation.Required
        String getSymbol();
        void setSymbol(String value);

        @Description("The output path of pipeline")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

    }

    private static Daily fetchDailyStock(String symbol) {
        int timeout = 30000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        TimeSeries stockTimeSeries = new TimeSeries(apiConnector);
        Daily response = null;

        while(response==null){
            try {
                response = stockTimeSeries.daily(symbol, OutputSize.FULL);
            } catch (AlphaVantageException e) {
                LOG.error("Fetch daily stock records failed due to {}, will retry", e.getMessage());
            }
        }
        return response;
    }

    public static void main(String[] args) {

        ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of("US/Eastern"));
        LocalDate localDate = zonedDateTime.toLocalDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formatLocalDate = localDate.format(formatter);
        LOG.info("Current date of US/Eastern zone is: {}", formatLocalDate);

        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        LOG.info("Symbol is {}, output is {}", options.getSymbol(), options.getOutput());

        Pipeline pipeline = Pipeline.create(options);

        Daily daily = fetchDailyStock(options.getSymbol());
        Map<String, String> metadata = daily.getMetaData();
        List<StockRecord> records = daily.getStockData().stream()
                                    .map(data -> StockMapper.map(data, metadata)).collect(Collectors.toList());


        pipeline
                .apply("Fetch Stock Records", Create.of(records)).setCoder(new StockRecordCoder())
                .apply("Map Pojo to Avro", ParDo.of(new PojoToAvroFn()))
                .apply(AvroIO.write(club.charliefeng.stock.StockRecord.class)
                        .to(String.format("%s/%s/%s-",
                                options.getOutput(),
                                formatLocalDate,
                                options.getSymbol()))
                        .withSuffix(".avro")
                        .withNumShards(1)
                );


        //Execute the pipeline and wait until it finishes running. (for local testing)
        pipeline.run().waitUntilFinish();

        //When staging job into gcs, need use this
//        pipeline.run();
    }

    static class PojoToAvroFn extends DoFn<StockRecord, club.charliefeng.stock.StockRecord> {
        @ProcessElement
        public void processElement(@Element StockRecord pojo, OutputReceiver<club.charliefeng.stock.StockRecord> out) {
            club.charliefeng.stock.StockRecord avroRecord = StockMapper.mapDaily(pojo);
            LOG.info("Processed avro record: {}", avroRecord);
            out.output(avroRecord);
        }
    }
}
