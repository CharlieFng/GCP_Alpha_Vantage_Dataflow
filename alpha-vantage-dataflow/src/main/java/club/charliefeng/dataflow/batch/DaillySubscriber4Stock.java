package club.charliefeng.dataflow.batch;

import club.charliefeng.common.dto.Stock;
import club.charliefeng.common.mapper.StockMapper;
import club.charliefeng.common.service.AlphaVantageService;
import club.charliefeng.dataflow.util.StockRecordCoder;
import club.charliefeng.stock.StockRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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
        @Description("The name of the equity of your choice.")
        @Validation.Required
        String getSymbol();
        void setSymbol(String value);

        @Description("The output path of pipeline")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

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

        Daily daily = AlphaVantageService.fetchDailyStock(options.getSymbol());
        Map<String, String> metadata = daily.getMetaData();
        List<Stock> records = daily.getStockData().stream()
                                    .map(data -> StockMapper.map(data, metadata)).collect(Collectors.toList());


        pipeline
                .apply("Fetch Stock Records", Create.of(records)).setCoder(new StockRecordCoder())
                .apply("Map Pojo to Avro", ParDo.of(new PojoToAvroFn()))
                .apply(AvroIO.write(StockRecord.class)
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

    static class PojoToAvroFn extends DoFn<Stock, StockRecord> {
        @ProcessElement
        public void processElement(@Element Stock pojo, OutputReceiver<StockRecord> out) {
            StockRecord avroRecord = StockMapper.mapDaily(pojo);
            LOG.info("Processed avro record: {}", avroRecord);
            out.output(avroRecord);
        }
    }
}
