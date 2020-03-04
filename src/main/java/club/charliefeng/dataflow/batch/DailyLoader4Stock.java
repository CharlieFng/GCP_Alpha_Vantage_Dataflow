package club.charliefeng.dataflow.batch;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DailyLoader4Stock {

    private static Logger LOG = LoggerFactory.getLogger(DailyLoader4Stock.class);

    public interface MyOptions extends PipelineOptions {

        @Description("The symbol of stock")
        @Validation.Required
        String getSymbol();
        void setSymbol(String value);


        @Description("The input file of pipeline")
        @Validation.Required
        String getInput();
        void setInput(String value);


        @Description("The output path of pipeline")
        @Validation.Required
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) throws IOException {

        ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of("US/Eastern"));
        LocalDate localDate = zonedDateTime.toLocalDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formatLocalDate = localDate.format(formatter);
        LOG.info("Current date of US/Eastern zone is: {}", formatLocalDate);

        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        String DEFAULT_SCHEMA_PATH = "avro/StockRecord.avsc";

        ClassLoader classLoader = DailyLoader4Stock.class.getClassLoader();
        File file = new File(classLoader.getResource(DEFAULT_SCHEMA_PATH).getFile());
        InputStream inputStream = new FileInputStream(file);

        Schema schema = new Schema.Parser().parse(inputStream);
        System.out.println("Avro schema is: " + schema);
        pipeline
                .apply("Read Avro from GCS",
                        AvroIO.readGenericRecords(schema).from(
                            String.format("%s/%s/%s-*.avro",
                                options.getInput(),
                                formatLocalDate,
                                options.getSymbol()
                            ))
                        )
                .apply("Write Parquet to GCS",
                        FileIO.<GenericRecord>write()
                            .via(ParquetIO.sink(schema)
                                .withCompressionCodec(CompressionCodecName.SNAPPY))
                            .to(String.format("%s/%s/parquet/",
                                    options.getOutput(),
                                    formatLocalDate))
                            .withSuffix(".parquet")
                       );

        //Execute the pipeline and wait until it finishes running. (for local testing)
//        pipeline.run().waitUntilFinish();

        //When staging job into gcs, need use this
        pipeline.run();
    }
}
