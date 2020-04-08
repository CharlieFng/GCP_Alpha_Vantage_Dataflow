package club.charliefeng.appengine.controller;

import club.charliefeng.appengine.config.AppConfig;
import club.charliefeng.appengine.util.RestUtils;
import club.charliefeng.common.mapper.StockMapper;
import club.charliefeng.common.service.AlphaVantageService;
import club.charliefeng.common.util.AvroCodec;
import club.charliefeng.stock.StockRecord;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.avro.specific.SpecificRecord;
import org.patriques.output.timeseries.IntraDay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("publish")
public class ProducerController {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerController.class);

    private final PubSubTemplate pubSubTemplate;

    @Autowired
    public ProducerController(PubSubTemplate pubSubTemplate) {
        this.pubSubTemplate = pubSubTemplate;
    }

    @GetMapping(path="/stock/{symbol}/intraday/{interval}", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> fetchIntradayStock(@PathVariable("symbol") String symbol,
                                                     @PathVariable("interval") String interval) {

        IntraDay intraday = AlphaVantageService.fetchIntrayStock(symbol, interval);
        Map<String, String> metadata = intraday.getMetaData();
        List<StockRecord> records = intraday.getStockData().stream()
                .map(data -> StockMapper.map(data, metadata))
                .filter(record -> {
                    ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of("US/Eastern"));
                    LocalDateTime localTime = zonedDateTime.toLocalDateTime();
                    LocalDate localDate = zonedDateTime.toLocalDate();

                    LocalDateTime recordTime = record.getDateTime();
                    // Get data in the window of 4o mins, job get triggered every 30 mins
                    if(recordTime.compareTo(localTime.minusMinutes(40))>0 && recordTime.compareTo(localTime) <=0) {
                        // For development and testing, set date compare == 0, only filter current day data
//                    if(recordTime.toLocalDate().compareTo(localDate)<=0) {
                        return true;
                    }
                    return false; })
                .map(StockMapper::mapIntraday)
                .collect(Collectors.toList());

        for(StockRecord record: records) {
            LOG.info("Stock record is {}", record);
            byte[] bytes = AvroCodec.toByteArray((SpecificRecord) record);
            String messageId = String.format("%s@%s", record.getMetadata().getSymbol(), record.getTimestamp());
            Map<String,String> attributes = new HashMap<>();
            attributes.put("uniqueId", messageId);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                                            .setMessageId(messageId)
                                            .putAllAttributes(attributes)
                                            .setData(ByteString.copyFrom(bytes))
                                            .build();
            pubSubTemplate.publish(AppConfig.stockTopic, pubsubMessage);
        }

        return RestUtils.CREATED(records.toString());

    }

}
