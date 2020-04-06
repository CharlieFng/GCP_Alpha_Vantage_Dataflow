package club.charliefeng.appengine.util;


import club.charliefeng.appengine.dto.StockRecord;
import club.charliefeng.stock.Metadata;
import org.patriques.output.timeseries.data.StockData;

import java.util.Map;

public class StockMapper {

    public static StockRecord map(StockData data, Map<String,String> metadata) {
        return new StockRecord(
                metadata,
                data.getDateTime(),
                data.getOpen(),
                data.getHigh(),
                data.getLow(),
                data.getClose(),
                data.getVolume());
    }

    public static club.charliefeng.stock.StockRecord mapDaily(StockRecord pojo) {
        Map<String,String> pojoMeta = pojo.getMetadata();
        Metadata avroMeta = StockMetadataMapper.mapDaily(pojoMeta);
        return new club.charliefeng.stock.StockRecord(
                avroMeta,
                pojo.getDateTime().toString(),
                pojo.getOpen(),
                pojo.getHigh(),
                pojo.getLow(),
                pojo.getClose(),
                pojo.getVolume());
    }

    public static club.charliefeng.stock.StockRecord mapIntraday(StockRecord pojo) {
        Map<String,String> pojoMeta = pojo.getMetadata();
        Metadata avroMeta = StockMetadataMapper.mapIntraday(pojoMeta);
        return new club.charliefeng.stock.StockRecord(
                avroMeta,
                pojo.getDateTime().toString(),
                pojo.getOpen(),
                pojo.getHigh(),
                pojo.getLow(),
                pojo.getClose(),
                pojo.getVolume());
    }
}
