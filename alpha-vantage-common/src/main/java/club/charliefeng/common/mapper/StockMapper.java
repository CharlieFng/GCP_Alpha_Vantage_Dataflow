package club.charliefeng.common.mapper;


import club.charliefeng.common.dto.Stock;
import club.charliefeng.stock.Metadata;
import club.charliefeng.stock.StockRecord;
import org.patriques.output.timeseries.data.StockData;

import java.util.Map;

public class StockMapper {

    public static Stock map(StockData data, Map<String,String> metadata) {
        return new Stock(
                metadata,
                data.getDateTime(),
                data.getOpen(),
                data.getHigh(),
                data.getLow(),
                data.getClose(),
                data.getVolume());
    }

    public static StockRecord mapDaily(Stock pojo) {
        Map<String,String> pojoMeta = pojo.getMetadata();
        Metadata avroMeta = StockMetadataMapper.mapDaily(pojoMeta);
        return new StockRecord(
                avroMeta,
                pojo.getDateTime().toString(),
                pojo.getOpen(),
                pojo.getHigh(),
                pojo.getLow(),
                pojo.getClose(),
                pojo.getVolume());
    }

    public static StockRecord mapIntraday(Stock pojo) {
        Map<String,String> pojoMeta = pojo.getMetadata();
        Metadata avroMeta = StockMetadataMapper.mapIntraday(pojoMeta);
        return new StockRecord(
                avroMeta,
                pojo.getDateTime().toString(),
                pojo.getOpen(),
                pojo.getHigh(),
                pojo.getLow(),
                pojo.getClose(),
                pojo.getVolume());
    }
}
