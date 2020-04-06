package club.charliefeng.appengine.util;

import club.charliefeng.stock.Metadata;

import java.util.Map;

public class StockMetadataMapper {

    public static Metadata mapDaily(Map<String,String> map) {
        Metadata metadata = new Metadata();
        metadata.setInformation(map.get("1. Information"));
        metadata.setSymbol(map.get("2. Symbol"));
        metadata.setLastRefreshed(map.get("3. Last Refreshed"));
        metadata.setOutputSize(map.get("4. Output Size"));
        metadata.setTimeZone(map.get("5. Time Zone"));
        return metadata;
    }

    public static Metadata mapIntraday(Map<String,String> map) {
        Metadata metadata = new Metadata();
        metadata.setInformation(map.get("1. Information"));
        metadata.setSymbol(map.get("2. Symbol"));
        metadata.setLastRefreshed(map.get("3. Last Refreshed"));
        metadata.setInterval(map.get("4. Interval"));
        metadata.setOutputSize(map.get("5. Output Size"));
        metadata.setTimeZone(map.get("6. Time Zone"));
        return metadata;
    }

}
