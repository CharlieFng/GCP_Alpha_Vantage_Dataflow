package club.charliefeng.common.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StockData implements Serializable {

    private Map<String,String> metadata;

    private LocalDateTime dateTime;
    private double open;
    private double high;
    private double low;
    private double close;
    private long volume;

    @JsonCreator
    public StockData(@JsonProperty("metadata") Map<String,String> metadata,
                     @JsonProperty("dateTime") LocalDateTime dateTime,
                     @JsonProperty("open") double open,
                     @JsonProperty("high") double high,
                     @JsonProperty("low") double low,
                     @JsonProperty("close") double close,
                     @JsonProperty("volume") long volume) {
        this.metadata = metadata;
        this.dateTime = dateTime;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }

    public Map<String,String> getMetadata() {return this.metadata;}

    public LocalDateTime getDateTime() {
        return this.dateTime;
    }

    public double getOpen() {
        return this.open;
    }

    public double getHigh() {
        return this.high;
    }

    public double getLow() {
        return this.low;
    }

    public double getClose() {
        return this.close;
    }

    public long getVolume() {
        return this.volume;
    }

}
