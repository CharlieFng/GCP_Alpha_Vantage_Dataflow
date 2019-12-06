package club.charliefeng.dataflow.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.LocalDateTime;


@JsonIgnoreProperties(ignoreUnknown = true)
public class StockData implements Serializable {

    private LocalDateTime dateTime;
    private double open;
    private double high;
    private double low;
    private double close;
    private double adjustedClose;
    private long volume;
    private double dividendAmount;
    private double splitCoefficient;

//    public StockData(LocalDateTime dateTime, double open, double high, double low, double close, double adjustedClose, long volume, double dividendAmount, double splitCoefficient) {
//        this.dateTime = dateTime;
//        this.open = open;
//        this.high = high;
//        this.low = low;
//        this.close = close;
//        this.adjustedClose = adjustedClose;
//        this.volume = volume;
//        this.dividendAmount = dividendAmount;
//        this.splitCoefficient = splitCoefficient;
//    }

    @JsonCreator
    public StockData(@JsonProperty("dateTime") LocalDateTime dateTime,
                     @JsonProperty("open") double open,
                     @JsonProperty("high") double high,
                     @JsonProperty("low") double low,
                     @JsonProperty("close") double close,
                     @JsonProperty("adjustedClose") double adjustedClose,
                     @JsonProperty("volume") long volume,
                     @JsonProperty("dividendAmount") double dividendAmount,
                     @JsonProperty("splitCoefficient") double splitCoefficient) {
        this.dateTime = dateTime;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjustedClose = adjustedClose;
        this.volume = volume;
        this.dividendAmount = dividendAmount;
        this.splitCoefficient = splitCoefficient;
    }

//    public StockData(LocalDateTime dateTime, double open, double high, double low, double close, double adjustedClose, long volume, double dividendAmount) {
//        this.dateTime = dateTime;
//        this.open = open;
//        this.high = high;
//        this.low = low;
//        this.close = close;
//        this.adjustedClose = adjustedClose;
//        this.volume = volume;
//        this.dividendAmount = dividendAmount;
//        this.splitCoefficient = 0.0D;
//    }


//    public StockData(LocalDateTime dateTime, double open, double high, double low, double close, long volume) {
//        this.dateTime = dateTime;
//        this.open = open;
//        this.high = high;
//        this.low = low;
//        this.close = close;
//        this.adjustedClose = 0.0D;
//        this.volume = volume;
//        this.dividendAmount = 0.0D;
//        this.splitCoefficient = 0.0D;
//    }

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

    public double getAdjustedClose() {
        return this.adjustedClose;
    }

    public long getVolume() {
        return this.volume;
    }

    public double getDividendAmount() {
        return this.dividendAmount;
    }

    public double getSplitCoefficient() {
        return this.splitCoefficient;
    }
}
