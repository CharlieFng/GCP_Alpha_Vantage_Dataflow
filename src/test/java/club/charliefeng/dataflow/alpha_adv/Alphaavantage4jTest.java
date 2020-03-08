package club.charliefeng.dataflow.alpha_adv;

import org.junit.Test;
import org.patriques.*;
import org.patriques.input.digitalcurrencies.Market;
import org.patriques.input.technicalindicators.Interval;
import org.patriques.input.technicalindicators.SeriesType;
import org.patriques.input.technicalindicators.TimePeriod;
import org.patriques.input.timeseries.OutputSize;
import org.patriques.output.AlphaVantageException;
import org.patriques.output.digitalcurrencies.Daily;
import org.patriques.output.digitalcurrencies.data.DigitalCurrencyData;
import org.patriques.output.exchange.CurrencyExchange;
import org.patriques.output.exchange.data.CurrencyExchangeData;
import org.patriques.output.sectorperformances.Sectors;
import org.patriques.output.sectorperformances.data.SectorData;
import org.patriques.output.technicalindicators.MACD;
import org.patriques.output.technicalindicators.data.MACDData;
import org.patriques.output.timeseries.data.StockData;

import java.util.List;
import java.util.Map;

import static org.patriques.input.timeseries.Interval.ONE_MIN;

public class Alphaavantage4jTest {

    private static final String apiKey = "4VLYMUYXJE1BF1UU";

    @Test
    public void testTimeSeriesIntraday() {
        int timeout = 30000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        TimeSeries stockTimeSeries = new TimeSeries(apiConnector);

        try {
            org.patriques.output.timeseries.IntraDay response = stockTimeSeries.intraDay("MSFT", ONE_MIN, OutputSize.FULL);
            Map<String, String> metaData = response.getMetaData();
            System.out.println("Information: " + metaData.get("1. Information"));
            System.out.println("Stock: " + metaData.get("2. Symbol"));
            System.out.println("Timezone: " + metaData.get("6. Time Zone"));

            List<StockData> stockData = response.getStockData();
            stockData.forEach(stock -> {
                System.out.println("date:   " + stock.getDateTime());
                System.out.println("open:   " + stock.getOpen());
                System.out.println("high:   " + stock.getHigh());
                System.out.println("low:    " + stock.getLow());
                System.out.println("close:  " + stock.getClose());
                System.out.println("volume: " + stock.getVolume());
            });
            System.out.println("Total number of records: " + stockData.size());
        } catch (AlphaVantageException e) {
            System.out.println("something went wrong");
        }
    }

    @Test
    public void testTimeSeriesDaily() {
        int timeout = 30000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        TimeSeries stockTimeSeries = new TimeSeries(apiConnector);

        try {
//            IntraDay response = stockTimeSeries.intraDay("MSFT", ONE_MIN, OutputSize.FULL);
            org.patriques.output.timeseries.Daily response = stockTimeSeries.daily("AAPL", OutputSize.FULL);
            Map<String, String> metaData = response.getMetaData();
            System.out.println("Information: " + metaData.get("1. Information"));
            System.out.println("Stock: " + metaData.get("2. Symbol"));
            System.out.println("Last refreshed: " + metaData.get("3. Last Refreshed"));

            List<StockData> stockData = response.getStockData();
            System.out.println("Total number: " + stockData.size());
            stockData.forEach(stock -> {
                System.out.println("date:   " + stock.getDateTime());
                System.out.println("open:   " + stock.getOpen());
                System.out.println("high:   " + stock.getHigh());
                System.out.println("low:    " + stock.getLow());
                System.out.println("close:  " + stock.getClose());
                System.out.println("volume: " + stock.getVolume());
            });
        } catch (AlphaVantageException e) {
            System.out.println("something went wrong");
        }
    }

    @Test
    public void testForeignExchange() {
        int timeout = 30000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        ForeignExchange foreignExchange = new ForeignExchange(apiConnector);

        try {
            CurrencyExchange currencyExchange = foreignExchange.currencyExchangeRate("USD", "SEK");
            CurrencyExchangeData currencyExchangeData = currencyExchange.getData();

            System.out.println("from currency code: " + currencyExchangeData.getFromCurrencyCode());
            System.out.println("from currency name: " + currencyExchangeData.getFromCurrencyName());
            System.out.println("to currency code:   " + currencyExchangeData.getToCurrencyCode());
            System.out.println("to currency name:   " + currencyExchangeData.getToCurrencyName());
            System.out.println("exchange rate:      " + currencyExchangeData.getExchangeRate());
            System.out.println("last refresh:       " + currencyExchangeData.getTime());
        } catch (AlphaVantageException e) {
            System.out.println("something went wrong"+  e);
        }
    }

    @Test
    public void testCryptoCurrencies() {
        int timeout = 30000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        DigitalCurrencies digitalCurrencies = new DigitalCurrencies(apiConnector);

        try {
            Daily response = digitalCurrencies.daily("BTC", Market.USD);
            Map<String, String> metaData = response.getMetaData();
            System.out.println("Information: " + metaData.get("1. Information"));
            System.out.println("Digital Currency Code: " + metaData.get("2. Digital Currency Code"));

            List<DigitalCurrencyData> digitalData = response.getDigitalData();
            digitalData.forEach(data -> {
                System.out.println("date:       " + data.getDateTime());
                System.out.println("price A:    " + data.getHighA());
                System.out.println("price B:    " + data.getHighB());
                System.out.println("volume:     " + data.getVolume());
                System.out.println("market cap: " + data.getMarketCap());
            });
        } catch (AlphaVantageException e) {
            System.out.println("something went wrong");
        }
    }

    @Test
    public void testTechnicalIndicators() {
        int timeout = 30000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        TechnicalIndicators technicalIndicators = new TechnicalIndicators(apiConnector);

        try {
            MACD response = technicalIndicators.macd("MSFT", Interval.DAILY, TimePeriod.of(10), SeriesType.CLOSE, null, null, null);
            Map<String, String> metaData = response.getMetaData();
            System.out.println("Symbol: " + metaData.get("1: Symbol"));
            System.out.println("Indicator: " + metaData.get("2: Indicator"));

            List<MACDData> macdData = response.getData();
            macdData.forEach(data -> {
                System.out.println("date:           " + data.getDateTime());
                System.out.println("MACD Histogram: " + data.getHist());
                System.out.println("MACD Signal:    " + data.getSignal());
                System.out.println("MACD:           " + data.getMacd());
            });
        } catch (AlphaVantageException e) {
            System.out.println("something went wrong");
        }
    }

    @Test
    public void testSectorPerformances() {
        int timeout = 30000;
        AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, timeout);
        SectorPerformances sectorPerformances = new SectorPerformances(apiConnector);

        try {
            Sectors response = sectorPerformances.sector();
            Map<String, String> metaData = response.getMetaData();
            System.out.println("Information: " + metaData.get("Information"));
            System.out.println("Last Refreshed: " + metaData.get("Last Refreshed"));

            List<SectorData> sectors = response.getSectors();
            sectors.forEach(data -> {
                System.out.println("key:           " + data.getKey());
                System.out.println("Consumer Discretionary: " + data.getConsumerDiscretionary());
                System.out.println("Consumer Staples:       " + data.getConsumerStaples());
                System.out.println("Energy:                 " + data.getEnergy());
                System.out.println("Financials:             " + data.getFinancials());
                System.out.println("Health Care:            " + data.getHealthCare());
                System.out.println("Industrials:            " + data.getIndustrials());
                System.out.println("Information Technology: " + data.getInformationTechnology());
                System.out.println("Materials:              " + data.getMaterials());
                System.out.println("Real Estate:            " + data.getRealEstate());
            });
        } catch (AlphaVantageException e) {
            System.out.println("something went wrong");
        }
    }
}
