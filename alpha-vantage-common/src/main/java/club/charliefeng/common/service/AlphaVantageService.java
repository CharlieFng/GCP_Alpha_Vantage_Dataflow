package club.charliefeng.appengine.service;

import org.patriques.AlphaVantageConnector;
import org.patriques.TimeSeries;
import org.patriques.input.timeseries.Interval;
import org.patriques.input.timeseries.OutputSize;
import org.patriques.output.AlphaVantageException;
import org.patriques.output.timeseries.IntraDay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static org.patriques.input.timeseries.Interval.*;
import static org.patriques.input.timeseries.Interval.ONE_MIN;

@Service
public class AlphaVantageService {

    private static Logger LOG = LoggerFactory.getLogger(AlphaVantageService.class);
    private static final String apiKey = "4VLYMUYXJE1BF1UU";
    private static final AlphaVantageConnector apiConnector = new AlphaVantageConnector(apiKey, 30000);

    public static IntraDay fetchIntrayStock(String symbol, String value) {
        TimeSeries stockTimeSeries = new TimeSeries(apiConnector);
        IntraDay response = null;
        Interval interval;
        switch (value) {
            case "5min":
                interval = FIVE_MIN;
                break;
            case "10min":
                interval = TEN_MIN;
                break;
            case "15min":
                interval = FIFTEEN_MIN;
                break;
            case "30min":
                interval = THIRTY_MIN;
                break;
            case "60min":
                interval = SIXTY_MIN;
                break;
            default:
                interval = ONE_MIN;
        }

        while (response == null) {
            try {
                response = stockTimeSeries.intraDay(symbol, interval, OutputSize.COMPACT);
            } catch (AlphaVantageException e) {
                LOG.error("Fetch intraday stock records failed due to {}, will retry", e.getMessage());
            }
        }
        return response;
    }

}
