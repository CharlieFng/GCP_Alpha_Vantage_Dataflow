package club.charliefeng.dataflow.util;

import club.charliefeng.common.dto.Stock;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class StockRecordCoder extends Coder<Stock> {

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public void encode(Stock data, OutputStream outStream) throws IOException {
        String serializedStock = objectMapper.writeValueAsString(data);
        outStream.write(serializedStock.getBytes());
    }

    @Override
    public Stock decode(InputStream inStream) throws IOException {
        String serializedStock = new String(StreamUtils.getBytesWithoutClosing(inStream));
        Stock data = objectMapper.readValue(serializedStock, Stock.class);
        return data;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {

    }
}
