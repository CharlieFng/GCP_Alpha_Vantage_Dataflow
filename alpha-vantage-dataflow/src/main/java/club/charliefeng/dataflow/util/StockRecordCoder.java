package club.charliefeng.dataflow.util;

import club.charliefeng.dataflow.dto.StockRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class StockRecordCoder extends Coder<StockRecord> {

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public void encode(StockRecord data, OutputStream outStream) throws IOException {
        String serializedStock = objectMapper.writeValueAsString(data);
        outStream.write(serializedStock.getBytes());
    }

    @Override
    public StockRecord decode(InputStream inStream) throws IOException {
        String serializedStock = new String(StreamUtils.getBytesWithoutClosing(inStream));
        StockRecord data = objectMapper.readValue(serializedStock, StockRecord.class);
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
