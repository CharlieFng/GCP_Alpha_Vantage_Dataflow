package club.charliefeng.appengine.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

public class AvroCodec {

    private static Logger LOG = LoggerFactory.getLogger(AvroCodec.class);

    public static byte[] toByteArray(GenericRecord record) {
//        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        try {
            writer.write(record, encoder);
            encoder.flush();
            outputStream.close();
        } catch (IOException e) {
            LOG.error("Serialize record into byte array failed: {}", e.getMessage() );
        }
        LOG.debug("Serialize record into byte array completed");
        return outputStream.toByteArray();
    }

    public static byte[] toByteArray(SpecificRecord record) {
        DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        try {
            writer.write(record, encoder);
            encoder.flush();
            outputStream.close();
        } catch (IOException e) {
            LOG.error("Serialize record into byte array failed: {}", e.getMessage() );
        }
        LOG.debug("Serialize record into byte array completed");
        return outputStream.toByteArray();
    }

    public static String toJsonString(GenericRecord record) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), outputStream);
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
            writer.write(record, jsonEncoder);
            jsonEncoder.flush();
            outputStream.flush();
        } catch (IOException e) {
            LOG.error("Serialize record into json failed: {}", e.getMessage());
        }
        LOG.debug("Serialize record into json completed");
        return new String(outputStream.toByteArray(), Charset.forName("ISO8859-1"));
    }

    public static GenericRecord toGenericRecord(Schema schema, byte[] bytes) {
//        DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord record = null;
        try {
            record = reader.read(null, decoder);
        } catch (IOException e) {
            LOG.error("Deserialize record from byte array failed");
        }
        LOG.debug("Deserialize record from byte array completed");
        return record;
    }

    public static SpecificRecord toSpecificRecord(Schema schema, byte[] bytes) {
        DatumReader<SpecificRecord> reader = new SpecificDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        SpecificRecord record = null;
        try {
            record = reader.read(null, decoder);
        } catch (IOException e) {
            LOG.error("Deserialize record from byte array failed");
        }
        LOG.debug("Deserialize record from byte array completed");
        return record;
    }

    public static GenericRecord toGenericRecord(Schema schema, String json) {
        GenericRecord record = null;
        try {
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, json);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            record = datumReader.read(null, jsonDecoder);
        } catch (IOException e) {
            LOG.error("Deserialize record from json failed");
        }
        LOG.debug("Deserialize record from json completed");
        return record;
    }

    private AvroCodec(){}
}
