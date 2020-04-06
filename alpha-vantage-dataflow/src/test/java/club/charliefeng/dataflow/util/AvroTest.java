package club.charliefeng.dataflow.util;

import club.charliefeng.stock.Metadata;
import club.charliefeng.stock.StockRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

public class AvroTest {

    @Test
    public void specificToGenericViaByteArray(){
        Metadata metadata = new Metadata("TEST","TEST","TEST","TEST","TEST","TEST");
        SpecificRecord specificRecord = new StockRecord(metadata,"TEST",1.0,1.0,1.0,1.0,100l);
        byte[] bytes = AvroCodec.toByteArray(specificRecord);

        GenericRecord genericRecord = AvroCodec.toGenericRecord(StockRecord.getClassSchema(), bytes);
        System.out.println(genericRecord);


    }
}
