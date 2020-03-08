package club.charliefeng.dataflow.util;
import com.google.bigtable.v2.Mutation;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


public class BigTableAvroUtils {

    public static Iterable<Mutation> specificRecordToCell(SpecificRecord record){
        ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();
        specificRecordToCell(record, mutations, null);
        return mutations.build();
    }

    private static void specificRecordToCell(SpecificRecord record,ImmutableList.Builder<Mutation> mutations, String parent){
        String columnFamily = parent==null ? record.getSchema().getName() : parent + "-" + record.getSchema().getName();
        for(Schema.Field field: record.getSchema().getFields()){
                if(field.schema().getType() == Schema.Type.RECORD) {
                    specificRecordToCell((SpecificRecord) record.get(field.pos()), mutations, record.getSchema().getName());
                    return;
                }
                Mutation.SetCell setCell = Mutation.SetCell.newBuilder()
                        .setFamilyName(columnFamily)
                        .setColumnQualifier(ByteString.copyFromUtf8(field.name()))
                        .setValue(ByteString.copyFrom(record.get(field.pos()).toString().getBytes()))
                        .build();
                mutations.add(Mutation.newBuilder().setSetCell(setCell).build());
        }
    }
}
