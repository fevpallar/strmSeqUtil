package Mapper;

import Model.SamplePojo;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class SamplePojoToRowMapper implements MapFunction<SamplePojo, Row> {
    // DataFrame is essentially a Dataset<Row>, let's see how we can create a Row from a Customer POJO.
    @Override
    public Row call(SamplePojo samplePojo) throws Exception {
        Row row = RowFactory.create(
                samplePojo.getId().toString(),
                samplePojo.getName().toUpperCase()
        );
        return row;
    }
}