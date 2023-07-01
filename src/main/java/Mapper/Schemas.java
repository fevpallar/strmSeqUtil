package Mapper;

import Model.SamplePojo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Schemas<T,E> {
    private StructType structType;
    private List<Row> rows1, rows2;
    private SparkSession spark = SparkSession.builder().appName("Build A dataframe").master("local[*]").getOrCreate();

    private String field1, field2;
    private T val1;
    private E val2;

    private List<SamplePojo> list = new ArrayList<>();
    public StructType getStructType() {
        return structType;
    }

    public void setStructTypeModel(String field1, String field2) {
        this.field1 = field1;
        this.field2 = field2;
        // StructType reresents SCHEMA of a DataFrame.
        this.structType = new StructType();
        structType = structType.add(field1, DataTypes.StringType, false);
        structType = structType.add(field2, DataTypes.StringType, false);
    }

    public List<Row> setRowModel1(T val1, E val2) {
        this.val1 = val1;
        this.val2 = val2;
        rows1 = new ArrayList<Row>();
        rows1.add(RowFactory.create(val1, val2));
        return rows1;
    }

    public List<Row> setRowModel2(List<SamplePojo> list) {
        rows2 = new ArrayList<Row>();
        list.stream()
                .map(c -> {
                    try {
                        rows2.add(new PojoToRowMapper().call(c));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }).collect(Collectors.toList());
        System.out.println(rows2);
        return rows2;
    }

    //use Dataset<Row> to represent a DataFrame.

    public Dataset<Row> getDatasetFrameModel1() {
        // Essentially, a Row uses efficient storage called Tungsten, which highly optimizes Spark operations in comparison with its predecessors.
        // jadi nanti modelnya itu kayak columns (A and B) , row 0 (value1 dan value2)
        return spark.createDataFrame(this.rows1, structType);
    }
    //use Dataset<Row> to represent a DataFrame.
    public Dataset<Row> getDatasetFrameModel2() {
        // Essentially, a Row uses efficient storage called Tungsten, which highly optimizes Spark operations in comparison with its predecessors.
        // jadi nanti modelnya itu kayak columns (A and B) , row 0 (value1 dan value2)
        return spark.createDataFrame(this.rows2, structType);
    }

    public void outputModel1() {
        Row[] rowsToOuput1 = (Row[]) getDatasetFrameModel1().collect();

        for (Row row : rowsToOuput1) {
            // Access the values of each row based on column index or column name
            String valueA = row.getString(0); // Access value of column "field1" by index
            String valueB = row.getString(row.fieldIndex(field2)); // Access value of column "field2" by name

            // Output the values
            System.out.println("Value : " + field1 + " is " + val1);
            System.out.println("Value : " + field2 + " is " + val2);
        }
    }

    public void outputModel2() {
        Row[] rowsToOuput2 = (Row[]) getDatasetFrameModel2().collect();

        for (Row row : rowsToOuput2) {
            // Access the values of each row based on column index or column name
            String valueA = row.getString(0); // Access value of column "field1" by index
            String valueB = row.getString(row.fieldIndex(field2)); // Access value of column "field2" by name

            // Output the values
            System.out.println("Value : " + field1 + " is " + valueA);
            System.out.println("Value : " + field2 + " is " + valueB);
        }
    }



}
