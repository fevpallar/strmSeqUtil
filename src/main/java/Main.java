
import Mapper.Schemas;
import Model.SamplePojo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {

    protected static String getSaltRand() {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder salt = new StringBuilder();
        Random randomStr = new Random();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (randomStr.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }

    public static void main(String args[]) {

        Schemas sch = new Schemas();
        sch.setStructTypeModel("C1", "C2");
        String value1 = getSaltRand();
        String value2 = getSaltRand();
        sch.setRowModel1(value1, value2);
        sch.getDatasetFrameModel1();
        sch.outputModel1();
        //==========================

        List<SamplePojo> list = new ArrayList<>();
        list.add(new SamplePojo(1, getSaltRand()));
        list.add(new SamplePojo(2, getSaltRand()));

        Schemas sch2 = new Schemas();
        sch2.setStructTypeModel("D1", "D2");
        sch2.setRowModel2(list);
        sch2.getDatasetFrameModel2();
        sch2.outputModel2();

    }
}


