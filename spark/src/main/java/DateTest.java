import utils.DateTimeUtilsV1;

import java.util.Date;

public class DateTest {
    public static void main(String[] args) {
        Date parse = DateTimeUtilsV1.parse("2018-05-20 14:48:11");
        System.out.println(parse);
    }
}
