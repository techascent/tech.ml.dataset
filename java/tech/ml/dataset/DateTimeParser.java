package tech.ml.dataset;
//Originally from the tablesaw project:
//https://github.com/jtablesaw/tablesaw


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

public class DateTimeParser
{

  public static final DateTimeFormatter dtTimef0 =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"); // 2014-07-09 13:03:44
  public static final DateTimeFormatter dtTimef2 =
      DateTimeFormatter.ofPattern(
          "yyyy-MM-dd HH:mm:ss.S"); // 2014-07-09 13:03:44.7 (as above, but without leading 0 in
  // millis)
  public static final DateTimeFormatter dtTimef4 =
      DateTimeFormatter.ofPattern("dd-MMM-yyyy HH:mm"); // 09-Jul-2014 13:03
  public static final DateTimeFormatter dtTimef5 = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
  public static final DateTimeFormatter dtTimef6; // ISO, with millis appended
  public static final DateTimeFormatter dtTimef7 = //  7/9/14 9:04
      DateTimeFormatter.ofPattern("M/d/yy H:mm");
  public static final DateTimeFormatter dtTimef8 =
      DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a"); //  7/9/2014 9:04:55 PM
  public static final DateTimeFormatter dtTimef9 =
      DateTimeFormatter.ofPattern("M/d/yyyy HH:mm:ss"); //  03/25/2020 01:30:11

  static {
    dtTimef6 =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .appendLiteral('.')
            .appendPattern("SSS")
            .toFormatter();
  }

  // A formatter that handles date time formats defined above
  public static final DateTimeFormatter DEFAULT_FORMATTER =
    new DateTimeFormatterBuilder()
    .appendOptional(dtTimef7)
    .appendOptional(dtTimef8)
    .appendOptional(dtTimef2)
    .appendOptional(dtTimef4)
    .appendOptional(dtTimef0)
    .appendOptional(dtTimef5)
    .appendOptional(dtTimef6)
    .appendOptional(dtTimef9)
    .toFormatter();
}
