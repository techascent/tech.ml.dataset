package tech.ml.dataset;
//Originally from the tablesaw project:
//https://github.com/jtablesaw/tablesaw

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

public class TimeParser
{

  public static final DateTimeFormatter timef1 = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
  public static final DateTimeFormatter timef2 = DateTimeFormatter.ofPattern("hh:mm:ss a");
  public static final DateTimeFormatter timef3 = DateTimeFormatter.ofPattern("h:mm:ss a");
  public static final DateTimeFormatter timef4 = DateTimeFormatter.ISO_LOCAL_TIME;
  public static final DateTimeFormatter timef5 = DateTimeFormatter.ofPattern("hh:mm a");
  public static final DateTimeFormatter timef6 = DateTimeFormatter.ofPattern("h:mm a");

  // only for parsing:
  public static final DateTimeFormatter timef7 = DateTimeFormatter.ofPattern("HHmm");

  // A formatter that handles time formats defined above used for type detection.
  // It is more conservative than the converter
  public static final DateTimeFormatter TIME_DETECTION_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendOptional(timef5)
          .appendOptional(timef2)
          .appendOptional(timef3)
          .appendOptional(timef1)
          .appendOptional(timef4)
          .appendOptional(timef6)
          .toFormatter();

  // A formatter that handles time formats defined above
  /**
   * A formatter for parsing. Useful when the user has specified that a numeric-like column is
   * really supposed to be a time See timef7 definition
   */
  public static final DateTimeFormatter TIME_CONVERSION_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendOptional(timef5)
          .appendOptional(timef2)
          .appendOptional(timef3)
          .appendOptional(timef1)
          .appendOptional(timef4)
          .appendOptional(timef6)
          .appendOptional(timef7)
          .toFormatter();

  public static final DateTimeFormatter DEFAULT_FORMATTER = TIME_DETECTION_FORMATTER;

}
