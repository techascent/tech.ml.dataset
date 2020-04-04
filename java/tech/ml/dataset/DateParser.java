package tech.ml.datatset;
//Originally from the tablesaw project:
//https://github.com/jtablesaw/tablesaw


import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

public class DateParser
{
  
  // Formats that we accept in parsing dates from strings
  public static final DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("yyyyMMdd");
  public static final DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("MM/dd/yyyy");
  public static final DateTimeFormatter dtf3 = DateTimeFormatter.ofPattern("MM-dd-yyyy");
  public static final DateTimeFormatter dtf4 = DateTimeFormatter.ofPattern("MM.dd.yyyy");
  public static final DateTimeFormatter dtf5 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  public static final DateTimeFormatter dtf6 = DateTimeFormatter.ofPattern("yyyy/MM/dd");
  public static final DateTimeFormatter dtf7 = DateTimeFormatter.ofPattern("dd/MMM/yyyy");
  public static final DateTimeFormatter dtf8 = DateTimeFormatter.ofPattern("dd-MMM-yyyy");
  public static final DateTimeFormatter dtf9 = DateTimeFormatter.ofPattern("M/d/yyyy");
  public static final DateTimeFormatter dtf10 = DateTimeFormatter.ofPattern("M/d/yy");
  public static final DateTimeFormatter dtf11 = DateTimeFormatter.ofPattern("MMM/dd/yyyy");
  public static final DateTimeFormatter dtf12 = DateTimeFormatter.ofPattern("MMM-dd-yyyy");
  public static final DateTimeFormatter dtf13 = DateTimeFormatter.ofPattern("MMM/dd/yy");
  public static final DateTimeFormatter dtf14 = DateTimeFormatter.ofPattern("MMM-dd-yy");
  public static final DateTimeFormatter dtf15 = DateTimeFormatter.ofPattern("MMM/dd/yyyy");
  public static final DateTimeFormatter dtf16 = DateTimeFormatter.ofPattern("MMM/d/yyyy");
  public static final DateTimeFormatter dtf17 = DateTimeFormatter.ofPattern("MMM-dd-yy");
  public static final DateTimeFormatter dtf18 = DateTimeFormatter.ofPattern("MMM dd, yyyy");
  public static final DateTimeFormatter dtf19 = DateTimeFormatter.ofPattern("MMM d, yyyy");

  // A formatter that handles all the date formats defined above
  public static final DateTimeFormatter DEFAULT_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendOptional(dtf1)
          .appendOptional(dtf2)
          .appendOptional(dtf3)
          .appendOptional(dtf4)
          .appendOptional(dtf5)
          .appendOptional(dtf6)
          .appendOptional(dtf7)
          .appendOptional(dtf8)
          .appendOptional(dtf9)
          .appendOptional(dtf10)
          .appendOptional(dtf11)
          .appendOptional(dtf12)
          .appendOptional(dtf13)
          .appendOptional(dtf14)
          .appendOptional(dtf15)
          .appendOptional(dtf16)
          .appendOptional(dtf17)
          .appendOptional(dtf18)
          .appendOptional(dtf19)
          .toFormatter();
}
