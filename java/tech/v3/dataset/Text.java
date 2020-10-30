package tech.v3.dataset;

import java.util.Objects;

public class Text implements Comparable
{
  public final String text;
  public Text(String data) {
    Objects.requireNonNull(data);
    text = data;
  }
  public String toString() { return text; }
  public int hashCode() { return text.hashCode(); }
  public boolean equals(Object other) {
    String strData = null;
    if (other instanceof String) {
      strData = (String)other;
    } else if (other instanceof Text) {
      strData = ((Text)other).text;
    }
    return text == strData;
  }
  public int compareTo(Object other) {
    String strData = null;
    if (other instanceof String) {
      strData = (String)other;
    } else if (other instanceof Text) {
      strData = ((Text)other).text;
    }
    return text.compareTo(strData);
  }
}
