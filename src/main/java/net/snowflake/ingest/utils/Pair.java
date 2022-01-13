package net.snowflake.ingest.utils;

import java.util.Map;
import java.util.Objects;

/** Simple Pair class. */
public class Pair<F, S> {

  private final F first;

  private final S second;

  public Pair(F first, S second) {
    this.first = first;
    this.second = second;
  }

  public Pair(final Map.Entry<F, S> entry) {
    this.first = entry.getKey();
    this.second = entry.getValue();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }

    if (other == this) {
      return true;
    }

    if (!(Pair.class.isInstance(other))) {
      return false;
    }

    Pair<F, S> pair2 = (Pair<F, S>) other;
    return Objects.equals(this.first, pair2.getFirst())
        && Objects.equals(this.second, pair2.getSecond());
  }

  /** @return the first */
  public F getFirst() {
    return first;
  }

  /** @return the second */
  public S getSecond() {
    return second;
  }

  /** @return the key */
  public F getKey() {
    return getFirst();
  }

  /** @return the value */
  public S getValue() {
    return getSecond();
  }

  @Override
  public int hashCode() {
    return 37 * Objects.hashCode(first) + Objects.hashCode(second);
  }

  @Override
  public String toString() {
    return "[ " + first + ", " + second + " ]";
  }
}
