/*
 * Replicated from snowflake-jdbc: net.snowflake.client.util.SFPair
 * Tag: v3.25.1
 */

package net.snowflake.ingest.utils;

import java.util.Objects;

public class SFPair<L, R> {
  public L left;
  public R right;

  public static <L, R> SFPair<L, R> of(L l, R r) {
    return new SFPair<>(l, r);
  }

  private SFPair(L left, R right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other == this) {
      return true;
    }
    if (!SFPair.class.isInstance(other)) {
      return false;
    }
    SFPair<?, ?> pair = (SFPair<?, ?>) other;
    return Objects.equals(this.left, pair.left) && Objects.equals(this.right, pair.right);
  }

  @Override
  public int hashCode() {
    int result = 0;
    if (this.left != null) {
      result += 37 * this.left.hashCode();
    }
    if (this.right != null) {
      result += this.right.hashCode();
    }
    return result;
  }

  @Override
  public String toString() {
    return "[ " + this.left + ", " + this.right + " ]";
  }
}
