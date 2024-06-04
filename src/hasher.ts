/**
 * Hasher interface. Implementations of this interface are used to hash data and compress two hashes into one.
 */
export interface Hasher {
  /**
   * Compress two hashes into one
   * @param lhs
   * @param rhs
   */
  compress(lhs: Buffer, rhs: Buffer): Buffer;

  /**
   * Hash the data
   * @param data
   */
  hash(data: Buffer): Buffer;
}
