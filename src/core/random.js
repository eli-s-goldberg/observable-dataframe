/**
 * random.js: one seeded PRNG for the whole ecosystem.
 *
 * Mulberry32: 32-bit state, one multiply-xorshift sandwich per draw, and
 * runs anywhere. Not cryptographic, deliberately; it exists so a Monte
 * Carlo run, a sample(), or a bootstrap can be replayed digit for digit.
 * The dynamics and business packages consume this same generator, so a
 * seed means the same sequence everywhere.
 */

/**
 * Seeded uniform PRNG. Returns a function that yields floats in [0, 1),
 * deterministically for a given integer seed. Drop-in for Math.random
 * anywhere a `random` option is accepted.
 *
 * @param {number} [seed=0] integer seed; the same seed replays the same sequence
 * @returns {() => number}
 */
export function random(seed = 0) {
  if (!Number.isInteger(seed)) throw new Error(`Random seed must be an integer.`);
  let state = seed >>> 0;
  return () => {
    state += 0x6d2b79f5;
    let value = Math.imul(state ^ (state >>> 15), 1 | state);
    value ^= value + Math.imul(value ^ (value >>> 7), 61 | value);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}
