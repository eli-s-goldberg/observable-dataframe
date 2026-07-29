/**
 * matrix.js — just enough linear algebra, and not one operation more.
 *
 * Dense row-major arrays of arrays, Gauss-Jordan inversion, the basics.
 * These matrices are experiment-design sized (a handful of periods and
 * sequences), so clarity beats cleverness and nobody needs BLAS.
 */

/** n×n identity. */
export function identity(n) {
  return Array.from({ length: n }, (_, i) =>
    Array.from({ length: n }, (_, j) => (i === j ? 1 : 0))
  );
}

/** Matrix product A·B. Dimensions are your responsibility; mismatches throw. */
export function matmul(A, B) {
  const n = A.length;
  const k = A[0].length;
  if (B.length !== k) {
    throw new Error(`matmul: ${n}x${k} times ${B.length}x${B[0].length} does not parse.`);
  }
  const m = B[0].length;
  const out = Array.from({ length: n }, () => new Array(m).fill(0));
  for (let i = 0; i < n; i++) {
    for (let p = 0; p < k; p++) {
      const a = A[i][p];
      if (a === 0) continue;
      for (let j = 0; j < m; j++) out[i][j] += a * B[p][j];
    }
  }
  return out;
}

/** Matrix–vector product A·v. */
export function matvec(A, v) {
  return A.map((row) => row.reduce((acc, x, j) => acc + x * v[j], 0));
}

/** Dot product. */
export function dot(a, b) {
  let s = 0;
  for (let i = 0; i < a.length; i++) s += a[i] * b[i];
  return s;
}

/**
 * Matrix inverse by Gauss-Jordan with partial pivoting. Throws on singular
 * input rather than returning creative numbers.
 */
export function inv(A) {
  const n = A.length;
  const aug = A.map((row, i) => [...row, ...identity(n)[i]]);
  for (let col = 0; col < n; col++) {
    let pivot = col;
    for (let r = col + 1; r < n; r++) {
      if (Math.abs(aug[r][col]) > Math.abs(aug[pivot][col])) pivot = r;
    }
    if (Math.abs(aug[pivot][col]) < 1e-12) {
      throw new Error(`Matrix is singular (or close enough to ruin your afternoon).`);
    }
    [aug[col], aug[pivot]] = [aug[pivot], aug[col]];
    const p = aug[col][col];
    for (let j = 0; j < 2 * n; j++) aug[col][j] /= p;
    for (let r = 0; r < n; r++) {
      if (r === col) continue;
      const f = aug[r][col];
      if (f === 0) continue;
      for (let j = 0; j < 2 * n; j++) aug[r][j] -= f * aug[col][j];
    }
  }
  return aug.map((row) => row.slice(n));
}

/** Transpose. */
export function transpose(A) {
  return A[0].map((_, j) => A.map((row) => row[j]));
}
