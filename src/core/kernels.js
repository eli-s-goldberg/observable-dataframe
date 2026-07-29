/**
 * kernels.js — the engine room.
 *
 * Everything in here operates on typed arrays and integer indices. No row
 * objects, no closures-per-element, no clever tricks that allocate a
 * surprise array per row. If you find yourself wanting to build a
 * `{...row}` in this file, go take a walk and reconsider.
 */

import { Column } from "./Column.js";

/**
 * Turn a boolean column into a selection vector: the indices where the
 * value is true (nulls count as false, because "maybe" doesn't pass a
 * filter). This is how a filter becomes a cheap gather instead of an
 * expensive copy-everything-and-see.
 *
 * @param {Column} mask a "bool" column
 * @returns {Uint32Array} indices of the rows that made the cut
 */
export function filterIndices(mask) {
  const n = mask.length;
  const data = mask.data;
  const validity = mask.validity;
  // Two passes: count, then fill. Cheaper than growing an array like it's 2009.
  let count = 0;
  if (validity === null) {
    for (let i = 0; i < n; i++) count += data[i];
    const out = new Uint32Array(count);
    for (let i = 0, j = 0; i < n; i++) if (data[i]) out[j++] = i;
    return out;
  }
  for (let i = 0; i < n; i++) count += data[i] & validity[i];
  const out = new Uint32Array(count);
  for (let i = 0, j = 0; i < n; i++) if (data[i] & validity[i]) out[j++] = i;
  return out;
}

/**
 * Stable argsort over one or more columns. Returns the permutation that
 * would sort the frame, which you then feed to `Column.take()`. Sorting
 * indices instead of rows means we pay for one permutation, not one
 * comparison-swap of entire records.
 *
 * Nulls sort last regardless of direction. They didn't show up with a
 * value; they don't get a say in the ordering.
 *
 * @param {Column[]} columns sort keys, most significant first
 * @param {boolean[]} descending per-key direction flags
 * @returns {Uint32Array}
 */
export function argsort(columns, descending) {
  const n = columns[0].length;
  const indices = new Uint32Array(n);
  for (let i = 0; i < n; i++) indices[i] = i;

  // Precompute comparable keys per column so the comparator stays branch-light.
  const keys = columns.map((c) => sortKeys(c));
  const dirs = columns.map((_, k) => (descending[k] ? -1 : 1));
  const valids = columns.map((c) => c.validity);

  const cmp = (a, b) => {
    for (let k = 0; k < keys.length; k++) {
      const va = valids[k] === null || valids[k][a] === 1;
      const vb = valids[k] === null || valids[k][b] === 1;
      if (!va || !vb) {
        if (va !== vb) return va ? -1 : 1; // nulls last, always
        continue; // both null: tie, next key
      }
      const ka = keys[k][a];
      const kb = keys[k][b];
      if (ka < kb) return -dirs[k];
      if (ka > kb) return dirs[k];
    }
    return a - b; // stability by original position
  };

  // Array.prototype.sort on a typed array is not guaranteed stable, so we
  // sort a regular array of indices (V8 sorts those stably) — the final
  // `a - b` tiebreak makes stability explicit either way.
  const arr = Array.from(indices);
  arr.sort(cmp);
  return Uint32Array.from(arr);
}

function sortKeys(column) {
  if (column.dtype === "str") {
    // Map dictionary codes to their rank in sorted dictionary order, so
    // string comparisons become integer comparisons. localeCompare per row
    // is how sorts go to die.
    const order = column.dict
      .map((s, i) => [s, i])
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
    const rank = new Uint32Array(column.dict.length);
    for (let r = 0; r < order.length; r++) rank[order[r][1]] = r;
    const n = column.length;
    const keys = new Uint32Array(n);
    for (let i = 0; i < n; i++) keys[i] = rank[column.data[i]];
    return keys;
  }
  return column.data;
}

/**
 * Assign every row a dense group id based on one or more key columns.
 * Single column: reuse its group codes directly. Multiple columns: combine
 * integer codes pairwise through a Map, which dodges the classic
 * join-with-a-delimiter bug where "a_b" + "c" collides with "a" + "b_c".
 * We have read that postmortem. We choose not to star in it.
 *
 * @param {Column[]} columns key columns
 * @returns {{ids: Int32Array, nGroups: number, firstRow: Uint32Array}}
 *   ids: group id per row; firstRow: a representative row index per group
 */
export function groupIds(columns) {
  const n = columns[0].length;
  let ids = null;
  let nGroups = 0;

  for (const column of columns) {
    const { codes, nCodes } = column.groupCodes();
    if (ids === null) {
      ids = codes;
      nGroups = nCodes;
      continue;
    }
    // Combine (previous id, new code) into a fresh dense id. Integer key
    // math when it fits in a double exactly; Map handles the dedup.
    const combined = new Int32Array(n);
    const seen = new Map();
    let next = 0;
    for (let i = 0; i < n; i++) {
      const key = ids[i] * nCodes + codes[i];
      let id = seen.get(key);
      if (id === undefined) {
        id = next++;
        seen.set(key, id);
      }
      combined[i] = id;
    }
    ids = combined;
    nGroups = next;
  }

  // Compact ids so they're dense 0..nGroups-1 even for the single-column
  // case (dictionary codes can have gaps after a filter).
  const remap = new Int32Array(nGroups).fill(-1);
  const firstRow = new Uint32Array(nGroups);
  let next = 0;
  for (let i = 0; i < n; i++) {
    const id = ids[i];
    if (remap[id] === -1) {
      remap[id] = next;
      firstRow[next] = i;
      next++;
    }
    ids[i] = remap[id];
  }
  return { ids, nGroups: next, firstRow: firstRow.slice(0, next) };
}

/**
 * Per-group row index lists, for aggregations that need to see all members
 * (median, quantile, implode). Built once, reused for every such agg.
 *
 * @param {Int32Array} ids group id per row
 * @param {number} nGroups
 * @returns {Uint32Array[]} rows per group
 */
export function groupRowIndices(ids, nGroups) {
  const counts = new Uint32Array(nGroups);
  for (let i = 0; i < ids.length; i++) counts[ids[i]]++;
  const groups = new Array(nGroups);
  for (let g = 0; g < nGroups; g++) groups[g] = new Uint32Array(counts[g]);
  const fill = new Uint32Array(nGroups);
  for (let i = 0; i < ids.length; i++) {
    const g = ids[i];
    groups[g][fill[g]++] = i;
  }
  return groups;
}

/**
 * Hash join on key columns: returns matched index pairs for the requested
 * join flavor. Build on the right (usually smaller — and if it isn't,
 * swapping is your job, future optimizer), probe with the left.
 *
 * Null keys never match anything, per SQL semantics and common decency.
 *
 * @param {Column[]} leftKeys
 * @param {Column[]} rightKeys
 * @param {"inner"|"left"|"right"|"outer"} how
 * @returns {{leftIdx: Int32Array, rightIdx: Int32Array}} -1 marks "no match, pad with nulls"
 */
export function hashJoin(leftKeys, rightKeys, how) {
  const nLeft = leftKeys[0].length;
  const nRight = rightKeys[0].length;

  const leftKeyOf = rowKeyFn(leftKeys);
  const rightKeyOf = rowKeyFn(rightKeys);

  // Build phase: key -> list of right row indices.
  const table = new Map();
  for (let j = 0; j < nRight; j++) {
    const key = rightKeyOf(j);
    if (key === null) continue;
    const bucket = table.get(key);
    if (bucket === undefined) table.set(key, [j]);
    else bucket.push(j);
  }

  const leftOut = [];
  const rightOut = [];
  const rightMatched = how === "right" || how === "outer" ? new Uint8Array(nRight) : null;

  for (let i = 0; i < nLeft; i++) {
    const key = leftKeyOf(i);
    const bucket = key === null ? undefined : table.get(key);
    if (bucket !== undefined) {
      for (let b = 0; b < bucket.length; b++) {
        leftOut.push(i);
        rightOut.push(bucket[b]);
        if (rightMatched) rightMatched[bucket[b]] = 1;
      }
    } else if (how === "left" || how === "outer") {
      leftOut.push(i);
      rightOut.push(-1);
    }
  }

  if (rightMatched) {
    for (let j = 0; j < nRight; j++) {
      if (!rightMatched[j]) {
        leftOut.push(-1);
        rightOut.push(j);
      }
    }
  }

  return { leftIdx: Int32Array.from(leftOut), rightIdx: Int32Array.from(rightOut) };
}

/**
 * Build a row -> primitive-key function for a set of key columns. Single
 * numeric/str column: raw value (fast path). Multiple columns: string of
 * dtype-tagged parts joined with \x00, which no reasonable data contains,
 * and unreasonable data deserves what it gets. Null anywhere → null key.
 */
function rowKeyFn(keys) {
  if (keys.length === 1) {
    const c = keys[0];
    if (c.dtype === "str") {
      return (i) => (c.isValid(i) ? c.dict[c.data[i]] : null);
    }
    return (i) => (c.isValid(i) ? c.data[i] : null);
  }
  return (i) => {
    let out = "";
    for (let k = 0; k < keys.length; k++) {
      const c = keys[k];
      if (!c.isValid(i)) return null;
      out += (c.dtype === "str" ? c.dict[c.data[i]] : c.data[i]) + "\x00";
    }
    return out;
  };
}

/**
 * Take with -1 sentinel: gather rows, turning -1 indices into nulls.
 * The muscle behind outer joins' padded rows.
 *
 * @param {Column} column
 * @param {Int32Array} indices row indices, -1 meaning "there is no row"
 * @returns {Column}
 */
export function takeWithNulls(column, indices) {
  const n = indices.length;
  const data = new column.data.constructor(n);
  let validity = new Uint8Array(n).fill(1);
  let anyNull = false;
  for (let i = 0; i < n; i++) {
    const idx = indices[i];
    if (idx === -1 || !column.isValid(idx)) {
      validity[i] = 0;
      anyNull = true;
    } else {
      data[i] = column.data[idx];
    }
  }
  if (!anyNull) validity = null;
  return new Column(column.dtype, data, { validity, dict: column.dict });
}
