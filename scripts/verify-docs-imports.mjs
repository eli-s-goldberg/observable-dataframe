#!/usr/bin/env node
/**
 * Verify Observable Framework can resolve every docs import path.
 * Run before docs:dev / docs:build; fails fast with a clear message.
 */

import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");
const require = createRequire(join(root, "package.json"));

const subpaths = [
  "observable-dataframe",
  "observable-dataframe/data",
  "observable-dataframe/stats",
  "observable-dataframe/plots",
  "observable-dataframe/layouts",
];

let failed = false;
for (const spec of subpaths) {
  try {
    const resolved = require.resolve(spec);
    if (!existsSync(resolved)) throw new Error(`resolved path missing: ${resolved}`);
    console.log(`ok  ${spec}`);
  } catch (err) {
    failed = true;
    console.error(`fail ${spec}: ${err.message}`);
  }
}

if (failed) {
  console.error(
    "\nDocs imports are broken. Try: npm install && npm run docs:clean && npm run docs:dev"
  );
  process.exit(1);
}
