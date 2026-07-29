/**
 * Data loader: member-month claims panel for the docs site.
 *
 * Observable Framework runs this at build/preview time and caches the CSV.
 * See https://observablehq.com/framework/data-loaders
 *
 * This always simulates, and deliberately does not look for a local extract.
 * Two reasons. A public site that prefers a local file publishes whatever
 * happens to sit on the machine that ran the build, which is a standing hazard
 * rather than a feature. And a seeded panel is byte-identical everywhere, so
 * the numbers quoted in the prose on /statistics stay true on every checkout.
 * To work against your own extract, point your own page at your own loader.
 *
 * The name matters. Framework serves a static file in preference to a loader
 * that targets the same path, so a stale CSV sitting next to a loader silently
 * wins. This loader is deliberately not named after any file that has ever been
 * written into docs/files by hand.
 */

import { claimsPanelCsv } from "observable-dataframe/data";

const { csv } = claimsPanelCsv({ seed: 42 });

process.stdout.write(csv);
