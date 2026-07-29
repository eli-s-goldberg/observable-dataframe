/**
 * observable-dataframe/layouts — page structure and table polish.
 *
 * The formatting system: inject the page style once, compose sections
 * with splitPanel/stackPanel, drop KPI cards in slots, and hand
 * tableFormatters to Inputs.table. Every page looks like it was designed
 * on purpose, because now it was.
 */

export {
  pageStyle,
  siteChromeStyle,
  numberedHeadingsStyle,
  injectPageStyle,
  createNumberHeadings,
} from "./pageStyle.js";
export { splitPanel, stackPanel, prose, kpiCard, cardRow } from "./splitPanel.js";
export { tabPanel } from "./tabPanel.js";
export {
  sparkbar,
  formatStatus,
  formatTwoLevel,
  formatWrappedText,
  formatTextBold,
  formatHeaderTextBold,
  formatBulletedList,
  formatProsList,
  formatConsList,
  formatNeutralList,
  withRowHeight,
  formatCurrency,
  formatPercent,
} from "./tableFormatters.js";
