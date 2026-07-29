/**
 * observable-dataframe/data: the member-month claims panel, both halves.
 *
 * claimsSlice.js parses and rolls up a published panel; simulateClaimsPanel.js
 * generates one from a seed. The generated panel is the path the docs site and
 * the test suite take, so nothing downstream depends on private data existing.
 */

export {
  DEFAULT_CLAIMS_SLICE_PATH,
  claimsSliceFromCSV,
  enrolledMemberMonths,
  memberRollup,
  monthlyTrend,
} from "./claimsSlice.js";

export {
  CLAIMS_PANEL_COLUMNS,
  CLAIMS_PANEL_DEFAULTS,
  claimsPanelCsv,
  simulateClaimsPanel,
} from "./simulateClaimsPanel.js";
