/**
 * observable-dataframe/plots — figures that take a DataFrame and behave.
 *
 * Everything here is (data, options) => a Plot figure or an htl element,
 * ready for view() in Observable Framework. Data can be a DataFrame or
 * plain rows; the primitives don't make you choose sides.
 */

export { colors, fonts, typography, cardStyle, exhibitLabelStyle, plotDefaults, tufteAxis } from "./theme.js";
export { resolveTip, createTooltip, tipHTML } from "./options.js";
export { asRows, fmtK, fmtPct } from "./util.js";
export {
  corrPlot,
  distPlot,
  forestPlot,
  funnelChart,
  boxPlot,
  timeline,
  designMatrixPlot,
} from "./basic.js";
export { dotPlot } from "./dotPlot.js";
export { waterfallPlot } from "./waterfallPlot.js";
export { twoGroupBoxPlot } from "./twoGroupBoxPlot.js";
export { facetedDensityPlot } from "./facetedDensityPlot.js";
export { summaryTable } from "./summaryTable.js";
export { experimentDesignTree, powerTable } from "./experimentDesignTree.js";
export { measurementTimeline } from "./measurementTimeline.js";
export { bumpChart } from "./bumpChart.js";
export { trapezoidFunnel } from "./trapezoidFunnel.js";
export { tufteLine, tufteLineMarks } from "./tufteLine.js";
export { sankeyFlow, nestFromFrame } from "./sankeyFlow.js";
export { treeExplore } from "./treeExplore.js";
export { tufteForestPlot, withDownloadButtons } from "./tufteForestPlot.js";
export { serpentineTimeline } from "./serpentineTimeline.js";
export { pictogramFill } from "./pictogram.js";
export { eventStudyPlot } from "./eventStudyPlot.js";
export { didPlot } from "./didPlot.js";
export { consortDiagram } from "./consortDiagram.js";
