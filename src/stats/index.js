/**
 * observable-dataframe/stats — the numbers behind the figures.
 *
 * Pure functions and small classes, no DOM, no plotting, no CDN imports.
 * Power analysis and experiment design (power.js), hypothesis tests and
 * regression (tests.js), Monte Carlo distributions (distributions.js),
 * and the special functions that make the p-values go (special.js).
 */

export { probit, normalCDF, studentTCDF, fCDF, tTwoSidedP, ibeta, gammaln, gammainc, chi2CDF } from "./special.js";
export { tableOne } from "./tableOne.js";
export {
  ExperimentDesign,
  Experiment,
  sampleSizePerArm,
  multiArmAdjustment,
  channelCascade,
} from "./experiment.js";
export {
  sampleSizeTwoProportions,
  sampleSizeTwoMeans,
  standardizedMeanDifference,
  powerAnalysis,
  evaluateCadence,
  varianceIndividual,
  varianceCluster,
  designEffect,
  varianceComparisonData,
  designMatrix,
  designMatrixData,
  varthetaM,
} from "./power.js";
export { oneSampleTTest, twoSampleTTest, welchTTest, ols, ancova } from "./tests.js";
export { fitOLS, withinTransform } from "./regression.js";
export {
  did,
  twfe,
  eventStudy,
  callawaySantAnna,
  checkParallelTrends,
  placeboTest,
} from "./did.js";
export { distributions, DistP } from "./distributions.js";
export { kde, silvermanBandwidth } from "./density.js";
export { identity, matmul, matvec, inv, transpose, dot } from "./matrix.js";
