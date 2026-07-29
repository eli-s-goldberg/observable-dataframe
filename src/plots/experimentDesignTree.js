/**
 * experimentDesignTree.js — the figure that gets experiments funded.
 *
 * An H-tree linking the whole causal chain in one view: population splits
 * into cohorts, each cohort splits into control and treatment arms, and
 * every node carries its numbers — base rate P1, behavior change D scaled
 * by attributable fraction F to target rate P2, required n per arm from
 * the power calc, and months until you can measure anything. It exists to
 * answer, before money moves, the question everyone asks after money
 * moves: "why can't we see the impact yet?"
 *
 * Driven entirely by stats/powerAnalysis, so the tree and the ratio table
 * below it can never disagree with each other. One source of arithmetic.
 */

import { html } from "htl";
import { powerAnalysis } from "../stats/power.js";
import { colors, fonts, cardStyle, exhibitLabelStyle } from "./theme.js";
import { fmtK, fmtPct } from "./util.js";

/**
 * @param {object} config
 * @param {Array<object>} config.cohorts one entry per cohort:
 *   {label, baseRate, behaviorChange, channels?, months?, ops?, note?}
 * @param {object} [config.inputs] power settings:
 *   {design, alpha, power, attributable} — see stats/powerAnalysis
 * @param {string} [config.title="Experiment design & power map"]
 * @param {string} [config.populationLabel="Eligible population"]
 * @returns {HTMLElement}
 */
export function experimentDesignTree({
  cohorts,
  inputs = {},
  title = "Experiment design & power map",
  populationLabel = "Eligible population",
} = {}) {
  const settings = {
    design: "one-sided-proportions",
    alpha: 0.1,
    power: 0.8,
    attributable: 1,
    ...inputs,
  };

  const results = cohorts.map((c) => ({
    cohort: c,
    r: powerAnalysis({
      baseRate: c.baseRate,
      behaviorChange: c.behaviorChange,
      attributable: settings.attributable,
      design: settings.design,
      alpha: settings.alpha,
      power: settings.power,
    }),
  }));
  const totalN = results.reduce((acc, { r }) => acc + r.totalN, 0);
  const singleArm = settings.design === "single-arm";

  const armBox = (kind, text, sub) => html`<div
    style="border-radius: 4px; padding: 10px 12px; text-align: center; font-weight: 700; font-size: 12px;
           line-height: 1.35; min-height: 58px; display: flex; flex-direction: column; align-items: center; justify-content: center;
           background: ${kind === "treatment" ? colors.treatment : colors.control};
           color: ${kind === "treatment" ? colors.treatmentText : colors.controlText};
           border: 1px solid ${kind === "treatment" ? "rgba(13,115,119,0.4)" : "#D5D9DD"};">
    <div>${text}</div>
    ${sub ? html`<div style="font-weight: 500; font-size: 10px; margin-top: 3px; color: ${colors.muted};">${sub}</div>` : ""}
  </div>`;

  const stat = (label, value, emphasize = false) => html`<div style="display: flex; justify-content: space-between; gap: 10px; font-size: 10.5px; padding: 2px 0;">
    <span style="color: ${colors.muted};">${label}</span>
    <span style="font-weight: 700; color: ${emphasize ? colors.clinical : colors.ink};">${value}</span>
  </div>`;

  const connector = html`<div style="width: 2px; height: 14px; background: ${colors.border}; margin: 0 auto;"></div>`;

  return html`<figure style="margin: 0; font-family: ${fonts.sans};">
    <div style="${exhibitLabelStyle} margin-bottom: 6px;">${title}</div>
    <div style="${cardStyle}">
      <!-- root -->
      <div style="max-width: 340px; margin: 0 auto;">
        <div style="background: ${colors.navy}; color: white; border-radius: 4px; padding: 10px; text-align: center;
                    font-weight: 700; font-size: 13px; box-shadow: 0 1px 4px rgba(0,0,0,0.12);">
          ${populationLabel}
          <div style="font-weight: 500; font-size: 10.5px; margin-top: 2px; opacity: 0.85;">
            powered N ≈ ${fmtK(totalN)} · α=${fmtPct(settings.alpha, 0)} ·
            power=${fmtPct(settings.power, 0)} · F=${fmtPct(settings.attributable, 0)}
          </div>
        </div>
      </div>
      ${connector}

      <!-- cohorts -->
      <div style="display: grid; grid-template-columns: repeat(${cohorts.length}, 1fr); gap: 18px;">
        ${results.map(
          ({ cohort, r }) => html`<div>
            <div style="background: ${colors.clinical}; color: white; border-radius: 4px; padding: 8px 10px;
                        text-align: center; font-weight: 700; font-size: 12.5px;">
              ${cohort.label}
              <div style="font-weight: 500; font-size: 10px; opacity: 0.9;">
                P1 = ${fmtPct(r.p1)} · D = ${fmtPct(r.p2 - r.p1, 2)} → P2 = ${fmtPct(r.p2, 2)}
              </div>
            </div>
            ${connector}
            <div style="display: grid; grid-template-columns: ${singleArm ? "1fr" : "1fr 1fr"}; gap: 10px;">
              ${singleArm
                ? armBox("treatment", `Single arm — n ≈ ${fmtK(r.nPerArm)}`, cohort.channels)
                : [
                    armBox("control", `Control — n ≈ ${fmtK(r.nPerArm)}`, "usual care / no outreach"),
                    armBox("treatment", `Treatment — n ≈ ${fmtK(r.nPerArm)}`, cohort.channels),
                  ]}
            </div>
            <div style="margin-top: 8px; border-top: 1px dashed ${colors.border}; padding-top: 6px;">
              ${stat("n per arm", fmtK(r.nPerArm), true)}
              ${stat("total n", fmtK(r.totalN))}
              ${cohort.months ? stat("months to measurement", cohort.months, true) : ""}
              ${cohort.ops ? stat("operational load", cohort.ops) : ""}
              ${cohort.note
                ? html`<div style="font-size: 10px; font-style: italic; color: ${colors.faint}; margin-top: 4px;">${cohort.note}</div>`
                : ""}
            </div>
          </div>`
        )}
      </div>

      <figcaption style="font-size: 10.5px; color: ${colors.faint}; font-style: italic; margin-top: 12px; line-height: 1.5;">
        n per arm = (Z₁₋α·√(2·p̄·q̄) + Z₁₋β·√(p₁q₁ + p₂q₂))² ÷ (P2 − P1)², normal approximation, 1:1
        allocation${settings.design === "difference-in-differences" ? ", ×1.5 DiD inflation" : ""}.
        P2 = P1 + D×F, where F is the attributable (detectable) fraction of the behavior change.
      </figcaption>
    </div>
  </figure>`;
}

/**
 * The companion ratio table: the same power calculation shown as the
 * A/B/C… parameter walk-through, one row per input, so reviewers can
 * audit the arithmetic line by line. Same engine as the tree; agreeing
 * with itself is its best feature.
 *
 * @param {{baseRate: number, behaviorChange: number, label?: string}} cohort
 * @param {object} [inputs] {design, alpha, power, attributable}
 * @returns {HTMLElement}
 */
export function powerTable(cohort, inputs = {}) {
  const settings = { design: "one-sided-proportions", alpha: 0.1, power: 0.8, attributable: 1, ...inputs };
  const r = powerAnalysis({
    baseRate: cohort.baseRate,
    behaviorChange: cohort.behaviorChange,
    ...settings,
  });
  const twoSided = settings.design === "two-sided-proportions" || settings.design === "chi-square";

  const rows = [
    ["A", "Z₁₋α", r.zAlpha.toFixed(2), `α = ${fmtPct(settings.alpha, 1)} (${twoSided ? "2-sided" : "1-sided"})`],
    ["B", "Z₁₋β", r.zBeta.toFixed(2), `${fmtPct(settings.power, 0)} power`],
    ["C", "P1 — rate in base population", fmtPct(r.p1), "observed / estimated"],
    ["D", "Behavior change", fmtPct(cohort.behaviorChange, 2), "estimated"],
    ["F", "Attributable fraction", fmtPct(r.attributable, 0), "share of D detectable in this metric"],
    ["E", "P2 — rate in target population", fmtPct(r.p2, 2), "E = C + (D × F)"],
    ["H", "n per arm", fmtK(r.nPerArm), `${r.arms} arm${r.arms > 1 ? "s" : ""} · total ${fmtK(r.totalN)}`],
  ];

  return html`<div style="font-family: ${fonts.sans};">
    ${cohort.label ? html`<div style="${exhibitLabelStyle} margin-bottom: 6px;">Ratio power calc — ${cohort.label}</div>` : ""}
    <table style="width: 100%; border-collapse: collapse; font-size: 11.5px; background: white;">
      <thead>
        <tr>
          ${["", "Parameter", "Value", "Basis"].map(
            (h) => html`<th style="background: ${colors.navy}; color: white; padding: 6px 10px; text-align: left; font-size: 10px; text-transform: uppercase; letter-spacing: 0.05em;">${h}</th>`
          )}
        </tr>
      </thead>
      <tbody>
        ${rows.map(
          ([idx, param, value, basis]) => html`<tr>
            <td style="padding: 6px 10px; border-bottom: 1px solid #ECECEC; color: ${colors.faint}; font-weight: 700;">${idx}</td>
            <td style="padding: 6px 10px; border-bottom: 1px solid #ECECEC; font-weight: 600;">${param}</td>
            <td style="padding: 6px 10px; border-bottom: 1px solid #ECECEC; font-weight: 800; color: ${idx === "H" ? colors.clinical : colors.ink};">${value}</td>
            <td style="padding: 6px 10px; border-bottom: 1px solid #ECECEC; color: #555; font-style: italic;">${basis}</td>
          </tr>`
        )}
      </tbody>
    </table>
  </div>`;
}
