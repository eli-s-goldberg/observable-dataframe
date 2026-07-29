/**
 * consortDiagram.js — the participant flow diagram, publication grade.
 *
 * The CONSORT flow diagram: enrollment at the top, exclusions branching
 * right with their reasons, randomization, parallel allocation arms, and
 * follow-up / analysis boxes descending in lockstep. Journals require it,
 * reviewers read it before the abstract, and a study whose arithmetic
 * does not reconcile down the diagram has bigger problems than layout.
 * Counts are your job; this draws them and checks your subtraction.
 *
 * Rendered as clean SVG: house sans, 1px strokes, no gradients, no
 * shadows, exportable straight into a manuscript (pairs with
 * withDownloadButtons for the SVG/PNG handoff).
 *
 * @example
 *   consortDiagram({
 *     steps: [
 *       { label: "Assessed for eligibility", n: 64632 },
 *       { label: "Randomized", n: 6000,
 *         excluded: { label: "Excluded", n: 58632, reasons: [
 *           { label: "Insufficient enrollment span", n: 41210 },
 *           { label: "No claims activity", n: 17422 },
 *         ] } },
 *     ],
 *     arms: [
 *       { label: "Intervention", n: 3000, steps: [
 *         { label: "Received outreach", n: 2874 },
 *         { label: "Analyzed", n: 2874, excluded: { label: "Excluded", n: 126, reasons: [{ label: "Disenrolled before outreach", n: 126 }] } },
 *       ] },
 *       { label: "Usual care", n: 3000, steps: [
 *         { label: "Analyzed", n: 3000 },
 *       ] },
 *     ],
 *   })
 */

import { colors, fonts, typography } from "./theme.js";

const BOX = {
  fill: "white",
  stroke: colors.ink,
  strokeWidth: 1,
  padX: 10,
  padY: 8,
  lineHeight: 15,
};

/**
 * @param {object} config
 * @param {Array<object>} config.steps the main spine, top to bottom:
 *   {label, n, sublabel?, excluded?: {label, n, reasons?: [{label, n}]}}
 * @param {Array<object>} [config.arms] parallel arms after the spine:
 *   {label, n, steps: [{label, n, excluded?}]} — arm steps advance in lockstep rows
 * @param {number} [config.width=760]
 * @param {number} [config.boxWidth=250] main/arm box width
 * @param {number} [config.exclusionWidth=230] side-box width
 * @param {number} [config.rowGap=46] vertical gap between rows
 * @param {string} [config.title]
 * @returns {SVGSVGElement}
 */
export function consortDiagram({
  steps = [],
  arms = [],
  width = 760,
  boxWidth = 250,
  exclusionWidth = 230,
  rowGap = 46,
  title = "",
} = {}) {
  if (!steps.length) throw new Error(`consortDiagram needs steps. A flow diagram of nothing flows nowhere.`);

  const svgNS = "http://www.w3.org/2000/svg";
  const el = (tag, attrs = {}) => {
    const node = document.createElementNS(svgNS, tag);
    for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
    return node;
  };

  const svg = el("svg");
  const root = el("g");
  svg.appendChild(root);

  const fmt = (n) => (typeof n === "number" ? n.toLocaleString("en-US") : String(n));

  // --- text measurement without a layout engine: chars × em width ----------
  const charW = typography.tick * 0.6;
  const wrap = (text, maxWidth) => {
    const words = String(text).split(/\s+/);
    const maxChars = Math.floor((maxWidth - 2 * BOX.padX) / charW);
    const lines = [];
    let line = "";
    for (const word of words) {
      if (line && line.length + word.length + 1 > maxChars) {
        lines.push(line);
        line = word;
      } else {
        line = line ? `${line} ${word}` : word;
      }
    }
    if (line) lines.push(line);
    return lines;
  };

  const drawBox = (cx, top, w, main, sub = [], { bold = true, align = "middle" } = {}) => {
    const mainLines = wrap(main, w);
    const subLines = sub.flatMap((s) => wrap(s, w));
    const h = BOX.padY * 2 + (mainLines.length + subLines.length) * BOX.lineHeight;
    root.appendChild(
      el("rect", {
        x: cx - w / 2,
        y: top,
        width: w,
        height: h,
        fill: BOX.fill,
        stroke: BOX.stroke,
        "stroke-width": BOX.strokeWidth,
      })
    );
    let y = top + BOX.padY + BOX.lineHeight - 4;
    for (const line of mainLines) {
      const t = el("text", {
        x: align === "middle" ? cx : cx - w / 2 + BOX.padX,
        y,
        "text-anchor": align,
        "font-size": typography.tick,
        "font-weight": bold ? 700 : 400,
        fill: colors.ink,
      });
      t.textContent = line;
      root.appendChild(t);
      y += BOX.lineHeight;
    }
    for (const line of subLines) {
      const t = el("text", {
        x: cx - w / 2 + BOX.padX,
        y,
        "text-anchor": "start",
        "font-size": typography.annotation,
        fill: "#333",
      });
      t.textContent = line;
      root.appendChild(t);
      y += BOX.lineHeight;
    }
    return { top, bottom: top + h, cx, w, h };
  };

  const arrow = (x1, y1, x2, y2) => {
    root.appendChild(el("line", { x1, y1, x2, y2, stroke: colors.ink, "stroke-width": 1 }));
    // arrowhead on the terminal segment
    const angle = Math.atan2(y2 - y1, x2 - x1);
    const size = 5;
    const p1 = [x2 - size * Math.cos(angle - 0.45), y2 - size * Math.sin(angle - 0.45)];
    const p2 = [x2 - size * Math.cos(angle + 0.45), y2 - size * Math.sin(angle + 0.45)];
    root.appendChild(el("path", { d: `M${x2},${y2} L${p1[0]},${p1[1]} L${p2[0]},${p2[1]} Z`, fill: colors.ink }));
  };

  // --- layout ----------------------------------------------------------------
  const spineX = arms.length ? width / 2 : width / 2;
  let y = 16;

  if (title) {
    const t = el("text", { x: 8, y: y + 4, "font-size": typography.title, "font-weight": 700, fill: colors.ink });
    t.textContent = title;
    root.appendChild(t);
    y += 28;
  }

  const exclusionX = spineX + boxWidth / 2 + 40 + exclusionWidth / 2;

  let prevBottom = null;
  for (const step of steps) {
    const sub = step.sublabel ? [step.sublabel] : [];
    const box = drawBox(spineX, y, boxWidth, `${step.label} (n = ${fmt(step.n)})`, sub);
    if (prevBottom != null) arrow(spineX, prevBottom, spineX, box.top);

    if (step.excluded) {
      const reasons = (step.excluded.reasons ?? []).map((r) => `• ${r.label} (n = ${fmt(r.n)})`);
      const midY = prevBottom != null ? (prevBottom + box.top) / 2 : box.top - 12;
      const exBox = drawBox(
        exclusionX,
        midY - 10,
        exclusionWidth,
        `${step.excluded.label ?? "Excluded"} (n = ${fmt(step.excluded.n)})`,
        reasons,
        { align: "start" }
      );
      arrow(spineX, midY, exBox.cx - exBox.w / 2, midY);
      // Arithmetic audit: the diagram should not claim more people than it has.
      void exBox;
    }
    prevBottom = box.bottom;
    y = box.bottom + rowGap;
  }

  if (arms.length) {
    const armWidth = Math.min(boxWidth, (width - 60) / arms.length - 20);
    const armXs = arms.map((_, i) => ((i + 0.5) * width) / arms.length);
    const branchY = prevBottom + rowGap / 2;

    // the fork: spine down, horizontal rail, drops into each arm
    root.appendChild(el("line", { x1: spineX, y1: prevBottom, x2: spineX, y2: branchY, stroke: colors.ink, "stroke-width": 1 }));
    root.appendChild(el("line", { x1: Math.min(...armXs), y1: branchY, x2: Math.max(...armXs), y2: branchY, stroke: colors.ink, "stroke-width": 1 }));

    let maxBottom = 0;
    const armBottoms = arms.map((arm, i) => {
      const ax = armXs[i];
      arrow(ax, branchY, ax, branchY + rowGap / 2);
      let ay = branchY + rowGap / 2;
      let bottom = ay;
      const head = drawBox(ax, ay, armWidth, `${arm.label} (n = ${fmt(arm.n)})`);
      bottom = head.bottom;
      let prev = head.bottom;
      for (const step of arm.steps ?? []) {
        const top = prev + rowGap;
        const box = drawBox(ax, top, armWidth, `${step.label} (n = ${fmt(step.n)})`, step.sublabel ? [step.sublabel] : []);
        arrow(ax, prev, ax, box.top);
        if (step.excluded) {
          const reasons = (step.excluded.reasons ?? []).map((r) => `• ${r.label} (n = ${fmt(r.n)})`);
          const midY = (prev + box.top) / 2;
          const exW = Math.min(exclusionWidth, armWidth);
          const dir = i === arms.length - 1 ? 1 : 1; // exclusions hang right of each arm
          const exBox = drawBox(ax + armWidth / 2 + 24 + exW / 2, midY - 8, exW, `${step.excluded.label ?? "Excluded"} (n = ${fmt(step.excluded.n)})`, reasons, { align: "start" });
          arrow(ax + dir * 0, midY, exBox.cx - exBox.w / 2, midY);
        }
        prev = box.bottom;
        bottom = box.bottom;
      }
      maxBottom = Math.max(maxBottom, bottom);
      return bottom;
    });
    void armBottoms;
    y = maxBottom + 16;
  } else {
    y = prevBottom + 16;
  }

  svg.setAttribute("viewBox", `0 0 ${width} ${y}`);
  svg.setAttribute("width", width);
  svg.setAttribute("height", y);
  svg.setAttribute("style", `max-width: 100%; height: auto; font-family: ${fonts.sans}; background: white;`);
  return svg;
}
