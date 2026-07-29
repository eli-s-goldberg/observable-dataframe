// Observable Framework configuration for the observable-dataframe docs site.
// See https://observablehq.com/framework/config
export default {
  title: "observable-dataframe",
  root: "docs",
  // Served from https://eli-s-goldberg.github.io/observable-dataframe/. Asset and
  // import URLs are emitted relative to each page, so the subpath needs no help;
  // `base` is only consumed by the <base href> on a custom 404 page.
  base: "/observable-dataframe/",
  pages: [
    { name: "Getting started", path: "/index" },
    { name: "Plot gallery", path: "/gallery" },
    { name: "Benchmarks", path: "/benchmarks" },
    {
      name: "Guides",
      open: true,
      pages: [
        { name: "The DataFrame", path: "/dataframe" },
        { name: "Panel data & DiD", path: "/statistics" },
        { name: "Healthcare data panel", path: "/data-panel" },
        { name: "Module catalog", path: "/modules" },
      ],
    },
    {
      name: "API reference",
      open: true,
      pages: [
        { name: "DataFrame & GroupBy", path: "/api/dataframe" },
        { name: "Expressions", path: "/api/expressions" },
        { name: "Column & IO", path: "/api/column-io" },
        { name: "Extension contract", path: "/api/extension-contract" },
        { name: "Statistics", path: "/api/stats" },
        { name: "Data", path: "/api/data" },
        { name: "Plots", path: "/api/plots" },
        { name: "Layouts", path: "/api/layouts" },
      ],
    },
  ],
  search: true,
  typographer: true,
  footer: "observable-dataframe: data science in the browser, no server harmed.",
};
