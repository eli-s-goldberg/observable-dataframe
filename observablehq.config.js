// Observable Framework configuration for the observable-dataframe docs site.
// See https://observablehq.com/framework/config
export default {
  title: "observable-dataframe",
  root: "docs",
  pages: [
    { name: "Getting started", path: "/index" },
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
    { name: "Plot gallery", path: "/gallery" },
  ],
  search: true,
  typographer: true,
  footer: "observable-dataframe: data science in the browser, no server harmed.",
};
