# DataFrame: My redundant .js pandas clone

What I've done here is waste a bunch of time... also, I've created a JavaScript implementation of a pandas-like DataFrame for data manipulation and analysis in JavaScript. Once again, yes. There are already pretty good implementations of this, like [`arquero`](https://idl.uw.edu/arquero/). However:

1. I wanted to implement my own minimal version and nothing teaches you like implementing your own stuff.
2. I didn't think arquero was python-esque enough for me.
3. I don't expect anyone else to use this stuff except me.

With Claude and OpenAI these days, it's easier than ever to _learn_, which is exactly why I've done what I've done. As I really am starting to love TypeScript (save for the analysis part), this implementation aims to provide a Python/pandas-like experience in JavaScript, making data manipulation and analysis more intuitive for Python developers, like me.

---

## Table of Contents

1. [Quickstart](#quickstart)
   1. [Import and Create](#import-and-create)
   2. [Basic Inspection](#basic-inspection)
   3. [Selection & Filtering](#selection--filtering)
   4. [Statistical Analysis](#statistical-analysis)
   5. [Group Operations](#group-operations)
2. [Key Methods Reference](#key-methods-reference)
   1. [Basic Operations](#basic-operations)
   2. [Data Access & Transformation](#data-access--transformation)
   3. [Statistics](#statistics)
   4. [Grouping](#grouping)
   5. [Iteration Methods](#iteration-methods)
3. [Advanced Features](#advanced-features)
   1. [Performance Comparison](#performance-comparison)
   2. [Concurrent Processing](#concurrent-processing)
   3. [Correlation Plot](#corrplot)
4. [Method Breakdown](#method-breakdown)

---

```js
// Run code
import * as d3 from "https://cdn.jsdelivr.net/npm/d3@7/+esm"
import { require } from "d3-require"
const jStat = await require("jstat@1.9.4")
const math = await require("mathjs@9.4.2")
import {
  generateTestData,
  runTest,
  comparePerformance,
} from "./components/DataFrame.js"
```

## Quickstart

Install:

- `npm install git+https://github.com/eli-s-goldberg/observable-dataframe`

### Import and Create

```javascript
// Show code
import { DataFrame } from "./components/DataFrame.js"
const nations = await FileAttachment("./data/nations.csv").csv({ typed: true })
const df = new DataFrame(nations)
```

```js
// Run code
import { DataFrame } from "./components/DataFrame.js"
const nations = await FileAttachment("./data/nations.csv").csv({ typed: true })
const df = new DataFrame(nations)
```

### Basic Inspection

```javascript
// Show code
view(df)
view(df.table())
view(df.describe().table())
```

Let's view the data and the table in its entirety.

```js
// Run code
view(df)
view(df.table())
```

Let's use the describe function and turn that into a table.

```js
view(df.describe().table())
```

---

## Selection & Filtering

```javascript
// Show code
const highIncome = df
  .query("row.income > 20000")
  .sort_values("population", false)
  .head(5)

view(highIncome.table())
```

```js
// Run code
const highIncome = df
  .query("row.income > 20000")
  .sort_values("population", false)
  .head(5)

view(highIncome.table())
```

Cool. That's great, but there are more than one entry per country, as it's a time series. Let's do a little pythonic manipulation to get just the last pop for each country. Here's the basic way:

```javascript
// Show code
const highIncomeUnique = []
for (const [key, group] of df.groupby("name", { iterable: true })) {
  const data = group.dropna({ axis: 0, subset: ["population"] })
  const lastYear = d3.max(data._data.year)
  const lastYearData = data.query(`row.year === ${lastYear}`)
  highIncomeUnique.push({
    ...key,
    year: lastYear,
    income: lastYearData._data.income[0],
    population: lastYearData._data.population[0],
    region: lastYearData._data.region[0],
  })
}
const highIncomeUniquedf = new DataFrame(highIncomeUnique).sort_values(
  "population",
  false
)
view(highIncomeUniquedf.table())
```

```js
// Run code
const highIncomeUnique = []
for (const [key, group] of df.groupby("name", { iterable: true })) {
  const data = group.dropna({ axis: 0, subset: ["population"] })
  const lastYear = d3.max(data._data.year)
  const lastYearData = data.query(`row.year === ${lastYear}`)
  highIncomeUnique.push({
    ...key,
    year: lastYear,
    income: lastYearData._data.income[0],
    population: lastYearData._data.population[0],
    region: lastYearData._data.region[0],
  })
}
const highIncomeUniquedf = new DataFrame(highIncomeUnique).sort_values(
  "population",
  false
)
view(highIncomeUniquedf.table())
```

Here’s an `agg` version, because it's way nicer to write.

```javascript
// Show code
const highIncomeUnique2 = df
  .dropna({ axis: 0, subset: ["population"] })
  .groupby(["name"])
  .agg({
    year: ["last"],
    income: ["last"],
    population: ["last"],
    region: ["last"],
  })
  .query("row.income > 350")
  .sort_values("population", false)
  .head(20)

view(highIncomeUnique2.table())
```

```js
// Run code
const highIncomeUnique2 = df
  .dropna({ axis: 0, subset: ["population"] })
  .groupby(["name"])
  .agg({
    year: ["last"],
    income: ["last"],
    population: ["last"],
    region: ["last"],
  })
  .query("row.income > 350")
  .sort_values("population", false)
  .head(20)

view(highIncomeUnique2.table())
```

---

## Statistical Analysis

```javascript
// Show code
view(df.corr("income", "population"))

const width_plot = 600
view(
  df.corrPlot({
    width: width_plot,
    height: (width_plot * 240) / 300,
    marginTop: 100,
  })
)
```

```js
// Run code
view(df.corr("income", "population"))

const width_plot = 600
view(
  df.corrPlot({
    width: width_plot,
    height: (width_plot * 240) / 300,
    marginTop: 100,
  })
)
```

### Group Operations

```javascript
// Show code
const incomeSummary = df.groupby(["name", "region"]).agg({
  population: ["min", "mean", "max"],
  lifeExpectancy: ["mean", "max"],
})

view(incomeSummary.table())
```

```js
// Run code
const incomeSummary = df.groupby(["name", "region"]).agg({
  population: ["min", "mean", "max"],
  lifeExpectancy: ["mean", "max"],
})

view(incomeSummary.table())
```

---

## Key Methods Reference

### Basic Operations

```javascript
df.head(n)
df.tail(n)
df.print()
df.table()
```

### Data Access & Transformation

```javascript
df.select(["colA", "colB"])
df.query("row.colA > 100")
df.sort_values("colA", true)
df.fillna(0)
df.apply("colA", (x) => x * 2)
df.map("colA", { oldValue: "newValue" })
```

### Statistics

```javascript
df.describe()
df.corr("colA", "colB")
df.corrMatrix()
df.corrPlot({ width: 500, height: 500 })
```

### Grouping

```javascript
df.groupby(["colA"]).agg({ colB: ["min", "max", "mean"] })
df.concurrentGroupBy(["colA", "colC"], { colB: ["min", "mean"] })
```

### Iteration Methods

```javascript
for (const [idx, row] of df.iterrows()) {
  /* ... */
}
for (const tuple of df.itertuples("Record")) {
  /* ... */
}
for (const [col, values] of df.items()) {
  /* ... */
}
for (const row of df) {
  /* ... */
}
```

---

### describe()

Generate descriptive statistics for all columns. Returns a DataFrame containing statistics like count, mean, std, min/max, and percentiles for numeric columns, and category distributions for non-numeric columns.

```javascript
// Get basic statistics
const stats = df.describe()

// Print the statistics in a formatted way
stats.print()

// Access category distributions for non-numeric columns
const categories = stats._data.categories

// Render as an HTML table using Observable's Inputs.table
Inputs.table(stats.print())
```

_Statistics provided:_

- **Numeric columns:** count, mean, std, min, 25%, 50%, 75%, max
- **Categorical columns:** count, unique values, top values, frequency, category distribution

### percentile(p[, columns])

Calculate percentile value(s) for numeric columns.

```javascript
// 75th percentile of age
const p75 = df.percentile(0.75, "age")

// Multiple columns
const quartiles = df.percentile(0.25, ["age", "salary"])
```

---

## Data Manipulation

### fillna(value)

Fill missing values in the DataFrame.

```javascript
const filled = df.fillna(0)
```

### apply(column, func)

Apply a function to a column.

```javascript
const df2 = df.apply("name", (name) => name.toUpperCase())
```

### map(column, mapper)

Map values in a column using a mapping object.

```javascript
const df2 = df.map("status", {
  A: "Active",
  I: "Inactive",
})
```

### drop() & dropna()

Remove specified columns or rows with missing values.

```javascript
const df2 = df.drop(["temp_col", "unused_col"])
```

```javascript
const df2 = df.dropna({ axis: 0, subset: ["population"] })
```

### rename(columnMap)

Rename columns using a mapping object.

```javascript
const df2 = df.rename({
  old_name: "new_name",
  prev_col: "next_col",
})
```

### assign(columnName, values)

Add a new column.

```javascript
// Add column with array
const df2 = df.assign("new_col", [1, 2, 3])

// Add column with function
const df3 = df.assign("bmi", (row) => row.weight / (row.height * row.height))
```

### merge(other[, options])

Merge two DataFrames.

```javascript
const merged = df1.merge(df2, {
  on: "id", // Join on same column name
  how: "inner", // Join type
  left_on: "id_1", // Custom left join column
  right_on: "id_2", // Custom right join column
})
```

---

## Mathematical Operations

### add(other)

Add scalar or DataFrame to numeric columns.

```javascript
// Add scalar
const df2 = df.add(10)

// Add DataFrame
const df3 = df1.add(df2)
```

### sub(other)

Subtract scalar or DataFrame from numeric columns.

```javascript
const df2 = df.sub(5)
```

### mul(other)

Multiply numeric columns by scalar or DataFrame.

```javascript
const df2 = df.mul(2)
```

### div(other)

Divide numeric columns by scalar or DataFrame.

```javascript
const df2 = df.div(100)
```

---

## Advanced Features

### Performance Comparison

Here's a comparison of regular and concurrent groupby performance on different dataset sizes. Spoiler: concurrency overhead is real, so if your DataFrame is tiny, you’re just wasting CPU cycles. However, we do see some good speedup for larger dataframes. On my computer (M2 Air), I can groupby aggregate 10M rows in ~1.2 seconds or less.

```javascript
const results = await comparePerformance()
view(results.table())
```

```js
// Run code
const results = await comparePerformance()
view(results.table())
```

### Concurrent Processing

```javascript
const regularGroupBy = df.groupby(["name", "region"]).agg({
  population: ["min", "mean", "max"],
})

const concurrentResult = await df.concurrentGroupBy(["name", "region"], {
  population: ["min", "mean", "max"],
})
```

```js
// Run code
const regularGroupBy = df.groupby(["name", "region"]).agg({
  population: ["min", "mean", "max"],
})

const concurrentResult = await df.concurrentGroupBy(["name", "region"], {
  population: ["min", "mean", "max"],
})
```

### corrPlot

```javascript
// Show code
const plot = df.corrPlot({
  width: 600,
  height: 480,
  marginTop: 100,
  scheme: "blues",
  decimals: 2,
})
const corrMatrix = df.corrMatrix()
const corrData = corrMatrix._data
```

```js
// Run code
const plot = df.corrPlot({
  width: 600,
  height: 480,
  marginTop: 100,
  scheme: "blues",
  decimals: 2,
})
const corrMatrix = df.corrMatrix()
const corrData = corrMatrix._data
```

---

## Method Breakdown

- **constructor(data, options)**  
  Initialize a new DataFrame from an array of objects or column-based data.

- **append(row)**  
  Append a single row to the DataFrame.

- **extend(rows)**  
  Append multiple rows at once.

- **head(n)**  
  Return the first _n_ rows.

- **tail(n)**  
  Return the last _n_ rows.

- **select(columns)**  
  Return a new DataFrame with only the specified columns.

- **filter()**  
  Return a QueryBuilder instance for filtering rows.

- **loc(condition)**  
  Filter rows using a condition function.

- **query(expr)**  
  Filter rows based on a string expression.

- **groupby(columns, options)**  
  Group rows by one or more columns; supports aggregation.

- **max(columns)**  
  Get the maximum value(s) from a column or columns.

- **min(columns)**  
  Get the minimum value(s) from a column or columns.

- **table(options)**  
  Return the DataFrame as an HTML table (using Observable's Inputs).

- **to_data()**  
  Convert DataFrame data to an array of objects.

- **sort_values(columns, ascending)**  
  Sort the DataFrame by the specified columns.

- **corr(col1, col2)**  
  Calculate the correlation coefficient between two columns.

- **corrMatrix()**  
  Generate a correlation matrix for numeric columns.

- **describe()**  
  Generate descriptive statistics for numeric and non-numeric columns.

- **setType(column, type)**  
  Set the data type for a specific column.

- **setTypes(typeMap)**  
  Set data types for multiple columns.

- **corrPlot(options)**  
  Generate a correlation plot.

- **percentile(p, columns)**  
  Calculate the percentile value(s) for numeric columns.

- **iterrows()**  
  Iterator over [index, row] pairs.

- **itertuples(name)**  
  Iterator over row tuples with an optional name.

- **items()**  
  Iterator over [column, values] pairs.

- **[Symbol.iterator]()**  
  Iterate directly over rows.

- **print(options)**  
  Return a printed representation of the DataFrame.

- **terminate()**  
  Terminate any active worker processes.

- **with_columns(columnExpressions)**  
  Add or modify columns using functions or literal expressions.

- **merge(other, options)**  
  Merge with another DataFrame based on join conditions.

- **mean(columns)**  
  Compute the mean of numeric columns.

- **sum(columns)**  
  Compute the sum of numeric columns.

- **value_counts(column)**  
  Count unique values in a column.

- **fillna(value)**  
  Replace missing values in the DataFrame.

- **apply(column, func)**  
  Apply a function to a column.

- **map(column, mapper)**  
  Map values in a column using a provided mapping.

- **drop(columns)**  
  Remove specified columns.

- **dropna(options)**  
  Remove rows or columns with missing values.

- **rename(columnMap)**  
  Rename columns using a mapping object.

- **assign(columnName, values)**  
  Add a new column to the DataFrame.

- **add(other)**  
  Add a scalar or DataFrame to numeric columns.

- **sub(other)**  
  Subtract a scalar or DataFrame from numeric columns.

- **mul(other)**  
  Multiply numeric columns by a scalar or another DataFrame.

- **div(other)**  
  Divide numeric columns by a scalar or another DataFrame.

- **index()**  
  Return an array of row indices.
