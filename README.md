# DataFrame: My redundant .js pandas clone

What I've done here is waste a bunch of time... also, I've created a JavaScript implementation of a pandas-like DataFrame for data manipulation and analysis in JavaScript. Once again, yes. There are already pretty good implementations of this, like [`arquero`](https://idl.uw.edu/arquero/). However:

1. I wanted to implement my own minimal version and nothing teaches you like implementing your own stuff.
2. I didn't think arquero was python-esque enough for me.
3. I don't expect anyone else to use this stuff except me.

With Claude and OpenAI these days, it's easier than ever to _learn_, which is exactly why I've done what I've done. As I really am starting to love TypeScript (save for the analysis part), this implementation aims to provide a Python/pandas-like experience in JavaScript, making data manipulation and analysis more intuitive for Python developers, like me.

---

# Observable DataFrame

A JavaScript implementation of a pandas-like DataFrame for data manipulation and analysis. This library provides a Python/pandas-like experience in JavaScript, making data transformation and analysis more intuitive.

## Installation

```bash
npm install observable-dataframe
```

## Table of Contents

1. [Basic Usage](#basic-usage)
   - [Creating DataFrames](#creating-dataframes)
   - [Viewing Data](#viewing-data)
   - [Basic Operations](#basic-operations)
2. [Data Manipulation](#data-manipulation)
   - [Column Types and Transformations](#column-types-and-transformations)
   - [Adding or Modifying Columns](#adding-or-modifying-columns)
   - [Filtering Data](#filtering-data)
   - [Sorting Data](#sorting-data)
3. [Aggregation Operations](#aggregation-operations)
   - [Basic Aggregation](#basic-aggregation)
   - [Grouping Data](#grouping-data)
   - [Time Window Aggregation](#time-window-aggregation)
4. [Statistical Analysis](#statistical-analysis)
   - [Descriptive Statistics](#descriptive-statistics)
   - [Correlation Analysis](#correlation-analysis)
5. [Advanced Features](#advanced-features)
   - [Concurrent Processing](#concurrent-processing)
   - [Iteration Methods](#iteration-methods)
   - [Mathematical Operations](#mathematical-operations)
6. [Method Reference](#method-reference)

## Basic Usage

### Creating DataFrames

```javascript
import { DataFrame } from "observable-dataframe";

// From array of objects (CSV-like data)
const data = [
  { name: "Alice", age: 30, city: "New York" },
  { name: "Bob", age: 25, city: "Los Angeles" },
  { name: "Charlie", age: 35, city: "Chicago" },
];
const df = new DataFrame(data);

// From CSV file (using FileAttachment in Observable)
const csvData = await FileAttachment("./data/mydata.csv").csv();
const df = new DataFrame(csvData);

// From column-based data
const columnData = {
  name: ["Alice", "Bob", "Charlie"],
  age: [30, 25, 35],
  city: ["New York", "Los Angeles", "Chicago"],
};
const df = new DataFrame(columnData);
```

### Viewing Data

```javascript
// Get first few rows
df.head(3);

// Get last few rows
df.tail(3);

// Basic view (in Observable)
df.table();

// Print with options
df.print({ max_rows: 10, precision: 2 });
```

### Basic Operations

```javascript
// Select specific columns
df.select(["name", "age"]);

// Basic information
df.describe().table();

// Get raw data
df.to_data();
```

## Data Manipulation

### Column Types and Transformations

```javascript
// Set types for columns
df.setType("age", "number");
df.setTypes({ age: "number", timestamp: "date" });

// Apply function to a column
df.apply("name", (name) => name.toUpperCase());

// Map values in a column
df.map("status", { A: "Active", I: "Inactive" });
```

### Adding or Modifying Columns

```javascript
// Add new columns with math operations
df.with_columns({
  // Using functions that work on each row
  adult: (row) => (row.age >= 18 ? "Yes" : "No"),
  name_length: (row) => row.name.length,

  // Using simple calculations
  age_in_months: (row) => row.age * 12,

  // Adding constant values
  cohort: "2023",
});

// Another way to add columns
df.assign("adult", (row) => (row.age >= 18 ? "Yes" : "No"));
```

#### Advanced Column Transformations

The `.with_columns()` method is powerful for data transformation and can be combined with other methods for complex workflows:

```javascript
// Load dataset of medical events
const events_df = new DataFrame(medical_events);

// Calculate derived risk measures
const risk_df = events_df
  .filter()
  .gt("baseline_measurement", 0) // Filter for valid baseline
  .execute()
  .with_columns({
    // Calculate absolute risk
    absolute_risk: (row) => (row.events / row.population) * 100,

    // Calculate risk ratios compared to baseline
    risk_ratio: (row) =>
      row.events /
      row.population /
      (row.baseline_events / row.baseline_population),

    // Flag high-risk events
    high_risk: (row) =>
      row.events / row.population > 0.05 ? "High" : "Normal",

    // Calculate risk-adjusted costs
    adjusted_cost: (row) => row.cost * Math.sqrt(row.risk_score),
  });

// Chain with groupby for risk stratification
const risk_strata = risk_df
  .groupby(["region", "high_risk"])
  .agg({
    absolute_risk: ["mean", "max"],
    adjusted_cost: ["sum", "mean"],
  })
  .with_columns({
    // Add cost per risk unit
    cost_per_risk_unit: (row) => row.adjusted_cost_sum / row.absolute_risk_mean,
  });
```

### Filtering Data

The DataFrame offers multiple ways to filter data:

#### Using filter() method with chainable conditions

```javascript
// Filter with multiple conditions
const filteredData = df
  .filter()
  .gt("age", 25) // age > 25
  .lt("income", 100000) // income < 100000
  .neq("status", "Inactive") // status != "Inactive"
  .execute(); // Don't forget to execute!

// More complex filtering
const results = df
  .filter()
  .lte("P Value", 0.05) // P Value <= 0.05
  .lte("Absolute Prevalence DiD", 0) // Correctly signed
  .notNull("Prevalence Measurement Period (Comparator)") // Not null values
  .execute()
  .with_columns({
    // Add calculated columns to filtered results
    absolute_events: (row) => row["Prevalence"] * population,
    events_prevented: (row) =>
      row["Prevalence"] * population -
      row["Prevalence"] * population * (1 + row["Relative DiD"]),
  });
```

#### Available filter conditions

```javascript
// Comparison operators
df.filter()
  .gt("column", value) // Greater than
  .lt("column", value) // Less than
  .gte("column", value) // Greater than or equal
  .lte("column", value) // Less than or equal
  .eq("column", value) // Equal to
  .neq("column", value) // Not equal to
  .between("column", lower, upper) // Between values (inclusive)

  // Null/NaN handling
  .isNull("column") // Is null or undefined
  .notNull("column") // Is not null or undefined
  .notNan("column") // Is not NaN (for numeric columns)

  // Set membership
  .isIn("column", [values]) // Value is in array

  // Custom conditions
  .where((row) => customLogic(row)) // Custom function

  // Execute the filter
  .execute();
```

#### Using query() with expression strings

```javascript
// Simple querying with strings
df.query("row.age > 30");
df.query("row.city === 'New York' && row.age < 40");
```

#### Using loc() with functions

```javascript
// Filtering with a function
df.loc((row) => row.age > 30 && row.city.startsWith("New"));
```

### Sorting Data

```javascript
// Sort by a column (ascending)
df.sort_values("age");

// Sort by a column (descending)
df.sort_values("age", false);

// Sort by multiple columns
df.sort_values(["city", "age"]);

// Sort by multiple columns with different directions
df.sort_values(["city", "age"], [true, false]); // city ascending, age descending
```

## Aggregation Operations

### Basic Aggregation

```javascript
// Sum of a column
df.sum("age");

// Mean of a column
df.mean("age");

// Multiple column aggregation
df.sum(["age", "income"]);
df.mean(["age", "income"]);

// Get unique values
df.unique("city");
df.unique(["city", "status"]);

// Count values
df.value_counts("city");
```

### Grouping Data

```javascript
// Basic groupby with aggregation
const summary = df.groupby(["city"]).agg({
  age: ["min", "mean", "max", "sum"],
  income: "mean",
});

// Multiple groupby columns
const detailedSummary = df.groupby(["city", "gender"]).agg({
  age: ["min", "max", "mean"],
  income: ["mean", "sum"],
});

// Other aggregation types
const aggResult = df.groupby(["city"]).agg({
  status: ["first", "last", "nunique"], // first/last value, number of unique values
  name: ["concat"], // concatenate strings
});

// Iterating through groups
for (const [key, group] of df.groupby("city", { iterable: true })) {
  console.log(`City: ${key.city}`);
  console.log(`Average age: ${group.mean("age")}`);
}
```

#### Available Aggregation Functions

Both `groupby()` and `groupby_dynamic()` support these aggregation functions:

| Aggregation                   | Description                | Works With      |
| ----------------------------- | -------------------------- | --------------- |
| `"min"`                       | Minimum value              | Numeric columns |
| `"max"`                       | Maximum value              | Numeric columns |
| `"mean"`                      | Average value              | Numeric columns |
| `"sum"`                       | Sum of values              | Numeric columns |
| `"first"`                     | First value in group       | Any column type |
| `"last"`                      | Last value in group        | Any column type |
| `"nunique"`                   | Count of unique values     | Any column type |
| `"concat"`                    | Concatenate string values  | String columns  |
| `"nested_concat"`             | Returns [head, tail] array | Any column type |
| `"nested_concat_array"`       | Groups identical values    | Any column type |
| `"nested_concat_array_count"` | Count of identical values  | Any column type |

Example using each aggregation type:

```javascript
// Create a DataFrame with various data types
const sales_df = new DataFrame([
  {
    date: "2023-01-01",
    region: "North",
    product: "A",
    quantity: 10,
    price: 25.5,
    tags: "sale,featured",
  },
  {
    date: "2023-01-02",
    region: "North",
    product: "B",
    quantity: 15,
    price: 19.99,
    tags: "new",
  },
  {
    date: "2023-01-01",
    region: "South",
    product: "A",
    quantity: 8,
    price: 24.99,
    tags: "sale",
  },
  {
    date: "2023-01-02",
    region: "South",
    product: "C",
    quantity: 20,
    price: 34.5,
    tags: "featured",
  },
  {
    date: "2023-01-03",
    region: "North",
    product: "A",
    quantity: 12,
    price: 25.0,
    tags: "sale",
  },
]);

// Use various aggregation types
const aggregated = sales_df.groupby(["region", "product"]).agg({
  quantity: ["min", "max", "mean", "sum"], // Numeric aggregations
  price: ["mean", "min"], // More numeric aggregations
  date: ["first", "last"], // First/last values
  tags: ["nunique", "concat"], // Count unique, concatenate strings
  product: ["nested_concat", "nested_concat_array"], // Nested aggregations
});

/*
Results include:
- quantity_min, quantity_max, quantity_mean, quantity_sum
- price_mean, price_min
- date_first, date_last
- tags_nunique, tags_concat
- product_nested_concat, product_nested_concat_array
*/
```

The `_performAggregation` method internally handles these aggregation types by:

1. Initializing aggregation accumulators for each group
2. Processing each row and updating the accumulators
3. Calculating final results based on accumulated values
4. Formatting the output as a new DataFrame

### Time Window Aggregation

The `groupby_dynamic()` method provides powerful time-series analysis capabilities by grouping data into time windows.

```javascript
// Time-based window aggregation
const timeSeriesDf = df.groupby_dynamic(
  "timestamp", // Column with timestamps
  {
    value: ["mean", "sum"], // Columns to aggregate
  },
  {
    every: "1d", // Window step size
    period: "7d", // Window size
    offset: "0h", // Time offset
    closed: "left", // Window boundary inclusion
    label: "left", // Window labeling strategy
    include_boundaries: true, // Include window bounds in output
    groupby_start_date: "day", // Align windows to calendar units
  }
);
```

#### Dynamic Time Window Example

Here's a real-world example analyzing sensor data with rolling time windows:

```javascript
// Sensor data with timestamps
const sensor_df = new DataFrame(sensorData);

// Set column types
sensor_df.setTypes({
  timestamp: "date",
  temperature: "number",
  humidity: "number",
  pressure: "number",
});

// Calculate hourly rolling windows with 10-minute steps
const hourly_stats = sensor_df.groupby_dynamic(
  "timestamp", // Time column
  {
    temperature: ["min", "mean", "max"],
    humidity: ["mean", "max"],
    pressure: ["mean"],
  },
  {
    every: "10m", // Step forward every 10 minutes
    period: "1h", // Use 1 hour windows
    offset: "0m", // No offset
    closed: "left", // Include left boundary, exclude right
    label: "left", // Label windows by start time
    include_boundaries: true,
    groupby_start_date: "minute", // Align to calendar minutes
  }
);

// Calculate day-over-day changes using window aggregation
const daily_changes = sensor_df
  .groupby_dynamic(
    "timestamp",
    {
      temperature: ["mean", "first", "last"],
      humidity: ["mean"],
    },
    {
      every: "1d", // Daily windows
      period: "1d", // 1 day window size
    }
  )
  .with_columns({
    // Calculate temperature change
    temp_change: (row) => row.temperature_last - row.temperature_first,

    // Calculate vs 24 hours before (if data available)
    day_over_day_temp: (row) => {
      // Logic to compare with previous day's data
      return row.temperature_mean - previous_day_temp;
    },
  });
```

## Statistical Analysis

### Descriptive Statistics

```javascript
// Get detailed statistics
const stats = df.describe();

// Access specific statistics
const mean = df.mean("age");
const maximum = df.max("age");
const minimum = df.min("age");

// Percentiles
const median = df.percentile(0.5, "age");
const quartiles = df.percentile(0.25, ["age", "income"]);
```

### Correlation Analysis

```javascript
// Correlation between two columns
const correlation = df.corr("age", "income");

// Correlation matrix for all numeric columns
const corrMatrix = df.corrMatrix();

// Visualize correlation matrix
const plot = df.corrPlot({
  width: 600,
  height: 600,
  marginTop: 100,
  scheme: "blues", // Color scheme
  decimals: 2, // Decimal places to display
});
```

## Advanced Features

### Concurrent Processing

```javascript
// Regular groupby
const regularResult = df.groupby(["state", "city"]).agg({
  population: ["min", "mean", "max"],
  income: "mean",
});

// Concurrent groupby for larger datasets
const concurrentResult = await df.concurrentGroupBy(["state", "city"], {
  population: ["min", "mean", "max"],
  income: "mean",
});

// Compare performance
const perfResults = await comparePerformance();
```

### Iteration Methods

```javascript
// Iterate row by row with index
for (const [index, row] of df.iterrows()) {
  console.log(`Row ${index}:`, row);
}

// Iterate with named tuples
for (const row of df.itertuples("Record")) {
  console.log(`${row.name} is ${row.age} years old`);
}

// Iterate column by column
for (const [column, values] of df.items()) {
  console.log(`Column ${column}:`, values);
}

// Direct iteration over rows
for (const row of df) {
  console.log(row);
}
```

### Mathematical Operations

```javascript
// Add a scalar
const df2 = df.add(10); // Add 10 to all numeric columns

// Subtract a scalar
const df3 = df.sub(5); // Subtract 5 from all numeric columns

// Multiply
const df4 = df.mul(2); // Double all numeric columns

// Divide
const df5 = df.div(100); // Divide all numeric columns by 100

// DataFrame math - add two DataFrames
const dfSum = df1.add(df2);
```

## Method Reference

- **Basic Operations**

  - `constructor(data, options)` - Create a new DataFrame
  - `head(n)` - Get first n rows
  - `tail(n)` - Get last n rows
  - `select(columns)` - Select specified columns
  - `print(options)` - Format data for printing
  - `table(options)` - Return an HTML table view
  - `to_data()` - Convert to array of objects

- **Data Manipulation**

  - `filter()` - Chain filtering operations
  - `loc(condition)` - Filter with a function
  - `query(expr)` - Filter with a string expression
  - `setType(column, type)` - Set column type
  - `setTypes(typeMap)` - Set multiple column types
  - `with_columns(columnExpressions)` - Add/modify columns
  - `assign(columnName, values)` - Add a new column
  - `drop(columns)` - Remove columns
  - `dropna(options)` - Remove rows with missing values
  - `fillna(value)` - Replace missing values
  - `rename(columnMap)` - Rename columns
  - `apply(column, func)` - Apply function to column
  - `map(column, mapper)` - Map values in column
  - `sort_values(columns, ascending)` - Sort data
  - `merge(other, options)` - Join two DataFrames

- **Aggregation**

  - `groupby(columns, options)` - Group by columns
  - `groupby_dynamic(timeColumn, aggSpec, options)` - Time-based grouping
  - `concurrentGroupBy(columns, aggregations)` - Parallel grouping
  - `unique(columns)` - Get unique values
  - `mean(columns)` - Calculate mean
  - `sum(columns)` - Calculate sum
  - `max(columns)` - Get maximum values
  - `min(columns)` - Get minimum values
  - `value_counts(column)` - Count unique values

- **Statistics**

  - `describe()` - Generate descriptive statistics
  - `percentile(p, columns)` - Calculate percentiles
  - `corr(col1, col2)` - Calculate correlation
  - `corrMatrix()` - Generate correlation matrix
  - `corrPlot(options)` - Visualize correlation matrix

- **Math Operations**

  - `add(other)` - Add scalar or DataFrame
  - `sub(other)` - Subtract scalar or DataFrame
  - `mul(other)` - Multiply by scalar or DataFrame
  - `div(other)` - Divide by scalar or DataFrame

- **Iteration**

  - `iterrows()` - Iterate over [index, row] pairs
  - `itertuples(name)` - Iterate over row tuples
  - `items()` - Iterate over [column, values] pairs
  - `[Symbol.iterator]()` - Direct iteration over rows

- **Other**
  - `append(row)` - Add a single row
  - `extend(rows)` - Add multiple rows
  - `terminate()` - Clean up workers

## Real-World Examples

### Data Processing & Analysis

```javascript
import { DataFrame } from "observable-dataframe";

// Load data
const va_data = await FileAttachment("./data/veterans_data.csv").csv();
const va_df = new DataFrame(va_data);

// Ensure numeric columns
va_df.setType("Veterans", "number");

// Create derivative columns
va_df.with_columns({
  belle_targets: (row) => row.Veterans * 0.5,
  remaining_vets: (row) => row.Veterans * 0.5,
});

// Aggregate by state
const state_summary = va_df.groupby(["Abbreviation"]).agg({
  Veterans: "sum",
  belle_targets: "sum",
  remaining_vets: "sum",
});

// Get total for further calculations
const veterans_sum = state_summary.sum(["Veterans_sum"]) / 2;
```

### Healthcare Data Analysis

```javascript
// Load and prepare study data
const study_df = new DataFrame(clinical_data);

// Calculate derived metric
const engaged_population =
  (((population * at_risk_fraction) / 100) * engagement_rate) / 100;

// Filter for significant results and add impact metrics
const significant_results = study_df
  .filter()
  .lte("P Value", 0.05) // Statistically significant
  .lte("Absolute Prevalence DiD", 0) // Correctly signed effect
  .notNull("Prevalence Measurement Period (Comparator)") // Valid data
  .execute()
  .with_columns({
    // Calculate impact metrics
    absolute_events: (row) =>
      row["Prevalence Measurement Period (Comparator)"] * engaged_population,

    absolute_events_with_intervention: (row) =>
      row["Prevalence Measurement Period (Comparator)"] *
      engaged_population *
      (1 + row["Relative DiD"]),

    events_prevented: (row) =>
      row["Prevalence Measurement Period (Comparator)"] * engaged_population -
      row["Prevalence Measurement Period (Comparator)"] *
        engaged_population *
        (1 + row["Relative DiD"]),
  });
```
