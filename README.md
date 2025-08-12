# Rusty Sheet

A DuckDB extension that enables reading Excel and OpenDocument spreadsheet files directly within SQL queries. This extension provides seamless integration for analyzing spreadsheet data using DuckDB's powerful SQL engine.

[中文说明请点击这里](https://github.com/redraiment/rusty-sheet/blob/main/README.zh.md)

## Features

- **High Performance**: Optimized for large files; e.g., reading a 1M-row XLSX file on MacBook M1 now takes under 13 seconds (down from 140+ seconds).
- **Multiple Format Support**: Read Excel files (`.xls`, `.xlsx`, `.xlsm`, `.xlsb`, `.xla`, `.xlam`) and OpenDocument Spreadsheet files (`.ods`)
- **Flexible Data Types**: Support for boolean, integer, double, varchar, datetime, date, time, and interval (ISO 8601 duration) types
- **Excel-Style Data Ranges**: Specify data ranges using familiar Excel notation (e.g., `"A1:C3"`)
- **Automatic Column Type Detection**: Column types are inferred automatically; override specific columns with the `columns` parameter
- **Header Row Handling**: Automatic detection and parsing of header rows
- **Error Handling**: Configurable behavior for parsing errors with precise cell location reporting
- **Type Safety**: Built-in data type validation and conversion
- **Pure Rust Implementation**: No C++ dependencies, leveraging Rust's memory safety

## Installation

### Prerequisites

- Python 3
- Python 3-venv
- [Make](https://www.gnu.org/software/make)
- Git
- Rust toolchain

### Building from Source

1. Clone the repository:
```bash
git clone https://github.com/redraiment/rusty-sheet.git
cd rusty-sheet
````

2. Configure the build environment:

```bash
make configure
```

3. Build the extension:

```bash
make debug    # For development
make release  # For production
```

4. The built extension will be available in `build/debug/extension/` or `build/release/extension/`

## Usage

### Loading the Extension

Start DuckDB with the unsigned flag to load local extensions:

```bash
duckdb -unsigned
```

Load the extension:

```sql
LOAD './build/debug/extension/rusty-sheet/rusty-sheet.duckdb_extension';
```

### Basic Examples

#### Read entire spreadsheet with headers

```sql
SELECT * FROM read_sheet('data.xlsx');
```

#### Read specific worksheet

```sql
SELECT * FROM read_sheet('workbook.xlsx', sheet_name='Sheet2');
```

#### Override specific column types (others auto-detected)

```sql
SELECT * FROM read_sheet('data.xlsx',
  columns={'id': 'bigint'}
);
```

#### Read specific data range (Excel-style notation)

```sql
SELECT * FROM read_sheet('data.xlsx', range='A2:E100');
```

#### Read without headers

```sql
SELECT * FROM read_sheet('data.xlsx',
  header=false,
  columns={'column1': 'varchar', 'column2': 'bigint'}
);
```

### Analyze column types without reading full data

```sql
SELECT * FROM analyze_sheet('data.xlsx', analyze_rows=20);
```

## Parameters

### Positional Parameters

* `file_path` (required): Path to the spreadsheet file

### Named Parameters

| Parameter       | Type    | Default     | Description                                                     |
| --------------- | ------- | ----------- | --------------------------------------------------------------- |
| `sheet_name`    | VARCHAR | First sheet | Name of the worksheet to read                                   |
| `header`        | BOOLEAN | `true`      | Whether first row contains column headers                       |
| `columns`       | MAP     | `{}`        | Partial column type overrides, e.g., `{'id': 'bigint'}`         |
| `range`         | VARCHAR | Full sheet  | Data range in Excel format, e.g., `"A1:C3"`                     |
| `error_as_null` | BOOLEAN | `false`     | Convert parsing errors to NULL instead of failing               |
| `analyze_rows`  | INTEGER | `10`        | Number of rows to analyze for type inference                    |

### Supported Data Types

| Type       | DuckDB Type | Description                                  |
| ---------- | ----------- | -------------------------------------------- |
| `boolean`  | BOOLEAN     | True/false values                            |
| `bigint`   | BIGINT      | 64-bit signed integers                       |
| `double`   | DOUBLE      | Double-precision floating point              |
| `varchar`  | VARCHAR     | Variable-length strings                      |
| `datetime` | TIMESTAMP   | Date and time with microsecond precision     |
| `date`     | DATE        | Date without time component                  |
| `time`     | TIME        | Time without date component                  |
| `interval` | INTERVAL    | Time intervals, including ISO 8601 durations |

## Advanced Usage

### Error Handling

Handle parsing errors gracefully:

```sql
-- Convert errors to NULL values
SELECT * FROM read_sheet('messy_data.xlsx', error_as_null=true);
```

### Working with Multiple Sheets

```sql
-- Read from different sheets and union results
SELECT 'Q1' as quarter, * FROM read_sheet('sales.xlsx', sheet_name='Q1')
UNION ALL
SELECT 'Q2' as quarter, * FROM read_sheet('sales.xlsx', sheet_name='Q2')
UNION ALL
SELECT 'Q3' as quarter, * FROM read_sheet('sales.xlsx', sheet_name='Q3')
UNION ALL
SELECT 'Q4' as quarter, * FROM read_sheet('sales.xlsx', sheet_name='Q4');
```

### Data Analysis Examples

```sql
-- Calculate summary statistics
SELECT 
  COUNT(*) as total_records,
  AVG(score) as avg_score,
  MAX(created_at) as latest_entry
FROM read_sheet('student_data.xlsx');

-- Filter and aggregate data
SELECT 
  department,
  COUNT(*) as employee_count,
  AVG(salary) as avg_salary
FROM read_sheet('hr_data.xlsx')
WHERE salary > 50000
GROUP BY department
ORDER BY avg_salary DESC;
```

## Testing

Run the test suite:

```bash
# Test debug build
make test_debug

# Test release build  
make test_release
```

Test with different DuckDB versions:

```bash
make clean_all
DUCKDB_TEST_VERSION=v1.3.2 make configure
make debug
make test_debug
```

## Development

This extension is built using the DuckDB Rust extension framework. The main components are:

* `src/lib.rs`: Main extension implementation
* `test/sql/`: SQL test files
* `Cargo.toml`: Rust dependencies and build configuration

To contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run `make test_debug` to verify
5. Submit a pull request

## Known Issues

* On Windows with Python 3.11, you may encounter extension loading issues. Use Python 3.12 or later.
* Very large spreadsheets may require significant memory allocation.
* Complex Excel formulas are not evaluated; only the computed values are read.

## Author

**Zhang, Zepeng**
Email: [redraiment@gmail.com](mailto:redraiment@gmail.com)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

* Built on the [DuckDB Rust extension template](https://github.com/duckdb/duckdb-rs)
* Uses the [calamine](https://crates.io/crates/calamine) crate for spreadsheet parsing
* Inspired by DuckDB's commitment to making data analysis more accessible
