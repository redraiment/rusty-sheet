# Rusty Sheet

A DuckDB extension that enables reading Excel and OpenDocument spreadsheet files directly within SQL queries. This extension provides seamless integration for analyzing spreadsheet data using DuckDB's powerful SQL engine.

[中文说明请点击这里](https://github.com/redraiment/rusty-sheet/blob/main/README.zh.md)

## Features

- **Multiple Format Support**: Read Excel files (`.xls`, `.xlsx`, `.xlsm`, `.xlsb`, `.xla`, `.xlam`) and OpenDocument Spreadsheet files (`.ods`)
- **Flexible Data Types**: Support for boolean, integer, double, varchar, datetime, date, and time data types
- **Custom Data Ranges**: Specify exact row and column ranges for data extraction
- **Header Row Handling**: Automatic detection and parsing of header rows
- **Error Handling**: Configurable behavior for empty cells and parsing errors
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

1. Clone the repository with submodules:
```bash
git clone --recurse-submodules https://github.com/redraiment/rusty-sheet.git
cd rusty-sheet
```

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

#### Define custom column types
```sql
SELECT * FROM read_sheet('data.xlsx',
  fields=[
    ['id', 'bigint'],
    ['name', 'varchar'], 
    ['score', 'double'],
    ['created_at', 'datetime']
  ]
);
```

#### Read specific data range
```sql
SELECT * FROM read_sheet('data.xlsx',
  start_row=2,
  end_row=100,
  start_column=1,
  end_column=5
);
```

#### Read without headers
```sql
SELECT * FROM read_sheet('data.xlsx',
  header=false,
  fields=[['col1', 'varchar'], ['col2', 'bigint']]
);
```

## Parameters

### Positional Parameters
- `file_path` (required): Path to the spreadsheet file

### Named Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sheet_name` | VARCHAR | First sheet | Name of the worksheet to read |
| `header` | BOOLEAN | `true` | Whether first row contains column headers |
| `fields` | LIST | Auto-detect | Column definitions as `[['name', 'type'], ...]` |
| `start_row` | INTEGER | 0 | Starting row index (inclusive, zero-based) |
| `start_column` | INTEGER | 0 | Starting column index (inclusive, zero-based) |
| `end_row` | INTEGER | Last row | Ending row index (inclusive) |
| `end_column` | INTEGER | Last column | Ending column index (inclusive) |
| `empty_as_null` | BOOLEAN | `false` | Convert empty cells to NULL instead of empty strings |
| `error_as_null` | BOOLEAN | `false` | Convert parsing errors to NULL instead of failing |

### Supported Data Types

| Type | DuckDB Type | Description |
|------|-------------|-------------|
| `boolean` | BOOLEAN | True/false values |
| `bigint` | BIGINT | 64-bit signed integers |
| `double` | DOUBLE | Double-precision floating point |
| `varchar` | VARCHAR | Variable-length strings |
| `datetime` | TIMESTAMP | Date and time with microsecond precision |
| `date` | DATE | Date without time component |
| `time` | TIME | Time without date component |

## Advanced Usage

### Error Handling

Handle parsing errors gracefully:
```sql
-- Convert errors to NULL values
SELECT * FROM read_sheet('messy_data.xlsx', 
  error_as_null=true,
  empty_as_null=true
);
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
FROM read_sheet('student_data.xlsx',
  fields=[
    ['student_id', 'bigint'],
    ['name', 'varchar'],
    ['score', 'double'], 
    ['created_at', 'datetime']
  ]
);

-- Filter and aggregate data
SELECT 
  department,
  COUNT(*) as employee_count,
  AVG(salary) as avg_salary
FROM read_sheet('hr_data.xlsx',
  fields=[
    ['name', 'varchar'],
    ['department', 'varchar'],
    ['salary', 'double']
  ]
)
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

- `src/lib.rs`: Main extension implementation
- `test/sql/`: SQL test files
- `Cargo.toml`: Rust dependencies and build configuration

To contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run `make test_debug` to verify
5. Submit a pull request

## Known Issues

- On Windows with Python 3.11, you may encounter extension loading issues. Use Python 3.12 or later.
- Very large spreadsheets may require significant memory allocation.
- Complex Excel formulas are not evaluated; only the computed values are read.

## Author

**Zhang, Zepeng**  
Email: redraiment@gmail.com

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Built on the [DuckDB Rust extension template](https://github.com/duckdb/duckdb-rs)
- Uses the [calamine](https://crates.io/crates/calamine) crate for spreadsheet parsing
- Inspired by DuckDB's commitment to making data analysis more accessible
