# Rusty Sheet

A DuckDB extension that enables reading Excel, WPS, and OpenDocument spreadsheet files directly within SQL queries. This extension provides seamless integration for analyzing spreadsheet data using DuckDB's powerful SQL engine.

[中文说明请点击这里](https://github.com/redraiment/rusty-sheet/blob/main/README.zh.md)

## Features

- **High Performance**: Optimized for large files with pure Rust implementation; e.g., reading a 1M-row XLSX file on MacBook M1 now takes under 13 seconds (down from 140+ seconds in v0.1.x).
- **Multiple Format Support**: Read Excel files (`.xls`, `.xlsx`, `.xlsm`, `.xlsb`, `.xla`, `.xlam`), WPS files (`.et`, `.ett`), and OpenDocument Spreadsheet files (`.ods`)
- **Batch Processing**: Analyze or read multiple files and worksheets with wildcard pattern matching
- **Flexible Data Types**: Support for boolean, integer, double, varchar, datetime, date, and time types
- **Excel-Style Data Ranges**: Specify data ranges using familiar Excel notation (e.g., `"A1:C3"`)
- **Automatic Column Type Detection**: Column types are inferred automatically; override specific columns with the `columns` parameter
- **Header Row Handling**: Automatic detection and parsing of header rows
- **Error Handling**: Configurable behavior for parsing errors with precise cell location and file name reporting
- **Type Safety**: Built-in data type validation and conversion
- **Advanced Data Filtering**: Skip empty rows or stop at first empty row for efficient data processing

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

#### Read first sheet of spreadsheet with headers

```sql
SELECT * FROM read_sheet('data.xlsx');
```

#### Read specific worksheet

```sql
SELECT * FROM read_sheet('workbook.xlsx', sheet='Sheet2');
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
  columns={'A': 'varchar', 'B': 'bigint'}
);
```

### Analyze column types without reading full data

```sql
SELECT * FROM analyze_sheet('data.xlsx', analyze_rows=20);
```

### Batch Processing Examples

#### Read multiple files with wildcard pattern

```sql
-- Read all Excel files in directory
SELECT * FROM read_sheets(['*.xlsx']);

-- Read all WPS files
SELECT * FROM read_sheets(['*.et']);

-- Read multiple file types using list patterns
SELECT * FROM read_sheets(['*.xls', '*.xlsx']);

-- Read multiple file types with different extensions
SELECT * FROM read_sheets(['*.xlsx', '*.ods', '*.et']);
```

#### Analyze multiple files and worksheets

```sql
-- Analyze all worksheets in all Excel files
SELECT * FROM analyze_sheets(['*.xlsx']);

-- Analyze specific worksheets across files
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['Sheet1', 'Sheet2']);

-- Analyze with wildcard pattern
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['Sheet*']);

-- Analyze multiple file types
SELECT * FROM analyze_sheets(['*.xlsx', '*.ods']);
```

#### Advanced pattern matching

```sql
-- Match specific worksheets only in specific file types
SELECT * FROM read_sheets(['*.xlsx'], sheets=['*.xlsx=Sheet*']);

-- Read multiple file types
SELECT * FROM read_sheets(['*.*']);

-- Use multiple file patterns with improved matching
SELECT * FROM read_sheets(['*.xls', '*.xlsx'], sheets=['Sheet*']);
```

### Improved Wildcard Matching

The `read_sheets` and `analyze_sheets` functions now feature enhanced matching logic:

- **Multiple Pattern Support**: Accepts lists of file patterns for flexible multi-format processing
- **Smart Worksheet Discovery**: When using wildcards, the system finds the first worksheet that matches both the file pattern and worksheet pattern, reducing "no matching worksheets" errors
- **File-Specific Matching**: Improved handling of file-specific worksheet patterns like `*.xlsx=Sheet*`

#### Advanced data filtering

```sql
-- Skip completely empty rows
SELECT * FROM read_sheet('data.xlsx', skip_empty_rows=true);

-- Stop reading at first empty row
SELECT * FROM read_sheet('data.xlsx', end_at_empty_row=true);
```

## Functions

### analyze_sheet

Analyzes the column structure of a single worksheet in a single file.

**Parameters:**

- **file_path** (required): Path to the spreadsheet file (no wildcard support)
- **sheet** (optional, default first sheet): Worksheet name (supports wildcards like `Sheet*`)
- **range** (optional): Data range in format `[start_col][start_row]:[end_col][end_row]`
- **header** (optional, default `true`): Whether the first row contains column headers
- **analyze_rows** (optional, default `10`): Number of rows to analyze for type inference
- **error_as_null** (optional, default `false`): If true, convert parsing errors to NULL instead of failing

**Examples:**

```sql
-- Analyze default worksheet
SELECT * FROM analyze_sheet('data.xlsx');

-- Analyze specific worksheet
SELECT * FROM analyze_sheet('data.xlsx', sheet='Sheet2');

-- Analyze specific range
SELECT * FROM analyze_sheet('data.xlsx', range='A1:C10');

-- Analyze more rows for better type inference
SELECT * FROM analyze_sheet('data.xlsx', analyze_rows=50);
```

### analyze_sheets

Analyzes column structures of multiple worksheets across multiple files with wildcard pattern matching.

**Parameters:**

- **file_pattern** (required): File path pattern(s) with wildcard support (e.g., `['*.xlsx']`, `['*.xls', '*.xlsx']`)
- **sheets** (optional): List of worksheet names (supports wildcards and file-specific patterns like `['Sheet*']`, `['*.xlsx=Sheet*']`)
- **range** (optional): Data range in format `[start_col][start_row]:[end_col][end_row]`
- **header** (optional, default `true`): Whether the first row contains column headers
- **analyze_rows** (optional, default `10`): Number of rows to analyze for type inference
- **error_as_null** (optional, default `false`): If true, convert parsing errors to NULL instead of failing

**Examples:**

```sql
-- Analyze all worksheets in all Excel files
SELECT * FROM analyze_sheets(['*.xlsx']);

-- Analyze specific worksheets across files
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['Sheet1', 'Sheet2']);

-- Use wildcard to match worksheets
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['Sheet*']);

-- File-specific pattern matching
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['*.xlsx=Sheet*']);

-- Analyze multiple file types
SELECT * FROM analyze_sheets(['*.xlsx', '*.ods']);
```

### read_sheet

Reads data from a single worksheet in a single file.

**Parameters:**

- **file_path** (required): Path to the spreadsheet file (no wildcard support)
- **sheet** (optional, default first sheet): Worksheet name (supports wildcards like `Sheet*`)
- **range** (optional): Data range in format `[start_col][start_row]:[end_col][end_row]`
- **header** (optional, default `true`): Whether the first row contains column headers
- **analyze_rows** (optional, default `10`): Number of rows to analyze for type inference
- **error_as_null** (optional, default `false`): If true, convert parsing errors to NULL instead of failing
- **skip_empty_rows** (optional, default `false`): Skip rows where all columns contain empty values
- **end_at_empty_row** (optional, default `false`): Stop reading at the first completely empty row

**Examples:**

```sql
-- Read default worksheet
SELECT * FROM read_sheet('data.xlsx');

-- Read specific worksheet
SELECT * FROM read_sheet('workbook.xlsx', sheet='Sheet2');

-- Read specific data range
SELECT * FROM read_sheet('data.xlsx', range='A2:E100');

-- Skip empty rows
SELECT * FROM read_sheet('data.xlsx', skip_empty_rows=true);

-- Stop at first empty row
SELECT * FROM read_sheet('data.xlsx', end_at_empty_row=true);

-- Handle errors as NULL
SELECT * FROM read_sheet('messy_data.xlsx', error_as_null=true);
```

### read_sheets

Reads data from multiple worksheets across multiple files with wildcard pattern matching.

**Important Note:** When using wildcard patterns, this function analyzes the column structure and data types from the **first matching worksheet** only. All subsequent worksheets with matching patterns will use the same column structure, even if their actual structure differs. For worksheets with varying structures, consider using `analyze_sheets` first to inspect individual worksheet structures.

**Parameters:**

- **file_pattern** (required): File path pattern(s) with wildcard support (e.g., `['*.xlsx']`, `['*.xls', '*.xlsx']`)
- **sheets** (optional): List of worksheet names (supports wildcards and file-specific patterns like `['Sheet*']`, `['*.xlsx=Sheet*']`)
- **range** (optional): Data range in format `[start_col][start_row]:[end_col][end_row]`
- **header** (optional, default `true`): Whether the first row contains column headers
- **analyze_rows** (optional, default `10`): Number of rows to analyze for type inference
- **error_as_null** (optional, default `false`): If true, convert parsing errors to NULL instead of failing
- **skip_empty_rows** (optional, default `false`): Skip rows where all columns contain empty values
- **end_at_empty_row** (optional, default `false`): Stop reading at the first completely empty row
- **file_name_column** (optional): Column name to include file source information in results
- **sheet_name_column** (optional): Column name to include worksheet source information in results
- **union_by_name** (optional, default `false`): When false, union data by position; when true, union data by column name

**Examples:**

```sql
-- Read all Excel files
SELECT * FROM read_sheets(['*.xlsx']);

-- Read all WPS files
SELECT * FROM read_sheets(['*.et']);

-- Read specific worksheets
SELECT * FROM read_sheets(['*.xlsx'], sheets=['Sheet1', 'Sheet2']);

-- Use wildcard to match worksheets
SELECT * FROM read_sheets(['*.xlsx'], sheets=['Sheet*']);

-- File-specific pattern matching
SELECT * FROM read_sheets(['*.xlsx'], sheets=['*.xlsx=Sheet*']);

-- Skip empty rows in batch processing
SELECT * FROM read_sheets(['*.xlsx'], skip_empty_rows=true);

-- Read multiple file types
SELECT * FROM read_sheets(['*.xls', '*.xlsx']);

-- Track data sources with custom column names
SELECT * FROM read_sheets(['*.xls', '*.xlsx'],
  sheets=['Sheet*'],
  file_name_column='file',
  sheet_name_column='worksheet'
);

-- Union data by column name instead of position
SELECT * FROM read_sheets(['*.xlsx'], union_by_name=true);

-- Union by name with specific worksheets
SELECT * FROM read_sheets(['*.xlsx'],
  sheets=['Sheet1', 'Sheet2'],
  union_by_name=true
);
```

### Supported Data Types

| Type | DuckDB Type | Description |
|------|-------------|-------------|
| `boolean` | BOOLEAN | True/false values |
| `bigint` | BIGINT | 64-bit signed integers |
| `double` | DOUBLE | Double-precision floating point |
| `varchar` | VARCHAR | Variable-length strings |
| `timestamp` | TIMESTAMP | Date and time with microsecond precision (supports ISO 8601 format) |
| `date` | DATE | Date without time component (supports ISO 8601 format) |
| `time` | TIME | Time without date component (including ISO 8601 durations) |

## Range Parameter Format

The `range` parameter supports flexible Excel-style cell range notation with five optional components:

### Range Components

1. **Start Column** (optional): Excel column letter (e.g., `A` for column 1, `B` for column 2, etc.)
   - If specified, reading starts from this column even if it contains no data
   - Does not skip empty columns

2. **Start Row** (optional): Excel row number (e.g., `1` for row 1, `2` for row 2, etc.)
   - If specified, reading starts from this row even if it contains no data
   - Does not skip empty rows

3. **Colon Separator**: Required only when specifying end column or end row

4. **End Column** (optional): Excel column letter
   - If specified, reading stops at this column even if data ends earlier
   - Ignores data beyond this column

5. **End Row** (optional): Excel row number
   - If specified, reading stops at this row even if data ends earlier
   - Ignores data beyond this row

### Range Examples

```sql
-- Single cell (B2 only)
SELECT * FROM read_sheet('data.xlsx', range='B2:B2');

-- Cell range (columns B-D, rows 2-5)
SELECT * FROM read_sheet('data.xlsx', range='B2:D5');

-- Column range only (all rows in columns A through C)
SELECT * FROM read_sheet('data.xlsx', range='A:C');

-- Row range only (all columns in rows 5 through 15)
SELECT * FROM read_sheet('data.xlsx', range='5:15');

-- Start column and row only (from B2 to end of sheet)
SELECT * FROM read_sheet('data.xlsx', range='B2');

-- Single column only (column B only)
SELECT * FROM read_sheet('data.xlsx', range='B:B');

-- Single row only (row 5 only)
SELECT * FROM read_sheet('data.xlsx', range='5:5');

-- Start column only (from column B to end of sheet)
SELECT * FROM read_sheet('data.xlsx', range='B');

-- Start row only (from row 5 to end of sheet)
SELECT * FROM read_sheet('data.xlsx', range='5');

-- End column only (from start to column D)
SELECT * FROM read_sheet('data.xlsx', range=':D');

-- End row only (from start to row 10)
SELECT * FROM read_sheet('data.xlsx', range=':10');
```

## Wildcard Pattern Matching

The extension supports Rust glob patterns for file and worksheet matching.

### Glob Pattern Syntax

- `?` - Matches any single character
- `*` - Matches any (possibly empty) sequence of characters
- `**` - Matches the current directory and arbitrary subdirectories
- `[...]` - Matches any character inside the brackets
- `[!...]` - Negation of `[...]`, matches characters not in the brackets

### Pattern Examples

```sql
-- Single character wildcard
SELECT * FROM read_sheets(['data_2024_??.xlsx']);

-- Multiple character wildcard
SELECT * FROM read_sheets(['report_*.xlsx']);

-- Character set matching
SELECT * FROM read_sheets(['data_[0-9].xlsx']);
SELECT * FROM read_sheets(['file_[abc].xlsx']);

-- Negation pattern
SELECT * FROM read_sheets(['file_[!test]*.xlsx']);

-- Recursive directory matching
SELECT * FROM read_sheets(['**/*.xlsx']);

-- Worksheet pattern matching
SELECT * FROM read_sheets(['*.xlsx'], sheets=['Sheet?']);
SELECT * FROM read_sheets(['*.xlsx'], sheets=['Data*']);

-- File-specific worksheet patterns
SELECT * FROM read_sheets(['*.xlsx'], sheets=['*.xlsx=Sheet*']);
```

### Special Notes

- `**` must form a single path component (e.g., `**/*.xlsx` is valid, `**a` is invalid)
- Character ranges use Unicode ordering (e.g., `[0-9]` matches digits 0-9)
- Metacharacters `?`, `*`, `[`, `]` can be matched using brackets (e.g., `[?]`)
- The `-` character in character sets must be at start or end (e.g., `[abc-]`)

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
DUCKDB_TEST_VERSION=v1.4.1 make configure
make debug
make test_debug
```

## Development

This extension is built using the DuckDB Rust extension framework. The main components are:

* `src/lib.rs`: Main extension implementation
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

* Built on the [DuckDB Rust extension template](https://github.com/duckdb/extension-template-rs)
* Inspired by DuckDB's commitment to making data analysis more accessible
