# Rusty Sheet

一个 DuckDB 扩展，支持在 SQL 查询中直接读取 Excel、WPS 和 OpenDocument 电子表格文件。

[English README is here](https://github.com/redraiment/rusty-sheet/blob/main/README.md)

## 功能特性

- **高性能**：采用纯 Rust 实现，针对大文件优化；例如，在 MacBook M1 上读取 100 万行的 XLSX 文件现在只需不到 13 秒（相比 v0.1.x 版本的 140+ 秒大幅提升）。
- **多格式支持**：读取 Excel 文件（`.xls`、`.xlsx`、`.xlsm`、`.xlsb`、`.xla`、`.xlam`）、WPS 文件（`.et`、`.ett`）和 OpenDocument 电子表格文件（`.ods`）
- **批处理**：使用通配符模式匹配分析和读取多个文件和工作表
- **灵活数据类型**：支持布尔值、整数、双精度浮点数、字符串、日期时间、日期和时间类型
- **Excel 风格数据范围**：使用熟悉的 Excel 表示法指定数据范围（例如 `"A1:C3"`）
- **自动列类型检测**：自动推断列类型；可通过 `columns` 参数覆盖特定列的类型
- **标题行处理**：自动检测和解析标题行
- **错误处理**：可配置的解析错误行为，提供精确的单元格位置和文件名报告
- **类型安全**：内置数据类型验证和转换
- **高级数据过滤**：跳过空行或在第一个空行处停止，实现高效数据处理
- **高级模式匹配**：在批处理操作中支持多个通配符模式

## 安装

### 前置要求

- Python 3
- Python 3-venv
- [Make](https://www.gnu.org/software/make)
- Git
- Rust 工具链

### 从源码构建

1. 克隆仓库：
```bash
git clone https://github.com/redraiment/rusty-sheet.git
cd rusty-sheet
```

2. 配置构建环境：

```bash
make configure
```

3. 构建扩展：

```bash
make debug    # 开发版本
make release  # 生产版本
```

4. 构建好的扩展将位于 `build/debug/extension/` 或 `build/release/extension/` 目录中

## 使用方法

### 加载扩展

使用无符号标志启动 DuckDB 以加载本地扩展：

```bash
duckdb -unsigned
```

加载扩展：

```sql
LOAD './build/debug/extension/rusty-sheet/rusty-sheet.duckdb_extension';
```

### 基础示例

#### 读取电子表格中第一张工作表（包含标题）

```sql
SELECT * FROM read_sheet('data.xlsx');
```

#### 读取特定工作表

```sql
SELECT * FROM read_sheet('workbook.xlsx', sheet='Sheet2');
```

#### 覆盖特定列类型（其他列自动检测）

```sql
SELECT * FROM read_sheet('data.xlsx',
  columns={'id': 'bigint'}
);
```

#### 读取特定数据范围（Excel 样式表示法）

```sql
SELECT * FROM read_sheet('data.xlsx', range='A2:E100');
```

#### 无标题读取

```sql
SELECT * FROM read_sheet('data.xlsx',
  header=false,
  columns={'A': 'varchar', 'B': 'bigint'}
);
```

### 在不读取完整数据的情况下分析列类型

```sql
SELECT * FROM analyze_sheet('data.xlsx', analyze_rows=20);
```

### 批处理示例

#### 使用通配符模式读取多个文件

```sql
-- 读取目录中的所有 Excel 文件
SELECT * FROM read_sheets(['*.xlsx']);

-- 读取所有 WPS 文件
SELECT * FROM read_sheets(['*.et']);

-- 使用列表模式读取多种文件类型
SELECT * FROM read_sheets(['*.xls', '*.xlsx']);

-- 读取不同扩展名的多种文件类型
SELECT * FROM read_sheets(['*.xlsx', '*.ods', '*.et']);
```

#### 分析多个文件和工作表

```sql
-- 分析所有 Excel 文件中的所有工作表
SELECT * FROM analyze_sheets(['*.xlsx']);

-- 跨文件分析特定工作表
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['Sheet1', 'Sheet2']);

-- 使用通配符模式分析
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['Sheet*']);

-- 分析多种文件类型
SELECT * FROM analyze_sheets(['*.xlsx', '*.ods']);
```

#### 高级模式匹配

```sql
-- 仅在特定文件类型中匹配特定工作表
SELECT * FROM read_sheets(['*.xlsx'], sheets=['*.xlsx=Sheet*']);

-- 读取多种文件类型
SELECT * FROM read_sheets(['*.*']);

-- 使用改进的匹配功能处理多个文件模式
SELECT * FROM read_sheets(['*.xls', '*.xlsx'], sheets=['Sheet*']);
```

### 改进的通配符匹配

`read_sheets` 和 `analyze_sheets` 函数现在具有增强的匹配逻辑：

- **多模式支持**：接受文件模式列表，实现灵活的多格式处理
- **智能工作表发现**：使用通配符时，系统会找到同时匹配文件模式和工作表模式的第一个工作表，减少"无匹配工作表"错误
- **文件特定匹配**：改进了对文件特定工作表模式（如 `*.xlsx=Sheet*`）的处理

#### 高级数据过滤

```sql
-- 跳过完全空白的行
SELECT * FROM read_sheet('data.xlsx', skip_empty_rows=true);

-- 在第一个空行处停止读取
SELECT * FROM read_sheet('data.xlsx', end_at_empty_row=true);
```

## 函数

### analyze_sheet

分析单个文件中单个工作表的列结构。

**参数：**

- **file_path**（必需）：电子表格文件路径（不支持通配符）
- **sheet**（可选，默认为第一个工作表）：工作表名称（支持通配符，如 `Sheet*`）
- **range**（可选）：数据范围，格式为 `[起始列][起始行]:[结束列][结束行]`
- **header**（可选，默认为 `true`）：第一行是否包含列标题
- **analyze_rows**（可选，默认为 `10`）：用于类型推断的分析行数
- **error_as_null**（可选，默认为 `false`）：如果为 true，将解析错误转换为 NULL 而不是失败

**示例：**

```sql
-- 分析默认工作表
SELECT * FROM analyze_sheet('data.xlsx');

-- 分析特定工作表
SELECT * FROM analyze_sheet('data.xlsx', sheet='Sheet2');

-- 分析特定范围
SELECT * FROM analyze_sheet('data.xlsx', range='A1:C10');

-- 分析更多行以获得更好的类型推断
SELECT * FROM analyze_sheet('data.xlsx', analyze_rows=50);
```

### analyze_sheets

使用通配符模式匹配分析多个文件中多个工作表的列结构。

**参数：**

- **file_pattern**（必需）：支持通配符的文件路径模式（例如 `['*.xlsx']`、`['*.xls', '*.xlsx']`）
- **sheets**（可选）：工作表名称列表（支持通配符和文件特定模式，如 `['Sheet*']`、`['*.xlsx=Sheet*']`）
- **range**（可选）：数据范围，格式为 `[起始列][起始行]:[结束列][结束行]`
- **header**（可选，默认为 `true`）：第一行是否包含列标题
- **analyze_rows**（可选，默认为 `10`）：用于类型推断的分析行数
- **error_as_null**（可选，默认为 `false`）：如果为 true，将解析错误转换为 NULL 而不是失败

**示例：**

```sql
-- 分析所有 Excel 文件中的所有工作表
SELECT * FROM analyze_sheets(['*.xlsx']);

-- 跨文件分析特定工作表
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['Sheet1', 'Sheet2']);

-- 使用通配符匹配工作表
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['Sheet*']);

-- 文件特定模式匹配
SELECT * FROM analyze_sheets(['*.xlsx'], sheets=['*.xlsx=Sheet*']);

-- 分析多种文件类型
SELECT * FROM analyze_sheets(['*.xlsx', '*.ods']);
```

### read_sheet

从单个文件中的单个工作表读取数据。

**参数：**

- **file_path**（必需）：电子表格文件路径（不支持通配符）
- **sheet**（可选，默认第一个工作表）：工作表名称（支持通配符如 `Sheet*`）
- **range**（可选）：数据范围，格式为 `[起始列][起始行]:[结束列][结束行]`
- **header**（可选，默认为 `true`）：第一行是否包含列标题
- **analyze_rows**（可选，默认为 `10`）：用于类型推断的分析行数
- **error_as_null**（可选，默认为 `false`）：如果为 true，将解析错误转换为 NULL 而不是失败
- **skip_empty_rows**（可选，默认为 `false`）：跳过所有列都包含空值的行
- **end_at_empty_row**（可选，默认为 `false`）：在第一个完全空白的行处停止读取

**示例：**

```sql
-- 读取默认工作表
SELECT * FROM read_sheet('data.xlsx');

-- 读取特定工作表
SELECT * FROM read_sheet('workbook.xlsx', sheet='Sheet2');

-- 读取特定数据范围
SELECT * FROM read_sheet('data.xlsx', range='A2:E100');

-- 跳过空行
SELECT * FROM read_sheet('data.xlsx', skip_empty_rows=true);

-- 在第一个空行处停止
SELECT * FROM read_sheet('data.xlsx', end_at_empty_row=true);

-- 将错误处理为 NULL
SELECT * FROM read_sheet('messy_data.xlsx', error_as_null=true);
```

### read_sheets

使用通配符模式匹配从多个文件中的多个工作表读取数据。

**重要说明：** 使用通配符模式时，此函数仅从**第一个匹配的工作表**分析列结构和数据类型。所有后续具有匹配模式的工作表将使用相同的列结构，即使它们的实际结构不同。对于具有不同结构的工作表，请考虑先使用 `analyze_sheets` 检查各个工作表的结构。

**参数：**

- **file_pattern**（必需）：支持通配符的文件路径模式（例如 `['*.xlsx']`、`['*.xls', '*.xlsx']`）
- **sheets**（可选）：工作表名称列表（支持通配符和文件特定模式，如 `['Sheet*']`、`['*.xlsx=Sheet*']`）
- **range**（可选）：数据范围，格式为 `[起始列][起始行]:[结束列][结束行]`
- **header**（可选，默认 `true`）：第一行是否包含列标题
- **analyze_rows**（可选，默认 `10`）：用于类型推断的分析行数
- **error_as_null**（可选，默认 `false`）：如果为 true，将解析错误转换为 NULL 而不是失败
- **skip_empty_rows**（可选，默认 `false`）：跳过所有列都包含空值的行
- **end_at_empty_row**（可选，默认 `false`）：在第一个完全空白的行处停止读取
- **file_name_column**（可选）：在结果中包含文件源信息的列名
- **sheet_name_column**（可选）：在结果中包含工作表源信息的列名
- **union_by_name**（可选，默认 `false`）：当为 false 时，按位置合并数据；当为 true 时，按列名合并数据

**示例：**

```sql
-- 读取所有 Excel 文件
SELECT * FROM read_sheets(['*.xlsx']);

-- 读取所有 WPS 文件
SELECT * FROM read_sheets(['*.et']);

-- 读取特定工作表
SELECT * FROM read_sheets(['*.xlsx'], sheets=['Sheet1', 'Sheet2']);

-- 使用通配符匹配工作表
SELECT * FROM read_sheets(['*.xlsx'], sheets=['Sheet*']);

-- 文件特定模式匹配
SELECT * FROM read_sheets(['*.xlsx'], sheets=['*.xlsx=Sheet*']);

-- 批处理中跳过空行
SELECT * FROM read_sheets(['*.xlsx'], skip_empty_rows=true);

-- 读取多种文件类型
SELECT * FROM read_sheets(['*.xls', '*.xlsx']);

-- 使用自定义列名跟踪数据源
SELECT * FROM read_sheets(['*.xls', '*.xlsx'],
  sheets=['Sheet*'],
  file_name_column='file',
  sheet_name_column='worksheet'
);

-- 按列名而不是位置合并数据
SELECT * FROM read_sheets(['*.xlsx'], union_by_name=true);

-- 按列名合并特定工作表的数据
SELECT * FROM read_sheets(['*.xlsx'],
  sheets=['Sheet1', 'Sheet2'],
  union_by_name=true
);
```

### 支持的数据类型

| 类型 | DuckDB 类型 | 描述 |
|------|-------------|-------------|
| `boolean` | BOOLEAN | 真/假值 |
| `bigint` | BIGINT | 64 位有符号整数 |
| `double` | DOUBLE | 双精度浮点数 |
| `varchar` | VARCHAR | 可变长度字符串 |
| `timestamp` | TIMESTAMP | 日期和时间，微秒精度（支持 ISO 8601 格式） |
| `date` | DATE | 不含时间成分的日期（支持 ISO 8601 格式） |
| `time` | TIME | 不含日期成分的时间（包括 ISO 8601 持续时间） |

## 范围参数格式

`range` 参数支持灵活的 Excel 风格单元格范围表示法，包含五个可选组件：

### 范围组件

1. **起始列**（可选）：Excel 列字母（例如 `A` 表示第 1 列，`B` 表示第 2 列等）
    - 如果指定，即使该列不包含数据，也从此列开始读取
    - 不会跳过空列

2. **起始行**（可选）：Excel 行号（例如 `1` 表示第 1 行，`2` 表示第 2 行等）
    - 如果指定，即使该行不包含数据，也从此行开始读取
    - 不会跳过空行

3. **冒号分隔符**：仅在指定结束列或结束行时需要

4. **结束列**（可选）：Excel 列字母
    - 如果指定，即使数据提前结束，也在此列停止读取
    - 忽略此列之后的数据

5. **结束行**（可选）：Excel 行号
    - 如果指定，即使数据提前结束，也在此行停止读取
    - 忽略此行之后的数据

### 范围示例

```sql
-- 单个单元格（仅 B2）
SELECT * FROM read_sheet('data.xlsx', range='B2:B2');

-- 单元格范围（B-D 列，2-5 行）
SELECT * FROM read_sheet('data.xlsx', range='B2:D5');

-- 仅列范围（A 到 C 列的所有行）
SELECT * FROM read_sheet('data.xlsx', range='A:C');

-- 仅行范围（5 到 15 行的所有列）
SELECT * FROM read_sheet('data.xlsx', range='5:15');

-- 仅起始列和行（从 B2 到工作表末尾）
SELECT * FROM read_sheet('data.xlsx', range='B2');

-- 仅单列（仅 B 列）
SELECT * FROM read_sheet('data.xlsx', range='B:B');

-- 仅单行（仅第 5 行）
SELECT * FROM read_sheet('data.xlsx', range='5:5');

-- 仅起始列（从 B 列到工作表末尾）
SELECT * FROM read_sheet('data.xlsx', range='B');

-- 仅起始行（从第 5 行到工作表末尾）
SELECT * FROM read_sheet('data.xlsx', range='5');

-- 仅结束列（从开始到 D 列）
SELECT * FROM read_sheet('data.xlsx', range=':D');

-- 仅结束行（从开始到第 10 行）
SELECT * FROM read_sheet('data.xlsx', range=':10');
```

## 通配符模式匹配

该扩展支持用于文件和工表匹配的 Rust glob 模式。

### Glob 模式语法

- `?` - 匹配任意单个字符
- `*` - 匹配任意（可能为空）字符序列
- `**` - 匹配当前目录和任意子目录
- `[...]` - 匹配括号内的任意字符
- `[!...]` - `[...]` 的否定形式，匹配不在括号内的字符

### 模式示例

```sql
-- 单字符通配符
SELECT * FROM read_sheets(['data_2024_??.xlsx']);

-- 多字符通配符
SELECT * FROM read_sheets(['report_*.xlsx']);

-- 字符集匹配
SELECT * FROM read_sheets(['data_[0-9].xlsx']);
SELECT * FROM read_sheets(['file_[abc].xlsx']);

-- 否定模式
SELECT * FROM read_sheets(['file_[!test]*.xlsx']);

-- 递归目录匹配
SELECT * FROM read_sheets(['**/*.xlsx']);

-- 工作表模式匹配
SELECT * FROM read_sheets(['*.xlsx'], sheets=['Sheet?']);
SELECT * FROM read_sheets(['*.xlsx'], sheets=['Data*']);

-- 文件特定工作表模式
SELECT * FROM read_sheets(['*.xlsx'], sheets=['*.xlsx=Sheet*']);
```

### 特别说明

- `**` 必须构成单个路径组件（例如 `**/*.xlsx` 有效，`**a` 无效）
- 字符范围使用 Unicode 排序（例如 `[0-9]` 匹配数字 0-9）
- 元字符 `?`、`*`、`[`、`]` 可以使用括号进行匹配（例如 `[?]`）
- 字符集中的 `-` 字符必须在开头或结尾（例如 `[abc-]`）

## 测试

运行测试套件：

```bash
# 测试调试版本
make test_debug

# 测试发布版本  
make test_release
```

使用不同 DuckDB 版本进行测试：

```bash
make clean_all
DUCKDB_TEST_VERSION=v1.4.1 make configure
make debug
make test_debug
```

## 开发

此扩展使用 DuckDB Rust 扩展框架构建。主要组件包括：

* `src/lib.rs`：主要扩展实现
* `Cargo.toml`：Rust 依赖项和构建配置

贡献指南：

1. Fork 仓库
2. 创建功能分支
3. 进行更改并添加测试
4. 运行 `make test_debug` 进行验证
5. 提交拉取请求

## 已知问题

* 在 Windows 上使用 Python 3.11 时，可能会遇到扩展加载问题。请使用 Python 3.12 或更高版本。
* 非常大的电子表格可能需要大量内存分配。
* 复杂的 Excel 公式不会被计算；只读取计算后的值。

## 作者

**张泽鹏**
邮箱：[redraiment@gmail.com](mailto:redraiment@gmail.com)

## 许可证

本项目采用 MIT 许可证 - 详见 LICENSE 文件。

## 致谢

* 基于 [DuckDB Rust 扩展模板](https://github.com/duckdb/extension-template-rs) 构建
* 灵感来源于 DuckDB 让数据分析更易用的承诺
