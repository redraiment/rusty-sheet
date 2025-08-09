# Rusty Sheet

一个 DuckDB 扩展插件，可以直接在 SQL 查询中读取 Excel 和 OpenDocument 电子表格文件。该扩展为使用 DuckDB 强大的 SQL 引擎分析电子表格数据提供了无缝集成。

## 功能特性

- **多格式支持**：读取 Excel 文件（`.xls`, `.xlsx`, `.xlsm`, `.xlsb`, `.xla`, `.xlam`）和 OpenDocument 电子表格文件（`.ods`）
- **灵活的数据类型**：支持布尔值、整数、双精度浮点、字符串、日期时间、日期和时间数据类型
- **自定义数据范围**：指定精确的行和列范围进行数据提取
- **表头处理**：自动检测和解析表头行
- **错误处理**：对空单元格和解析错误提供可配置的处理行为
- **类型安全**：内置数据类型验证和转换
- **纯 Rust 实现**：无 C++ 依赖，充分利用 Rust 的内存安全特性

## 安装说明

### 前置要求

- Python 3
- Python 3-venv
- [Make](https://www.gnu.org/software/make)
- Git
- Rust 工具链

### 从源码构建

1. 克隆仓库并包含子模块：
```bash
git clone --recurse-submodules https://github.com/redraiment/rusty-sheet.git
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

4. 构建完成的扩展将位于 `build/debug/extension/` 或 `build/release/extension/` 目录

## 使用说明

### 加载扩展

使用 unsigned 标志启动 DuckDB 以加载本地扩展：

```bash
duckdb -unsigned
```

加载扩展：
```sql
LOAD './build/debug/extension/rusty-sheet/rusty-sheet.duckdb_extension';
```

### 基础示例

#### 读取包含表头的完整电子表格
```sql
SELECT * FROM read_sheet('data.xlsx');
```

#### 读取指定工作表
```sql
SELECT * FROM read_sheet('workbook.xlsx', sheet_name='Sheet2');
```

#### 定义自定义列类型
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

#### 读取特定数据范围
```sql
SELECT * FROM read_sheet('data.xlsx',
  start_row=2,
  end_row=100,
  start_column=1,
  end_column=5
);
```

#### 读取无表头数据
```sql
SELECT * FROM read_sheet('data.xlsx',
  header=false,
  fields=[['col1', 'varchar'], ['col2', 'bigint']]
);
```

## 参数说明

### 位置参数
- `file_path`（必需）：电子表格文件路径

### 命名参数

| 参数名 | 类型 | 默认值 | 描述 |
|--------|------|--------|------|
| `sheet_name` | VARCHAR | 第一个工作表 | 要读取的工作表名称 |
| `header` | BOOLEAN | `true` | 第一行是否包含列标题 |
| `fields` | LIST | 自动检测 | 列定义，格式为 `[['name', 'type'], ...]` |
| `start_row` | INTEGER | 0 | 起始行索引（包含，从0开始） |
| `start_column` | INTEGER | 0 | 起始列索引（包含，从0开始） |
| `end_row` | INTEGER | 最后一行 | 结束行索引（包含） |
| `end_column` | INTEGER | 最后一列 | 结束列索引（包含） |
| `empty_as_null` | BOOLEAN | `false` | 将空单元格转换为 NULL 而不是空字符串 |
| `error_as_null` | BOOLEAN | `false` | 将解析错误转换为 NULL 而不是失败 |

### 支持的数据类型

| 类型 | DuckDB 类型 | 描述 |
|------|-------------|------|
| `boolean` | BOOLEAN | 真/假值 |
| `bigint` | BIGINT | 64位有符号整数 |
| `double` | DOUBLE | 双精度浮点数 |
| `varchar` | VARCHAR | 可变长度字符串 |
| `datetime` | TIMESTAMP | 具有微秒精度的日期时间 |
| `date` | DATE | 不含时间部分的日期 |
| `time` | TIME | 不含日期部分的时间 |

## 高级用法

### 错误处理

优雅地处理解析错误：
```sql
-- 将错误转换为 NULL 值
SELECT * FROM read_sheet('messy_data.xlsx', 
  error_as_null=true,
  empty_as_null=true
);
```

### 处理多个工作表

```sql
-- 读取不同工作表并合并结果
SELECT 'Q1' as quarter, * FROM read_sheet('sales.xlsx', sheet_name='Q1')
UNION ALL
SELECT 'Q2' as quarter, * FROM read_sheet('sales.xlsx', sheet_name='Q2')
UNION ALL
SELECT 'Q3' as quarter, * FROM read_sheet('sales.xlsx', sheet_name='Q3')
UNION ALL
SELECT 'Q4' as quarter, * FROM read_sheet('sales.xlsx', sheet_name='Q4');
```

### 数据分析示例

```sql
-- 计算汇总统计信息
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

-- 过滤和聚合数据
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
DUCKDB_TEST_VERSION=v1.3.2 make configure
make debug
make test_debug
```

## 开发说明

该扩展使用 DuckDB Rust 扩展框架构建。主要组件包括：

- `src/lib.rs`：主要扩展实现
- `test/sql/`：SQL 测试文件
- `Cargo.toml`：Rust 依赖和构建配置

贡献代码：

1. Fork 仓库
2. 创建功能分支
3. 进行更改并添加测试
4. 运行 `make test_debug` 进行验证
5. 提交 pull request

## 已知问题

- 在 Windows 系统上使用 Python 3.11 时，可能遇到扩展加载问题。请使用 Python 3.12 或更高版本。
- 非常大的电子表格可能需要大量内存分配。
- 不会计算复杂的 Excel 公式；只读取计算后的值。

## 作者信息

**张泽鹏（Zhang, Zepeng）**  
邮箱：redraiment@gmail.com

## 许可证

本项目采用 MIT 许可证 - 详见 LICENSE 文件。

## 致谢

- 基于 [DuckDB Rust 扩展模板](https://github.com/duckdb/duckdb-rs) 构建
- 使用 [calamine](https://crates.io/crates/calamine) crate 进行电子表格解析
- 受 DuckDB 致力于让数据分析更易访问的理念启发
