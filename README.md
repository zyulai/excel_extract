# Excel数据提取工具

## 功能概述
本工具用于从Excel/CSV文件中提取结构化数据，支持：
- ✅ 多级字典嵌套结构
- ✅ 自动空值清洗
- ✅ 头部行跳过（skiprows参数）

## 快速开始
### 安装依赖
```bash
pip install pandas>=1.5.0 openpyxl>=3.0.10 numpy>=1.23.0
```

## 模板配置指南

### example1
```
# 对应数据表示例：
| 年份 | 季度  | 营收      |
|------|------|----------|
| 2023 | Q1   | 5000000  |
| 2023 | Q2   | 6200000  |
```

```python
# 复合主键结构
[
    [None, None],  # 年份+季度
    [None]         # 营收数据
]
```

返回的数据结构为：
```python
{
    (2023, 'Q1'): [5000000],
    (2023, 'Q2'): [6200000]
}
```

### example2
```
# 对应数据表示例：
# | Col A   | Col B    | Col C     |
# |---------|----------|-----------|
# | Report1 | SectionA | Value1    |
# |         | SectionB | Value2    | <= Col A empty implies 'Report1'
# | Report2 | SectionC | N/A       | <= "N/A" becomes None
# | Report3 |          | Value3    | <= Col B empty becomes None key
```

```python
# 复合主键结构
[ 
    [None],
    [None, None],
]
```

返回的数据结构为：
```python
{
    'Report1': {
        ['SectionA', 'Value1'],
        ['SectionB', 'Value2'],
    },
    'Report2': {
        ['SectionC', None],
    },
    'Report3': {
        [None, 'Value3'],
    }
]
```

### example3
```
# 对应数据表示例：
# | Col A   | Col B    | Col C     |
# | Report1 | SectionA | Value1    |
# |         | SectionB | Value2    | <= Col A empty implies 'Report1'
# | Report2 | SectionC | N/A       | <= "N/A" becomes None
# | Report3 |          | Value3    | <= Col B empty becomes None key
```

```python
# 复合主键结构
[
    [None],
    [None],
]
```

返回的数据结构为：
```python
{
    'Report1': ["SectionA", "SectionB"],
    'Report2': ["SectionC"],
    'Report3': [None],
}
```

### example4: 层层嵌套
```
# 对应数据表示例：
# | Col A   | Col B    | Col C     ｜ Col D    ｜ Col E    |  Col F    |
# | Report1 | SectionA | Value1    |  2023     |  Q1       | 5000000   |
# |         | SectionB | Value2    |  2023     |  Q2       | 6200000   |
# |         ｜         ｜ Value3    |  2024     |  Q1       | 7000000   |
# |         |          |            |  2024     |  Q2       | 8000000   |
# |         | SectionC | N/A       |  2024     |  Q3       | N/A       |
# | Report2 | SectionD | Value4    |  2025     |  Q1       | 9000000   |
# | Report3 |          | Value5    |  2025     |  Q2       | 10000000  |
```

```python
# 复合主键结构
[
    [None],
    [None],
    [None],
    [None, None],
    [None],
]
```

返回的数据结构为：
```python
{
    'Report1': {
        'SectionA': {
            "Value1": {
                (2023, 'Q1'): [5000000],
            },
        "SectionB": {
            "Value2": {
                (2023, 'Q2'): [6200000],
            },
            "Value3": {
                (2024, 'Q1'): [7000000],
                (2024, 'Q2'): [8000000],
            },
        },
    },
    'Report2': {
        'SectionD': {
            "Value4": {
                (2025, 'Q1'): [9000000],
            },
        },
    },
    'Report3': {
        None: {
            "Value5": {
                (2025, 'Q2'): [10000000],
            },
        },
    }
}
```



### 空值处理列表
```python
['', ' ', 'nan', 'na', 'n/a', 'null', '#N/A', '#NA', 
 '-nan', '1.#qnan', '<NA>', pd.NA, np.nan, None, 
 'NaN', 'Null', 'NULL', 'None', 'nan%']
```

### 跳过表头行
```python
# 跳过前3行包含合并单元格的表头
results = extract_data_with_excel_dict(
    "with_header.xlsx",
    template=[[None, None]],
    skiprows=3
)
```
