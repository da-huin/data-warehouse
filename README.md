<p align="center">
  <a href="" rel="noopener">
 <img width=200px height=200px src="./static/icon.png" alt="Project logo" ></a>
 <br>

 
</p>

<h3 align="center">Data Warehouse</h3>

<div align="center">

[![Status](https://img.shields.io/badge/status-active-success.svg)]()
[![GitHub Issues](https://img.shields.io/github/issues/da-huin/data_warehouse.svg)](https://github.com/da-huin/data_warehouse/issues)
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/da-huin/data_warehouse.svg)](https://github.com/da-huin/data_warehouse/pulls)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

<p align="center"> 데이터 웨어하우스를 만들 때 사용하는 인터페이스입니다.
    <br> 
</p>

## 📝 Table of Contents

- [About](#about)
- [Getting Started](#getting_started)
- [Usage](#usage)
- [Acknowledgments](#acknowledgement)

## 🧐 About <a name = "about"></a>

데이터 저장소를 구축 할 때 복잡성을 줄이기 위해 인터페이스가 있으면 복잡성이 줄어들기 때문에 만들어졌습니다.

인터페이스를 사용하면 저장소는 아래와 같은 형식으로 이루어집니다.

### **1. 저장소**

`default/warehouse/raw`

* 여기는 가공되지 않은 데이터들이 저장됩니다.
* `value` 와 `meta` 가 따로 저장됩니다.
    * `value` 는 실제 데이터이고 `meta` 는 크롤링 시간과 같은 메타 정보가 저장됩니다.

`default/warehouse/discovery`

* 여기는 반정형, 정형데이터가 저장됩니다.
* 이 저장소도 위의 저장소와 같이 `value` 와 `meta` 가 따로 저장됩니다.

`(deprecated) default/warehouse/normal`

* `value` 와 `meta` 를 같이 저장하던 저장소지만, 가공 비용 문제로 사용중지 되었습니다.

### **2. 저장소에 데이터를 저장하는 방법**

1.  `Save` 함수로 저장합니다.

### **3. 저장소에서 데이터 불러오는 방법**

1. (Best Practice) `AWS Glue Crawler` 로 크롤링 후 `AWS Athena Query` 로 불러오기

    * `Athena Query` 함수가 있습니다.

1. `List`, `Load` 함수로 불러오기

1. 어떻게든 불러오기

## 🏁 Getting Started <a name = "getting_started"></a>

### Installing

```
pip install data_warehouse
```

<a name="prerequisites"></a>

### Prerequisites 

#### 1. 핸들러 생성하기

아래의 코드로 핸들러를 생성하세요.

```python

import data_warehouse
import time

bucket_name = "YOUR_BUCKET_NAME"

# 생략해도 되지만 생략하면 save, load, list 함수를 사용 할 때 table_name 을 넣어주어야 합니다.
table_name = "YOUR_TABLE_NAME"

# ~/.aws/config. 에 인증파일이 있다면 생략해도 됩니다.
aws_access_key_id = "YOUR_AWS_ACCESS_KEY_ID"
aws_secret_access_key = "YOUR_AWS_SECRET_ACCESS_KEY"

region_name = "YOUR_REGION_NAME"

handler = data_warehouse.DataWarehouse(bucket_name, table_name, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key, region_name=region_name)

print(handler)
```

#### 2. 이제 웨어하우스 인터페이스를 사용 할 수 있게 되었습니다.

## 🎈 Usage <a name="usage"></a>

`Usage` 를 시작하기 전에 [Prerequisites](#prerequisites) 를 확인하세요.

### 🌱 **raw_save & discovery_save**

* 만약 parquet 확장자로 저장하려면 `fastparquet` 와 `pandas` 를 설치해야 합니다.

* 기본적으로 gz 압축되어 저장됩니다.

`RAW 저장되는 경로`: raw_save 는 비정형 데이터를 저장 할 때 사용합니다. 저장되는 경로는 아래와 같습니다.

    default/warehouse/raw/[table_kind]/[table_name]/[ymd]/[user_path]

`DISCOVERY 저장되는 경로`: discovery_save 는 반정형, 정형 데이터를 저장 할 때 사용합니다. 저장되는 경로는 아래와 같습니다.

    default/warehouse/discovery/[table_kind]/[table_name]/[ymd]/[user_path]

**Parameters**

* `(required) user_path`: str

    위의 `저장되는 경로` 를 참조하세요.

* `(required) value`: dict | list | str | bytes | int | float | ...

    저장 할 데이터입니다.

* `meta`: dict (default = {})

    저장한 시간 등 기본 메타데이터가 저장되는데 추가로 저장 할 메타데이터입니다.

* `use_ymd`: bool (default = True)

    위의 `저장되는 경로` 에서 ymd 부분을 자동으로 오늘 날짜로 지정 할 지 결정합니다. False 라면 ymd 폴더가 생성되지 않습니다.

* `table_name`: str (default = "")

    테이블 이름입니다. 만약 핸들러를 만들 때 table_name 을 넣어주었다면 생략해도 됩니다.

* `table_kind`: str (default = "general")

    테이블 분류입니다. 특별한 목적이 없다면 기본값으로 두는 편이 좋습니다.

* `ymd`: str (default = "")
    
    use_ymd 는 현재 시간으로 저장되는데, 현재 시간이 아닌 다른 시간으로 저장 할 때 사용하는 매개변수입니다.
    
    이 매개변수를 2020-08-30 과 같은 형식으로 입력하면 `저장되는 경로` 에서 ymd가 2020-08-30 으로 바뀝니다.

**Returns**

* `S3에 저장 된 파일의 URI`: str

**Examples**
* raw_save
    ```python
    path = "hello/world/apple.json"

    value = "<html><div>Hello World</div></html>"
    meta = {
        "time_ns": time.time_ns()
    }

    url = handler.raw_save(path, value, meta, use_ymd=False)
    print(url)
    ```
    실행결과
    ```
    https://BUCKET_NAME.s3.ap-northeast-2.amazonaws.com/default/warehouse/discovery/general/value/TABLE_NAME/hello/world/apple.json.gz
    ```

* discovery_save
    ```python
    path = "hello/world/apple.json"
    value = {
        "kind": "fruit",
        "price": "1200"
    }

    meta = {
        "time_ns": time.time_ns()
    }


    url = handler.discovery_save(path, value, meta, use_ymd=False)
    print(url)
    ```
    실행결과
    ```
    https://BUCKET_NAME.s3.ap-northeast-2.amazonaws.com/default/warehouse/raw/general/value/TABLE_NAME/hello/world/apple.json.gz
    ```

### 🌱 **raw_load & discovery_load**

`RAW 불러오는 경로`: raw_load 는 비정형 데이터를 불러 올 때 사용합니다. 불러오는 경로는 아래와 같습니다.

    default/warehouse/raw/[table_kind]/[table_name]/[ymd]/[user_path]

`DISCOVERY 불러오는 경로`: discovery_load 는 반정형, 정형 데이터를 불러 올 때 사용합니다. 불러오는 경로는 아래와 같습니다.

    default/warehouse/discovery/[table_kind]/[table_name]/[ymd]/[user_path]

**Parameters**

* `(required) user_path`: str

    위의 `불러오는 경로` 를 참조하세요.

* `table_name`: str (default = "")

    테이블 이름입니다. 만약 핸들러를 만들 때 table_name 을 넣어주었다면 생략해도 됩니다.

* `table_kind`: str (default = "general")

    테이블 분류입니다. 특별한 목적이 없다면 기본값으로 두는 편이 좋습니다.

* `ymd`: str (default = "")
    
    이 매개변수를 2020-08-30 과 같은 형식으로 입력하면 `불러오는 경로` 에서 ymd가 2020-08-30 으로 바뀝니다.

* `is_meta`: bool (default = False)

    메타데이터를 불러 올 지 데이터를 불러 올 지 설정하는 매개변수입니다.


**Returns**

* `불러온 데이터`: str | dict | list | ...

**Examples**

* raw_load
```python
path = "hello/world/apple.json"
print(handler.raw_load(path))
```

실행결과

```html
<html><div>Hello World</div></html>
```

* discovery_load
```python
path = "hello/world/apple.json"

print(handler.discovery_load(path))
```

실행결과

```python
{'kind': 'fruit', 'price': '1200'}
```

### 🌱 **raw_list & discovery_list**

`RAW 리스트하는 경로`: raw_load 는 비정형 데이터를 리스트 할 때 사용합니다. 리스트하는 경로는 아래와 같습니다.

    default/warehouse/raw/[table_kind]/[table_name]/[ymd]/[user_dir]

`DISCOVERY 리스트하는 경로`: discovery_load 는 반정형, 정형 데이터를 리스트 할 때 사용합니다. 리스트하는 경로는 아래와 같습니다.

    default/warehouse/discovery/[table_kind]/[table_name]/[ymd]/[user_dir]

**Parameters**

* `(required) user_dir`: str

    위의 `리스트하는 경로` 를 참조하세요.

* `table_name`: str (default = "")

    테이블 이름입니다. 만약 핸들러를 만들 때 table_name 을 넣어주었다면 생략해도 됩니다.

* `table_kind`: str (default = "general")

    테이블 분류입니다. 특별한 목적이 없다면 기본값으로 두는 편이 좋습니다.

* `ymd`: str (default = "")
    
    이 매개변수를 2020-08-30 과 같은 형식으로 입력하면 `리스트하는 경로` 에서 ymd가 2020-08-30 으로 바뀝니다.

* `is_meta`: bool (default = False)

    메타데이터를 리스트 할 지 데이터를 리스트 할 지 설정하는 매개변수입니다.

* `load` bool (default = False)

    리스트하며 데이터를 불러올 수 있습니다.

* `is_direoctry`: bool (default = False)

    파일이 아닌 디렉토리를 리스트 할 수 있습니다.

* `includes`: str list 

    리스트를 이 매개변수에 포함된 값으로 필터합니다.


**Returns**

* `리스트 결과`: list

**Examples**

* raw_list

    ```python
    dirname = "hello/world"
    print(handler.raw_list(user_dir=dirname))
    ```

    실행결과

    ```python
    [{'key': 'default/warehouse/raw/general/value/TABLE_NAME/hello/world/apple.json.gz', 'data': '<html><div>Hello World</div></html>'}]
    ```

* discovery_list

    ```python
    dirname = "hello/world"
    print(handler.discovery_list(user_dir=dirname))
    ```

    실행결과

    ```python
    [{'key': 'default/warehouse/discovery/general/value/TABLE_NAME/hello/world/apple.json.gz', 'data': {'kind': 'fruit', 'price': '1200'}}]
    ```

### 🌱 **load_with_athena_query**

아테나로 데이터를 불러 올 때 사용합니다.

**Parameters**

* `query`: str

    아테나로 요청할 쿼리입니다.

    ```
    SELECT * FROM dbname.tablename
    ```

* `request_limit`: int (default=10000)


**Returns**

* `아테나 쿼리 결과`: list

**Examples**

```
data = load_with_athena_query("SELECT * FROM dbname.tablename")
```

### 🌱 **load_with_full_path**

S3 전체 경로로 데이터를 불러와야 할 때 사용하는 함수입니다.

**Parameters**

* `full_path`: str

**Returns**

* `불러온 데이터`: dict | list | str | ...

**Examples**

```python
>>> path = "default/warehouse/discovery/general/value/TABLE_NAME/hello/world/apple.json.gz"
>>> print(handler.load_with_full_path(path))
{'kind': 'fruit', 'price': '1200'}
```

### 🌱 **load_meta_with_full_path**

S3 전체 경로로 메타 데이터를 불러와야 할 때 사용하는 함수입니다.

**Parameters**

* `full_path`: str

**Returns**

* `불러온 메타 데이터`: dict

**Examples**

```python
>>> path = "default/warehouse/discovery/general/value/TABLE_NAME/hello/world/apple.json.gz"
>>> print(handler.load_meta_with_full_path(path))
{'default_table_name': 'hello', 'discovery_time_ns': 1596991652526973300, 'discovery_full_path': 'default/warehouse/discovery/general/value/TABLE_NAME/hello/world/apple.json.gz', 'discovery_stored_time': '2020-08-10 01:47:32.526973', 'discovery_ymd': '2020-08-10'}
```

## 🎉 Acknowledgements <a name = "acknowledgement"></a>

- Title icon made by [Freepik](https://www.flaticon.com/kr/authors/freepik).

- If you have a problem. please make [issue](https://github.com/da-huin/data_warehouse/issues).

- Please help develop this project 😀

- Thanks for reading 😄
