# Apache Beam


- Apache Beam은 일괄 및 스트리밍 데이터 동시 처리 파이프라인을 정의할 수 있는 오픈 소스 통합 모델이다.
- Apache Beam 프로그래밍 모델을 사용하면 대규모 데이터 처리 방식이 간단해진다.
- Beam SDK를 사용하여 파이프라인을 정의하는 프로그램을 빌드할 수 있습니다.
- 그러면 `Dataflow` 와 같은 분산 처리 백엔드 중 하나가 파이프라인을 실행합니다.
- 유용한 추상화가 제공되어 개별 작업자 조정, 데이터셋 샤딩, 작업과 같은 분산 처리의 세밀한 부분에 신경쓰지 않아도 됩니다.

---

### 기본개념

- 파이프라인
    - 파이프라인은 입력 데이터 읽기, 데이터 변환, 출력 데이터 쓰기와 관련된 일련의 계산 전체를 캡슐화합니다.
        - reading input data
        - tranforming that data
        - writing output data
    - 입력 소스와 출력 싱크는 같은 타입 혹은 서로 다른 타입일 수 있으며 타입 변환도 가능합니다.
    - Beam 프로그램은 `Pipeline` 객체 생성 후 이 객체를 파이프라인의 데이터 셋을 만드는 기초로 사용하는 것부터 시작합니다.
    - 각 파이프라인은 반복 가능한 단일 작업을 나타냅니다.

- PColleciton
    - `PCollection` 은 파이프라인의 데이터 역할을 하는 잠재적으로 분산된 다중 요소 데이터세트를 나타냅니다.
    - Apache Beam은 `PCollection` 객체를 파이프라인의 각 단계에서 입력 및 출력으로 사용합니다.
    - `PCollection` 은 지속적으로 업데이트되는 데이터 소스에서 고정된 크기의 데이터셋 또는 제한되지 않은 데이터셋을 보관할 수 있습니다.
    - 스파크의 `RDD` 와 동일함. 빔 파이프라인이 동작하는 분산 데이터를 표현
    - 배치/스트리밍 데이터
        - bounded : 데이터의 양을 알고, 파일과 같이 고정된 소스로부터 온다는 것 `배치작업`
        - unbounded : kafka, Google pub-sub, sockets, stream API와 같이 지속적으로 데이터가 유입되는 경우  `스트리밍`
    - Beam은 배치, 스트리밍 데이터에 대한 1 unified abstraction 만을 가진다.
        - `Spark`
            - Bounded - DataFrame
            - Unbounded - DStreams
        - `Flink`
            - Bounded - DataSet
            - Unbounded - DataStream
    - 불변성
        - Pcollection은 본질적으로 불변성을 가진다.
        - Pcollection에 변환을 하면 새로운 pcollection이 생성된다.
        
    - 데이터타입
        - PCollection은 어떤 타입도 가능하다. 하지만 반드시 같은 타입을 가져야한다.
        - 분산 프로세싱을 지원하기위해선 빔이 각 요소를 바이트 스트링 타입으로 인코딩해야한다. 그래서 이 값을 분산 worker에게 주입할 수 있다.
        - 빔 SDK가 데이터 인코딩 메커니즘을 제공한다. `Builtin encoding`, `custom encoding`
    - Operation Type
        - Pcollection이 grained operation을 지원하지 않는다.
        - Pcollection의 특정 element에 대한 변환을 적용할 수는 없다.
        - 변환은 일부 요소가 아닌 전체에 적용됨
    
    - Timestamps
        - Pcollection의 각 요소는 타임스탬프값을 가진다.
            - 스트리밍: source assigns the timestamp
            - 배치: Every element is set to same timestamp
    - PTransform
        - 파이프라인에서 데이터 프로세싱 동작을 말한다.
            - `ParDo` , `Filter` , `flatten` , `combine` 등
- 변환
    - 변환은 데이터를 변환하는 처리 작업을 나타냅니다.
    - `PCollcetion` 한 개 이상을 입력으로 사용하고, 컬렉션의 각 요소에 지정된 작업을 수행하고, `PCollection` 한 개 이상을 출력으로 생성합니다.
    - 데이터에서 수학적 계산 수행, 한 형식에서 다른 형식으로 변환, 그룹화, 읽기 쓰기, 필터링, 데이터 요소를 단일 값으로 결합 등을 포함한 거의 모든 종류의 처리 작업을 수행할 수 있습니다.
    
- ParDO
    - `ParDo` 는 Beam SDK의 핵심 동시 처리 작업으로, 입력 `PCollection` 의 각 요소에 사용자 지정 함수를 호출합니다. `ParDo` 는 출력 `PCollection` 에서 출력 요소를 0개 이상 수집합니다.
    - `ParDo` 변환에서는 요소를 독립적으로 동시에 처리할 수 있습니다.

- 집계
    - 여러 입력 요소에서 일부 값을 계산하는 과정입니다.
    - Apache Beam 집계의 기본 계산 패턴은 모든 요소를 공통 키와 기간으로 그룹화하는 것입니다.
    - 그런 다음 각 요소 그룹을 결합합니다.
    
- 실행기
    - 파이프라인을 수락하고 실행하는 소프트웨어입니다.
    - 대부분의 실행기는 빅데이터 처리 시스템을 대규모로 동시 처리하는 번역기 또는 어댑터입니다.
    - 다른 실행기는 로컬 테스트 및 디버깅 용입니다.

- 소스
    - 외부 스토리지 시스템에서 읽는 변환입니다.
    - 파이프라인은 일반적으로 소스에서 입력 데이터를 읽습니다.
    - 소스 유형은 싱크 유형과 다를 수 있으므로 파이프라인을 통해 이동하는 데이터 형식을 변경할 수 있습니다.

- 싱크
    - 파일 또는 데이터베이스와 같은 외부 데이터 스토리지 시스템에 쓰는 변환 입니다.
    
- 이벤트 시간
    - 데이터 이벤트가 발생한 시간으로, 데이터 요소 자체의 타임스탬프로 결정됩니다.
    - 이는 파이프라인의 임의 단계에서 실제 데이터 요소가 처리되는 시간과 다릅니다.

- 윈도우
    - 윈도우에서는 개별 요소의 타임스탬프에 따라 컬렉션을 유한 컬렉션의 기간으로 나눠 제한되지 않은 컬렉션에서 그룹화 작업을 수행할 수 있습니다.
    - 윈도우 함수는 요소를 최초 기간에 할당하는 방법과 그룹화된 요소의 기간을 병합하는 방법을 실행기에 알려줍니다.
    - Apache Beam에서는 여러 종류의 기간을 정의하거나 사전 정의된 윈도우 함수를 사용할 수 있습니다.
    
- 워터마크
    - Apache Beam은 특정 기간의 모든 데이터가 파이프라인에 도착했다고 예측할 수 있는 시점에 대한 시스템 표기법인 워터마크를 추적합니다.
    - 데이터가 시간 순서대로 또는 예측 가능한 간격으로 파이프라인에 도착하지 않을 수 있으므로 Beam은 워터마크를 추적합니다.
    - 데이터 이벤트가 생성된 순서와 동일한 순서로 파이프라인에 나타나지 않을 수 있습니다.
    
- 트리거
    - 데이터가 도착할 때 집계된 결과를 내보낼 시점을 결정합니다.
    - 제한된 데이터의 경우 모든 입력이 처리된 후에 결과가 내보내기 됩니다.
    - 제한되지 않은 데이터의 경우 시스템이 해당 기간의 모든 입력 데이터가 처리됐다고 판단했음을 나타내는 워터마크의 기간 끝 통과 시점에 결과가 내보내기 됩니다.
    - Beam에서는 사전 정의된 몇 가지 트리거가 제공되며, 개발자는 이러한 트리거를 결합할 수 있습니다.
    

---

### Beam Programming Guide

[Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)

---

### Beam Architecture

- SDKS

`Java SDK, Python SDK, Other SDK` ⇒ 

- Runner API & SDK workers

`Beam/Runner API` 

- Runners/Executors
    - 파이프라인이 어디서 실행되는지 결정(Spark, flink, apex 등)
    - 

`Spark, Flink, Apex, Cloud DataFlow, Others` ⇒ `Java, python worker`

---

### 빔 프로그래밍 모델의 처리 흐름

1. Input
    - 텍스트 파일, 로그 파일, 데이터베이스, 스트림(카프카, pub/sub)
    - `READ` ⇒ `p1`
        - 데이터를 읽음
        - 변환을 통해 `immutable` 한 새로운 pcollection을 생성
2. PCollection1 ⇒ Transform 작업 `READ` , `FILTER`
3. PCollection2 `GROUP BY` `SUM`
4. Output
    - 텍스트 파일, 인메모리, hdfs, gfs, stream(kafka, pub/sub)
    

```python
# overall flow of beam programming

import apache_beam as beam

p1 = beam.Pipeline()

attendence_count = (

    p1
    |beam.io.ReadFromText('dept_data.txt')
    |beam.Map(lambda record: record.split(','))
    |beam.Filter(lambda record: record[3] == 'Account')
    |beam.Map(lambda record: (record[1], 1))
    |beam.CombinePerKey(sum)
    |beam.io.WriteToText('data/output_new_final')
)

p1.run()
```

---


---

## READ TRANSFORMS

### ReadFromText

- parse a text files as newline delimited elements i.e
- it reads the file line by line and every line is a single element in pCollection
- parameters
    - file_patterns (str)
        - This mandatory parameter specifies the full path of the input file. Path can contain glob characters
        - ex) `data/input*.txt`
    - min_bundle_size (int)
        - Specifies the minimum size of bundles that should be generated when splitting the source into bundles
    - 전체를 한번에 처리하지는 않고 여러개의 번들을 만들어서 배치처리해서 병렬적으로 다른 머신에서 계산작업을 하게된다.
    - compression_type (str)
        - Specifies the compression type of input file
        - 예를들어 `gzip` 의 경우 `/data/input.gzip`
    - strip_trailing_newlines (boolean)
        - Indicates whether source should remove the newline character(/n) from each line before reading it. By default `true`
            
            ```python
            beam.io.ReadFromText('demo.txt', strip_trailing_newlines=True)
            
            >>
            Hello there
            How are you
            
            beam.io.ReadFromText('demo.txt', strip_trailing_newlines=True)
            
            >>
            Hello there
            
            How are you
            ```
            
    - validate (boolean)
        - If set to true, it will verify the presence of file during pipeline creating. By default `True`
    
    - skip_header_lines (int)
        - Specifies the number of header lines you want to skip from your input file. By default `0`

---

### ReadFromAvro - Used to read Avro files

parameters

- file_pattern
- min_bundle_size
- validate
- use_fastavro (boolean)
    - When set to true, uses the `fastavro` library to read the avro file.
    

---

### ReadFromParquet - Used to read Parquet files

parameters

- file_pattern
- min_bundle_size
- validate
- columns (list[str])
    - specifies the list of columns that will be read from the input file

---

### ReadFromTFRecord - Used to read tensorflow records

- simple format for stroing a sequence of binary records
- these records are nowdays become famous since they are serialized and therefore faster to stream over the network.
- This format can also be useful for caching any data-preprocessing

parameters

- file_pattern
- validate
- compression_type
- coder (str)
    - specifies the coder name to decode the input record. By default `bytesCoder`

---

## Messaging queue

### Apache kafka, Amazon kinesis, JMS, MQTT, Google Cloud Pub

### ReadFromPubSub - Used to read messages from Google Pub/sub service

- topic (str)
    - specifies the topic name where the messages are getting published and Beam has to read it
- subscription (str)
    - Specifies the subscription name from where Beam has to read the messages
- id_label (str)
    - Provide the attribute from incoming messages which should be considered as unique identifier
- with_attributes (boolean)
    - If set to true then output elements will be of type `object` , otherwise of type `bytes` . By default False

- timestamp_attributes (int)
    - specifiy the value to be used as element timestamp. Specified argument should be a numerical value representing the number of milliseconds since the unix epoch

---

### Apache cassandra, HBase, Kudu, BigQuery, MongoDB, Redis, GoogleCloud Datastore

---

## WRITE TRANSFORMS

### WriteToText() - Writes each element of the PCollection as a single line in the output file

- parameters
    - file_path_prefix (str)
        - This mandatory parameter specifies the file path to write the pcollection to. Also the files will be written with the specified prefix
        - `WriteToText('/data/part')`
        - `<Prefix><num_shards><Suffix>` : A prefix followed by  a shard identifier. number of shards followed by a suffix
            - ex: part-0000-of-0001-accounts
                - prefix : part-0000
                - num_shards: 0001
                - accounts: suffix
    - file_name_suffix (str)
        - This specifies the suffix of output file name
    - num_shards
        - This specifies the no. of shards / no. of files written as output
        - 파이프라인 성능을 제한시킬 수 있으므로 웬만하면 건들이지 않는 것이 좋다.
    - append_trailing_newlines (boolean)
        - Decides if the lines should be delimeted with newline. By defalut true
    - coder (str)
        - Specifies the coder name to encode each line
    - compression_type (str)
        - Specifies the compression type of the Output file
    - header (str)
        - Specifies the header line of the output file

---

### WriteToAvro - Writes each element of the PCollection to Avro File

parameters

- file_path_prefix (str)
    - This mandatory parameter specifies the file path to write the PCollection to. Also the files will be written with the specified prefix
- file_name_suffix (str)
    - This specifies the suffix of output file name
- num_shards (int)
    - This specifies the no. of shards  / no. of files written as output
- codec
    - This is the compression codec to use for block level compression. By default ‘defalte’
- compression_type (str)
    - Specifies the compression type of the Output file
- use_fastavro (boolean)
    - When set to true, uses the ‘fastavro’ library to write the avro file
- mine_type
    - MIME type to use for the produced output files

---

### WriteToParquet - Writes each element of the PCollection to Parquet file

parameter

- file_path_prefix (str)
    - This mandatory parameter specifies the file path to write the PCollection to. Also the files will be written with the specified prefix
- file_name_suffix (str)
    - This specifies the suffix of output file name
- num_shards (int)
    - This specifies the no. of shards / no. of files writeen as output
- codec
    - This is the compression codec to use for block level compression
- mime_type
    - MIME type to use for the produced output files
- schema
    - The schema to use for writing, as returned by `avro.schema.Parse`
- row_group_buffer_size
    - Specifies the byte size of the row group buffer. Default `67108864`
    - where row group is a segment of parquet file that holds serialized arrays of column entries
- record_batch_size
    - Specifies the number of records in each record batch. default `1000`
        - batch is a basic unit used for storing data in the row group buffer

---

### WriteToTFRecord

- file_path_prefix
- file_name_prefix
- num_shards
- compression_type

---

### WriteToPubSub

- topic (str)
- with_attributes (boolean)
    - If set to true then input elements will be of type `Object` , otherwise of type `Bytes` By default `False`
- id_label (str)
    - If set, will set an attribute for each Cloud pubsub message with given name and a unique value
- timestamp_attribute (int)
    - If set, will set an attribute for each Cloud pubsub message with the given name and the messages’s publish time as the value

---

## Map, FlatMap & Filter

```python
# If you want to explicitly output a list from FlatMap
# then type cast it to a list

import apache_beam as beam

def split_row(elem):
    # Explicityle represent it as a list
    return elem.split(',')

def filter(record):
    return record[3] == 'Accounts'

p7 = beam.Pipeline()

lines = (
    p7
    | 'Read From Text' >> beam.io.ReadFromText('dept_data.txt')
    | beam.Map(split_row)
    | beam.Filter(filter)
    | beam.Map(lambda record: (record[1], 1))
    | beam.CombinePerKey(sum)
    | beam.io.WriteToText('data/output_new7')
)

p7.run()

!{'head -n 10 data/output_new7-00000-of-00001'}
```

```python
with beam.Pipeline() as p:
    lines = (
        p
        | beam.io.ReadFromText('dept_data.txt')
        | beam.Map(split_row)
        | beam.io.WriteToText('data/output_with')
    )

# p7.run()
!{'head -n 10 data/output_with-00000-of-00001'}
```

---

## Multi branches

- 하나의 PCollection이 분기되는 경우

```python
import apache_beam as beam

def split_row(elem):
    return elem.split(',')

p = beam.Pipeline()

input_collection = (
    p
    | 'Read from text file' >> beam.io.ReadFromText('dept_data.txt')
    | 'Split rows' >> beam.Map(split_row)
)

accounts_count = (
    input_collection
    | 'Get all accounts dept persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
    | 'Pair each accounts emplyee with 1' >> beam.Map(lambda record: (record[1], 1))
    | 'Group and sum1' >> beam.CombinePerKey(sum)
    | 'Write results for account' >> beam.io.WriteToText('data/Account')
)

hr_count = (
    input_collection
    | 'Get all HR dept persons' >> beam.Filter(lambda record: record[3] == 'HR')
    | 'Pair each HR emplyee with 1' >> beam.Map(lambda record: (record[1], 1))
    | 'Group and sum' >> beam.CombinePerKey(sum) # accounts_count의 verbose와 달라야함
    | 'Write results for HR' >> beam.io.WriteToText('data/HR')
)

p.run()

!{'head -n 10 data/Account-00000-of-00001'}
print('--------------------------------')
!{'head -n 10 data/HR-00000-of-00001'}

('Marco', 31)
('Rebekah', 31)
('Itoe', 31)
('Edouard', 31)
('Kyle', 62)
('Kumiko', 31)
('Gaston', 31)
('Ayumi', 30)
--------------------------------
('Beryl', 62)
('Olga', 31)
('Leslie', 31)
('Mindy', 31)
('Vicky', 31)
('Richard', 31)
('Kirk', 31)
('Kaori', 31)
('Oscar', 31)
```

- merge

```python
import apache_beam as beam

def split_row(elem):
    return elem.split(',')

p2 = beam.Pipeline()

input_collection = (
    p2
    | 'Read from text file' >> beam.io.ReadFromText('dept_data.txt')
    | 'Split rows' >> beam.Map(split_row)
)

accounts_count = (
    input_collection
    | 'Get all accounts dept persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
    | 'Pair each accounts emplyee with 1' >> beam.Map(lambda record: ('Account ' + record[1], 1))
    | 'Group and sum1' >> beam.CombinePerKey(sum)
    # | 'Write results for account' >> beam.io.WriteToText('data/Account')
)

hr_count = (
    input_collection
    | 'Get all HR dept persons' >> beam.Filter(lambda record: record[3] == 'HR')
    | 'Pair each HR emplyee with 1' >> beam.Map(lambda record: ('HR ' + record[1], 1))
    | 'Group and sum' >> beam.CombinePerKey(sum) # accounts_count의 verbose와 달라야함
    # | 'Write results for HR' >> beam.io.WriteToText('data/HR')
)

output = (
    (accounts_count, hr_count)
    | beam.Flatten()
    | beam.io.WriteToText('data/both')
)

p2.run()

!{'head -n 10 data/both-00000-of-00001'}
print("-------------")
!{'tail -n 10 data/both-00000-of-00001'}
```

---

### ParDo

- ParDo processing paradigm is similar to the `“Map”` phase of a map-reduce program
- A Pardo transform takes each element of input PCollection , performs processing function on it and emits 0, 1 or multiple elements

 

Use case of ParDO

- `Filtering` : ParDo can take each element of Pcollection and decide either to output or discard it
- `Formatting` or `Type Conversion` : ParDo can change the type of format of input elements
- `Extracting individual parts` : ParDo can be used to extract individual elements from a single element
- `Computations` : ParDo can perform any processing function on the input elements and outputs a PCollection

---

### Combiner

- Beam transform for combining elements or values in your data
- Combiner is a mini `reducer` which does the reduce task locally to a mapper machine

List of Combiner methods

1. create_accumulator
    - Creates a new local accumulator in each machine
    - keeps a record of `(sum, counts)`
2. add_input
    - Adds an input element to accumulator, returning new `(sum, count)` value
3. merge_accumulators
    - Merges all machines accumulators into a single one.
    - In our case all the `(sum, counts)` from various machines are gathered and summed up
4. extract_output
    - Performs the final computation on the `merge_accumulator's result.`
    - Called by on the `merge_accumulators' result`
    

---

### CombineGlobally

- you can pass few built in functions : `sum` , `min`, `max` more complex functions, you can define a subclass of `CombineFn`

```python
import apache_beam as beam

class AverageFn(beam.CombineFn):

    def create_accumulator(self):
        return (0.0, 0) # (sum, count)

    def add_input(self, sum_count, input):
        (sum, count) = sum_count
        return sum + input, count + 1
    
    def merge_accumulators(self, accumulators):
        ind_sums, ind_counts = zip(*accumulators) # zip = [(27,3),(39,3),(18,2)] --> [(27,39,18), (3,3,2)]
        return sum(ind_sums), sum(ind_counts) # (84,8)

    def extract_output(self, sum_count):
        (sum, count) = sum_count # combine globally using CombineFn
        return sum / count if count else float('NaN')

p = beam.Pipeline()

lines = (
    p
    | beam.Create([15,5,7,7,9,23,13,5])
    | beam.CombineGlobally(AverageFn())
    | beam.io.WriteToText('data/combine')
)

p.run()

!{'head -n 10 data/combine-00000-of-00001'}
```

---

## Composite transform

- branch가 생시면 branch 마다 transform logic이 추가되는 경우 각 브랜치마다 코드를 수정해야하는 문제가 발생한다.
- composite transform은 group multiple transformations 을 single unit 로 적용한다

```python
import apache_beam as beam

class MyTransform(beam.PTransform):

    def expand(self, input_col):
        t = (
            input_col
             | 'Group and sum1' >> beam.CombinePerKey(sum)
             | 'count filter' >> beam.Filter(filter_on_count)
             | 'Regular accounts employee' >> beam.Map(format_output) 
        )
        return t

def split_row(elem):
    return elem.split(',')

def filter_on_count(elem):
    name, count = elem
    if count > 30:
        return elem
    
def format_output(elem):
    name, count = elem
    return ', '.join((name, str(count), 'Regular Employee'))

p = beam.Pipeline()

input_collection = (
    p
    | 'Read from text file' >> beam.io.ReadFromText('dept_data.txt')
    | 'Split rows' >> beam.Map(split_row)
)

accounts_count = (
    input_collection
    | 'Get all accounts dept persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
    | 'Pair each accounts employee with 1' >> beam.Map(lambda record: ('Accounts ' + record[1], 1))
    | 'composite accounts' >> MyTransform()
    | 'Write results for accounts' >> beam.io.WriteToText('data/accounts_comp')    
)

hr_count = (
    input_collection
    | 'Get all HR dept persons' >> beam.Filter(lambda record: record[3] == 'HR')
    | 'Pair each HR employee with 1' >> beam.Map(lambda record: ('hr ' + record[1], 1))
    | 'composite hr' >> MyTransform()
    | 'Write results for hr' >> beam.io.WriteToText('data/hr_comp')    
)

p.run()

!{'head -n 10 data/accounts_comp-00000-of-00001'}

print("-" * 100)

!{'head -n 10 data/hr_comp-00000-of-00001'}

WARNING:root:Make sure that locally built Python SDK docker image has Python 3.7 interpreter.
Accounts Marco, 31, Regular Employee
Accounts Rebekah, 31, Regular Employee
Accounts Itoe, 31, Regular Employee
Accounts Edouard, 31, Regular Employee
Accounts Kyle, 62, Regular Employee
Accounts Kumiko, 31, Regular Employee
Accounts Gaston, 31, Regular Employee
----------------------------------------------------------------------------------------------------
hr Beryl, 62, Regular Employee
hr Olga, 31, Regular Employee
hr Leslie, 31, Regular Employee
hr Mindy, 31, Regular Employee
hr Vicky, 31, Regular Employee
hr Richard, 31, Regular Employee
hr Kirk, 31, Regular Employee
hr Kaori, 31, Regular Employee
hr Oscar, 31, Regular Employee
```

---

## CoGroupByKey

- Relational join of two or more key/value PCollection
- Accepts a dictionary of key/value PCollections and output a single PCollection containing 1 key/value Tuple for each key in the input PCollections
- 파이썬 SDK에서 JOIN

```python
import apache_beam as beam

def return_tuple(elem):
    t = elem.split(',')
    return (t[0], t[1:])

p = beam.Pipeline()

# Apply a ParDo to the PCollection "words" to compute lengths for each word.
dep_rows = (
    p
    | "Reading File 1" >> beam.io.ReadFromText('dept_data.txt')
    | "Pair each employee with key" >> beam.Map(return_tuple)
)

loc_rows = (
    p
    | "Reading File 2" >> beam.io.ReadFromText('location.txt')
    | "Pair each loc with key" >> beam.Map(return_tuple)
)

results = ({
    "dep_data" : dep_rows,
    "loc_data" : loc_rows}
    | beam.CoGroupByKey()
    | 'Write results' >> beam.io.WriteToText('data/result')
)

p.run()

!{'head -n 10 data/result-00000-of-00001'}

('149633CM', {'dep_data': [['Marco', '10', 'Accounts', '1-01-2019'], ['Marco', '10', 'Accounts', '2-01-2019'], ['Marco', '10', 'Accounts', '3-01-2019'], ['Marco', '10', 'Accounts', '4-01-2019'], ['Marco', '10', 'Accounts', '5-01-2019'], ['Marco', '10', 'Accounts', '6-01-2019'], ['Marco', '10', 'Accounts', '7-01-2019'], ['Marco', '10', 'Accounts', '8-01-2019'], ['Marco', '10', 'Accounts', '9-01-2019'], ['Marco', '10', 'Accounts', '10-01-2019'], ['Marco', '10', 'Accounts', '11-01-2019'], ['Marco', '10', 'Accounts', '12-01-2019'], ['Marco', '10', 'Accounts', '13-01-2019'], ['Marco', '10', 'Accounts', '14-01-2019'], ['Marco', '10', 'Accounts', '15-01-2019'], ['Marco', '10', 'Accounts', '16-01-2019'], ['Marco', '10', 'Accounts', '17-01-2019'], ['Marco', '10', 'Accounts', '18-01-2019'], ['Marco', '10', 'Accounts', '19-01-2019'], ['Marco', '10', 'Accounts', '20-01-2019'], ['Marco', '10', 'Accounts', '21-01-2019'], ['Marco', '10', 'Accounts', '22-01-2019'], ['Marco', '10', 'Accounts', '23-01-2019'], ['Marco', '10', 'Accounts', '24-01-2019'], ['Marco', '10', 'Accounts', '25-01-2019'], ['Marco', '10', 'Accounts', '26-01-2019'], ['Marco', '10', 'Accounts', '27-01-2019'], ['Marco', '10', 'Accounts', '28-01-2019'], ['Marco', '10', 'Accounts', '29-01-2019'], ['Marco', '10', 'Accounts', '30-01-2019'], ['Marco', '10', 'Accounts', '31-01-2019']], 'loc_data': [['9876843261', 'New York'], ['9204232778', 'New York']]})
('212539MU', {'dep_data': [['Rebekah', '10', 'Accounts', '1-01-2019'], ['Rebekah', '10', 'Accounts', '2-01-2019'], ['Rebekah', '10', 'Accounts', '3-01-2019'], ['Rebekah', '10', 'Accounts', '4-01-2019'], ['Rebekah', '10', 'Accounts', '5-01-2019'], ['Rebekah', '10', 'Accounts', '6-01-2019'], ['Rebekah', '10', 'Accounts', '7-01-2019'], ['Rebekah', '10', 'Accounts', '8-01-2019'], ['Rebekah', '10', 'Accounts', '9-01-2019'], ['Rebekah', '10', 'Accounts', '10-01-2019'], ['Rebekah', '10', 'Accounts', '11-01-2019'], ['Rebekah', '10', 'Accounts', '12-01-2019'], ['Rebekah', '10', 'Accounts', '13-01-2019'], ['Rebekah', '10', 'Accounts', '14-01-2019'], ['Rebekah', '10', 'Accounts', '15-01-2019'], ['Rebekah', '10', 'Accounts', '16-01-2019'], ['Rebekah', '10', 'Accounts', '17-01-2019'], ['Rebekah', '10', 'Accounts', '18-01-2019'], ['Rebekah', '10', 'Accounts', '19-01-2019'], ['Rebekah', '10', 'Accounts', '20-01-2019'], ['Rebekah', '10', 'Accounts', '21-01-2019'], ['Rebekah', '10', 'Accounts', '22-01-2019'], ['Rebekah', '10', 'Accounts', '23-01-2019'], ['Rebekah', '10', 'Accounts', '24-01-2019'], ['Rebekah', '10', 'Accounts', '25-01-2019'], ['Rebekah', '10', 'Accounts', '26-01-2019'], ['Rebekah', '10', 'Accounts', '27-01-2019'], ['Rebekah', '10', 'Accounts', '28-01-2019'], ['Rebekah', '10', 'Accounts', '29-01-2019'], ['Rebekah', '10', 'Accounts', '30-01-2019'], ['Rebekah', '10', 'Accounts', '31-01-2019']], 'loc_data': [['9995440673', 'Denver']]})
('231555ZZ', {'dep_data': [['Itoe', '10', 'Accounts', '1-01-2019'], ['Itoe', '10', 'Accounts', '2-01-2019'], ['Itoe', '10', 'Accounts', '3-01-2019'], ['Itoe', '10', 'Accounts', '4-01-2019'], ['Itoe', '10', 'Accounts', '5-01-2019'], ['Itoe', '10', 'Accounts', '6-01-2019'], ['Itoe', '10', 'Accounts', '7-01-2019'], ['Itoe', '10', 'Accounts', '8-01-2019'], ['Itoe', '10', 'Accounts', '9-01-2019'], ['Itoe', '10', 'Accounts', '10-01-2019'], ['Itoe', '10', 'Accounts', '11-01-2019'], ['Itoe', '10', 'Accounts', '12-01-2019'], ['Itoe', '10', 'Accounts', '13-01-2019'], ['Itoe', '10', 'Accounts', '14-01-2019'], ['Itoe', '10', 'Accounts', '15-01-2019'], ['Itoe', '10', 'Accounts', '16-01-2019'], ['Itoe', '10', 'Accounts', '17-01-2019'], ['Itoe', '10', 'Accounts', '18-01-2019'], ['Itoe', '10', 'Accounts', '19-01-2019'], ['Itoe', '10', 'Accounts', '20-01-2019'], ['Itoe', '10', 'Accounts', '21-01-2019'], ['Itoe', '10', 'Accounts', '22-01-2019'], ['Itoe', '10', 'Accounts', '23-01-2019'], ['Itoe', '10', 'Accounts', '24-01-2019'], ['Itoe', '10', 'Accounts', '25-01-2019'], ['Itoe', '10', 'Accounts', '26-01-2019'], ['Itoe', '10', 'Accounts', '27-01-2019'], ['Itoe', '10', 'Accounts', '28-01-2019'], ['Itoe', '10', 'Accounts', '29-01-2019'], ['Itoe', '10', 'Accounts', '30-01-2019'], ['Itoe', '10', 'Accounts', '31-01-2019']], 'loc_data': [['9196597290', 'Boston']]})
('503996WI', {'dep_data': [['Edouard', '10', 'Accounts', '1-01-2019'], ['Edouard', '10', 'Accounts', '2-01-2019'], ['Edouard', '10', 'Accounts', '3-01-2019'], ['Edouard', '10', 'Accounts', '4-01-2019'], ['Edouard', '10', 'Accounts', '5-01-2019'], ['Edouard', '10', 'Accounts', '6-01-2019'], ['Edouard', '10', 'Accounts', '7-01-2019'], ['Edouard', '10', 'Accounts', '8-01-2019'], ['Edouard', '10', 'Accounts', '9-01-2019'], ['Edouard', '10', 'Accounts', '10-01-2019'], ['Edouard', '10', 'Accounts', '11-01-2019'], ['Edouard', '10', 'Accounts', '12-01-2019'], ['Edouard', '10', 'Accounts', '13-01-2019'], ['Edouard', '10', 'Accounts', '14-01-2019'], ['Edouard', '10', 'Accounts', '15-01-2019'], ['Edouard', '10', 'Accounts', '16-01-2019'], ['Edouard', '10', 'Accounts', '17-01-2019'], ['Edouard', '10', 'Accounts', '18-01-2019'], ['Edouard', '10', 'Accounts', '19-01-2019'], ['Edouard', '10', 'Accounts', '20-01-2019'], ['Edouard', '10', 'Accounts', '21-01-2019'], ['Edouard', '10', 'Accounts', '22-01-2019'], ['Edouard', '10', 'Accounts', '23-01-2019'], ['Edouard', '10', 'Accounts', '24-01-2019'], ['Edouard', '10', 'Accounts', '25-01-2019'], ['Edouard', '10', 'Accounts', '26-01-2019'], ['Edouard', '10', 'Accounts', '27-01-2019'], ['Edouard', '10', 'Accounts', '28-01-2019'], ['Edouard', '10', 'Accounts', '29-01-2019'], ['Edouard', '10', 'Accounts', '30-01-2019'], ['Edouard', '10', 'Accounts', '31-01-2019']], 'loc_data': [['9468234252', 'Miami']]})
('704275DC', {'dep_data': [['Kyle', '10', 'Accounts', '1-01-2019'], ['Kyle', '10', 'Accounts', '2-01-2019'], ['Kyle', '10', 'Accounts', '3-01-2019'], ['Kyle', '10', 'Accounts', '4-01-2019'], ['Kyle', '10', 'Accounts', '5-01-2019'], ['Kyle', '10', 'Accounts', '6-01-2019'], ['Kyle', '10', 'Accounts', '7-01-2019'], ['Kyle', '10', 'Accounts', '8-01-2019'], ['Kyle', '10', 'Accounts', '9-01-2019'], ['Kyle', '10', 'Accounts', '10-01-2019'], ['Kyle', '10', 'Accounts', '11-01-2019'], ['Kyle', '10', 'Accounts', '12-01-2019'], ['Kyle', '10', 'Accounts', '13-01-2019'], ['Kyle', '10', 'Accounts', '14-01-2019'], ['Kyle', '10', 'Accounts', '15-01-2019'], ['Kyle', '10', 'Accounts', '16-01-2019'], ['Kyle', '10', 'Accounts', '17-01-2019'], ['Kyle', '10', 'Accounts', '18-01-2019'], ['Kyle', '10', 'Accounts', '19-01-2019'], ['Kyle', '10', 'Accounts', '20-01-2019'], ['Kyle', '10', 'Accounts', '21-01-2019'], ['Kyle', '10', 'Accounts', '22-01-2019'], ['Kyle', '10', 'Accounts', '23-01-2019'], ['Kyle', '10', 'Accounts', '24-01-2019'], ['Kyle', '10', 'Accounts', '25-01-2019'], ['Kyle', '10', 'Accounts', '26-01-2019'], ['Kyle', '10', 'Accounts', '27-01-2019'], ['Kyle', '10', 'Accounts', '28-01-2019'], ['Kyle', '10', 'Accounts', '29-01-2019'], ['Kyle', '10', 'Accounts', '30-01-2019'], ['Kyle', '10', 'Accounts', '31-01-2019']], 'loc_data': [['9776235961', 'Miami']]})
('957149WC', {'dep_data': [['Kyle', '10', 'Accounts', '1-01-2019'], ['Kyle', '10', 'Accounts', '2-01-2019'], ['Kyle', '10', 'Accounts', '3-01-2019'], ['Kyle', '10', 'Accounts', '4-01-2019'], ['Kyle', '10', 'Accounts', '5-01-2019'], ['Kyle', '10', 'Accounts', '6-01-2019'], ['Kyle', '10', 'Accounts', '7-01-2019'], ['Kyle', '10', 'Accounts', '8-01-2019'], ['Kyle', '10', 'Accounts', '9-01-2019'], ['Kyle', '10', 'Accounts', '10-01-2019'], ['Kyle', '10', 'Accounts', '11-01-2019'], ['Kyle', '10', 'Accounts', '12-01-2019'], ['Kyle', '10', 'Accounts', '13-01-2019'], ['Kyle', '10', 'Accounts', '14-01-2019'], ['Kyle', '10', 'Accounts', '15-01-2019'], ['Kyle', '10', 'Accounts', '16-01-2019'], ['Kyle', '10', 'Accounts', '17-01-2019'], ['Kyle', '10', 'Accounts', '18-01-2019'], ['Kyle', '10', 'Accounts', '19-01-2019'], ['Kyle', '10', 'Accounts', '20-01-2019'], ['Kyle', '10', 'Accounts', '21-01-2019'], ['Kyle', '10', 'Accounts', '22-01-2019'], ['Kyle', '10', 'Accounts', '23-01-2019'], ['Kyle', '10', 'Accounts', '24-01-2019'], ['Kyle', '10', 'Accounts', '25-01-2019'], ['Kyle', '10', 'Accounts', '26-01-2019'], ['Kyle', '10', 'Accounts', '27-01-2019'], ['Kyle', '10', 'Accounts', '28-01-2019'], ['Kyle', '10', 'Accounts', '29-01-2019'], ['Kyle', '10', 'Accounts', '30-01-2019'], ['Kyle', '10', 'Accounts', '31-01-2019']], 'loc_data': [['9925595092', 'Houston']]})
('241316NX', {'dep_data': [['Kumiko', '10', 'Accounts', '1-01-2019'], ['Kumiko', '10', 'Accounts', '2-01-2019'], ['Kumiko', '10', 'Accounts', '3-01-2019'], ['Kumiko', '10', 'Accounts', '4-01-2019'], ['Kumiko', '10', 'Accounts', '5-01-2019'], ['Kumiko', '10', 'Accounts', '6-01-2019'], ['Kumiko', '10', 'Accounts', '7-01-2019'], ['Kumiko', '10', 'Accounts', '8-01-2019'], ['Kumiko', '10', 'Accounts', '9-01-2019'], ['Kumiko', '10', 'Accounts', '10-01-2019'], ['Kumiko', '10', 'Accounts', '11-01-2019'], ['Kumiko', '10', 'Accounts', '12-01-2019'], ['Kumiko', '10', 'Accounts', '13-01-2019'], ['Kumiko', '10', 'Accounts', '14-01-2019'], ['Kumiko', '10', 'Accounts', '15-01-2019'], ['Kumiko', '10', 'Accounts', '16-01-2019'], ['Kumiko', '10', 'Accounts', '17-01-2019'], ['Kumiko', '10', 'Accounts', '18-01-2019'], ['Kumiko', '10', 'Accounts', '19-01-2019'], ['Kumiko', '10', 'Accounts', '20-01-2019'], ['Kumiko', '10', 'Accounts', '21-01-2019'], ['Kumiko', '10', 'Accounts', '22-01-2019'], ['Kumiko', '10', 'Accounts', '23-01-2019'], ['Kumiko', '10', 'Accounts', '24-01-2019'], ['Kumiko', '10', 'Accounts', '25-01-2019'], ['Kumiko', '10', 'Accounts', '26-01-2019'], ['Kumiko', '10', 'Accounts', '27-01-2019'], ['Kumiko', '10', 'Accounts', '28-01-2019'], ['Kumiko', '10', 'Accounts', '29-01-2019'], ['Kumiko', '10', 'Accounts', '30-01-2019'], ['Kumiko', '10', 'Accounts', '31-01-2019']], 'loc_data': [['9837402343', 'Boston']]})
('796656IE', {'dep_data': [['Gaston', '10', 'Accounts', '1-01-2019'], ['Gaston', '10', 'Accounts', '2-01-2019'], ['Gaston', '10', 'Accounts', '3-01-2019'], ['Gaston', '10', 'Accounts', '4-01-2019'], ['Gaston', '10', 'Accounts', '5-01-2019'], ['Gaston', '10', 'Accounts', '6-01-2019'], ['Gaston', '10', 'Accounts', '7-01-2019'], ['Gaston', '10', 'Accounts', '8-01-2019'], ['Gaston', '10', 'Accounts', '9-01-2019'], ['Gaston', '10', 'Accounts', '10-01-2019'], ['Gaston', '10', 'Accounts', '11-01-2019'], ['Gaston', '10', 'Accounts', '12-01-2019'], ['Gaston', '10', 'Accounts', '13-01-2019'], ['Gaston', '10', 'Accounts', '14-01-2019'], ['Gaston', '10', 'Accounts', '15-01-2019'], ['Gaston', '10', 'Accounts', '16-01-2019'], ['Gaston', '10', 'Accounts', '17-01-2019'], ['Gaston', '10', 'Accounts', '18-01-2019'], ['Gaston', '10', 'Accounts', '19-01-2019'], ['Gaston', '10', 'Accounts', '20-01-2019'], ['Gaston', '10', 'Accounts', '21-01-2019'], ['Gaston', '10', 'Accounts', '22-01-2019'], ['Gaston', '10', 'Accounts', '23-01-2019'], ['Gaston', '10', 'Accounts', '24-01-2019'], ['Gaston', '10', 'Accounts', '25-01-2019'], ['Gaston', '10', 'Accounts', '26-01-2019'], ['Gaston', '10', 'Accounts', '27-01-2019'], ['Gaston', '10', 'Accounts', '28-01-2019'], ['Gaston', '10', 'Accounts', '29-01-2019'], ['Gaston', '10', 'Accounts', '30-01-2019'], ['Gaston', '10', 'Accounts', '31-01-2019']], 'loc_data': [['9538848876', 'Houston']]})
('718737IX', {'dep_data': [['Ayumi', '10', 'Accounts', '2-01-2019'], ['Ayumi', '10', 'Accounts', '3-01-2019'], ['Ayumi', '10', 'Accounts', '4-01-2019'], ['Ayumi', '10', 'Accounts', '5-01-2019'], ['Ayumi', '10', 'Accounts', '6-01-2019'], ['Ayumi', '10', 'Accounts', '7-01-2019'], ['Ayumi', '10', 'Accounts', '8-01-2019'], ['Ayumi', '10', 'Accounts', '9-01-2019'], ['Ayumi', '10', 'Accounts', '10-01-2019'], ['Ayumi', '10', 'Accounts', '11-01-2019'], ['Ayumi', '10', 'Accounts', '12-01-2019'], ['Ayumi', '10', 'Accounts', '13-01-2019'], ['Ayumi', '10', 'Accounts', '14-01-2019'], ['Ayumi', '10', 'Accounts', '15-01-2019'], ['Ayumi', '10', 'Accounts', '16-01-2019'], ['Ayumi', '10', 'Accounts', '17-01-2019'], ['Ayumi', '10', 'Accounts', '18-01-2019'], ['Ayumi', '10', 'Accounts', '19-01-2019'], ['Ayumi', '10', 'Accounts', '20-01-2019'], ['Ayumi', '10', 'Accounts', '21-01-2019'], ['Ayumi', '10', 'Accounts', '22-01-2019'], ['Ayumi', '10', 'Accounts', '23-01-2019'], ['Ayumi', '10', 'Accounts', '24-01-2019'], ['Ayumi', '10', 'Accounts', '25-01-2019'], ['Ayumi', '10', 'Accounts', '26-01-2019'], ['Ayumi', '10', 'Accounts', '27-01-2019'], ['Ayumi', '10', 'Accounts', '28-01-2019'], ['Ayumi', '10', 'Accounts', '29-01-2019'], ['Ayumi', '10', 'Accounts', '30-01-2019'], ['Ayumi', '10', 'Accounts', '31-01-2019']], 'loc_data': [['9204232788', 'Austin']]})
('331593PS', {'dep_data': [['Beryl', '20', 'HR', '1-01-2019'], ['Beryl', '20', 'HR', '2-01-2019'], ['Beryl', '20', 'HR', '3-01-2019'], ['Beryl', '20', 'HR', '4-01-2019'], ['Beryl', '20', 'HR', '5-01-2019'], ['Beryl', '20', 'HR', '6-01-2019'], ['Beryl', '20', 'HR', '7-01-2019'], ['Beryl', '20', 'HR', '8-01-2019'], ['Beryl', '20', 'HR', '9-01-2019'], ['Beryl', '20', 'HR', '10-01-2019'], ['Beryl', '20', 'HR', '11-01-2019'], ['Beryl', '20', 'HR', '12-01-2019'], ['Beryl', '20', 'HR', '13-01-2019'], ['Beryl', '20', 'HR', '14-01-2019'], ['Beryl', '20', 'HR', '15-01-2019'], ['Beryl', '20', 'HR', '16-01-2019'], ['Beryl', '20', 'HR', '17-01-2019'], ['Beryl', '20', 'HR', '18-01-2019'], ['Beryl', '20', 'HR', '19-01-2019'], ['Beryl', '20', 'HR', '20-01-2019'], ['Beryl', '20', 'HR', '21-01-2019'], ['Beryl', '20', 'HR', '22-01-2019'], ['Beryl', '20', 'HR', '23-01-2019'], ['Beryl', '20', 'HR', '24-01-2019'], ['Beryl', '20', 'HR', '25-01-2019'], ['Beryl', '20', 'HR', '26-01-2019'], ['Beryl', '20', 'HR', '27-01-2019'], ['Beryl', '20', 'HR', '28-01-2019'], ['Beryl', '20', 'HR', '29-01-2019'], ['Beryl', '20', 'HR', '30-01-2019'], ['Beryl', '20', 'HR', '31-01-2019']], 'loc_data': [['9137216186', 'Miami']]})
```

---

## Side Inputs

- Additional data provided to a DoFn object
- Can be provided to Pardo or its child Transforms (Map, flatmap)
    - pardo will take side input as an extra input which it can access each time it processes an element in the input PCollection
- the side input data needs to be determined at **runtime**

```python
# dept_data.txt

149633CM,Marco,10,Accounts,1-01-2019
212539MU,Rebekah,10,Accounts,1-01-2019
231555ZZ,Itoe,10,Accounts,1-01-2019
503996WI,Edouard,10,Accounts,1-01-2019
704275DC,Kyle,10,Accounts,1-01-2019
957149WC,Kyle,10,Accounts,1-01-2019
241316NX,Kumiko,10,Accounts,1-01-2019
796656IE,Gaston,10,Accounts,1-01-2019
331593PS,Beryl,20,HR,1-01-2019

# 제외하고 싶은 직원 목록 exclude_ids

149633CM
212539MU
231555ZZ
704275DC

 추가조건 : Do the attendance count with below conditions

- Accounts dept employees excluding few
- Also the employee name's length be between 3-10
```

### ParDo Transform

- which will perform the functioning defined in this class
    - As side input, it takes the cut off length
    - character `A`
    - with_outputs
        - we have to provide the tags of different outputs
        - 'Short_Names', 'Long_Names', main="Names_A"
        - Short_names tag for less than 4 characters name
        - Long_Names tag for more than 4 characters
        - these 2 tags are the additional outpus and for main output we have to specify like this
        - `main = Names_A`
    
    ```python
     beam.ParDo(ProcessWords(), cutoff_length=4, marker='A').with_outputs('Short_Names', 'Long_Names', main="Names_A")
    ```
    
    ```python
    import apache_beam as beam
    
    # DoFn function
    class ProcessWords(beam.DoFn):
        
        def process(self, element, cutoff_length, marker):
    
            name = element.split(',')[1] # employee name
    
            if len(name) <= cutoff_length:
                return [beam.pvalue.TaggedOutput('Short_Names', name)]
            
            else:
                return [beam.pvalue.TaggedOutput('Long_Names', name)]
            
            if name.startswith(marker):
                return name
    
    p = beam.Pipeline()
    
    results = (
        p
        | beam.io.ReadFromText('dept_data.txt')
        | beam.ParDo(ProcessWords(), cutoff_length=4, marker='A').with_outputs('Short_Names', 'Long_Names', main="Names_A")
    )
    
    short_collection = results.Short_Names
    long_collection = results.Long_Names
    startA_collection = results.Names_A
    
    # 파일 생성
    short_collection | 'Write 1' >> beam.io.WriteToText('short')
    long_collection | 'Write 2' >> beam.io.WriteToText('long')
    startA_collection | 'Write 3' >> beam.io.WriteToText('start_a')
    
    p.run()
    
    !{'head -n 5 short-00000-of-00001'}
    !{'head -n 5 long-00000-of-00001'}
    !{'head -n 5 start_a-00000-of-00001'} # We don't have any Name which starts with `A`
    
    Itoe
    Kyle
    Kyle
    Olga
    Kirk
    Marco
    Rebekah
    Edouard
    Kumiko
    Gaston
    ```
    

- Which of the following pair of PTransforms can emit multiple outputs per input?
    - ParDo & Map
    - ParDo & FlatMap
    - Map & FlatMap
- which of the follwing is NOT a parameter of ReadFromPubSub
    - id_label
    - topic
    - subscription
    - compression_type
- which of the follwing is NOT a parameter of WriteToText?
    - num_shards
    - schema
    - header
    - coder
- which of the following is correct method to implement additional outputs?
    - with_outpus
- To create composite PTransform which class is must to inherit
    - DoFn
    - CombineFn
    - PTranform

---
