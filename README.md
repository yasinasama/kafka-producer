# kafka-producer
A simple kafka producer
You can produce test data to kafka topic via manual or from excel/csv.

## Usage
````
git clone https://github.com/yasinasama/kafka-producer.git
or
wget https://github.com/yasinasama/kafka-producer/archive/master.zip
unzip master.zip

(manual)python kafka-producer.py -c config.json -m
(not manual)python kafka-producer.py -c config.json
````

## Config.json
````
{
  "bootstrap_servers":"127.0.0.1:9092",
  "topic":"test",
  "interval":1100,
  "source_file":"./test_excel.xls",
  "source_type":"excel",
  "source_key":0
}
````




