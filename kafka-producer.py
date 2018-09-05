# -*- coding: utf-8 -*-

import time
import os
import json
import sys
import getopt

import pandas as pd
from kafka import KafkaProducer

def check_file(filepath):
    if not os.path.exists(filepath):
        print('%s not exists'%filepath)
        sys.exit()

def read_csv(filepath):
    csv = pd.read_csv(filepath,dtype=object)
    csv = csv.fillna('')
    data = csv.to_dict('records')
    return data

def read_excel(filepath):
    csv = pd.read_excel(filepath,dtype=object)
    csv = csv.fillna('')
    data = csv.to_dict('records')
    return data

def print_help():
    print('''usage: kafka-producer [OPTION]...
    A kafka producer.

    You can produce test data via manual or from excel/csv.

    General options:
      -h, --help             show this help message and exit
      -c, --config           your config file path
      -m, --manual           produce test data via manual
                             input format:
                                  key:value
                                demo:               
                                  hangzhou:i love python
                                demo(if not key)
                                  :i love python (do not forget `:`)
      
    Config options:
          bootstrap_servers     kafka bootstrap-server
          topic                 where should your test data go
        If not manual(produce data from excel or csv)
          interval(ms)          interval between twice sends
          source_file           source file path
          source_type           source type(excel or csv)
          source_key            if the data include a key
                                if has key  set source_key to 1 and you must have one column which named `key`
                                if not key  set source_key to 0
      
    Online help: <https://github.com/yasinasama/kafka-producer>
    ''')

class Producer:
    def __init__(self,bootstrap_servers):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def send(self,topic,key=None,value=None):
        try:
            key = key.encode() if key else None
            value = value.encode() if value else None
            self.producer.send(topic, key=key, value=value)
        except Exception:
            raise
        finally:
            self.producer.flush()

    def close(self):
        self.producer.close()

def main():
    manual = False
    try:
        opts,args = getopt.getopt(sys.argv[1:],'hc:m',['help','config=','manual'])
    except getopt.GetoptError:
        print('usage: python kafka-producer.py -c <config>')
        sys.exit()
    for optkey,optvalue in opts:
        if optkey == '-h':
            print_help()
            sys.exit()
        if optkey == '-c':
            fpath = optvalue
        if optkey == '-m':
            manual = True
    check_file(fpath)
    try:
        with open(fpath,'r') as f:
            config = json.loads(f.read())
    except ValueError as e:
        print('found an error in config.json: %s'%str(e))
        sys.exit()

    try:
        bootstrap_servers = config['bootstrap_servers']
        topic = config['topic']
        if not manual:
            interval = config.get('interval', 0)
            source_file = config['source_file']
            source_type = config['source_type']
            source_key = config.get('source_key',0)
    except KeyError as e:
        print('%s not found' % str(e))
        sys.exit()

    producer = Producer(bootstrap_servers=bootstrap_servers)
    if manual:
        try:
            while True:
                _input = input('Please input data >> ')
                key,value = _input.split(':',1)
                if key == '':
                    key = None
                producer.send(topic,key,value)
        except KeyboardInterrupt:
            print('bye')
            sys.exit()
        except ValueError:
            print('maybe you forget `:` check it')
            sys.exit()
        finally:
            producer.close()
    else:
        try:
            check_file(source_file)
            if source_type=='csv':
                data = read_csv(source_file)
            elif source_type=='excel':
                data = read_excel(source_file)
            else:
                print('source_type: %s not supported'%source_type)
                sys.exit()
            for row in data:
                if source_key==0:
                    key = None
                elif source_key==1:
                    key = row.pop('key')
                    key = None if key == '' else key
                else:
                    print('source_key must be 0 or 1')
                    sys.exit()
                producer.send(topic, key, json.dumps(row))
                time.sleep(int(interval)/1000)
        except:
            raise
        finally:
            producer.close()


if __name__=='__main__':
    main()






