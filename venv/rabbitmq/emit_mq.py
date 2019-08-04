# -*- coding: utf-8 -*-

import pika
import time
import configparser
import pymysql as mysqldb
import logging
import datetime
import threading

class Logger():
    def __init__(self, logname, loglevel, logger):
        '''
           指定保存日志的文件路径，日志级别，以及调用文件
           将日志存入到指定的文件中
        '''
        # 用字典保存日志级别
        format_dict =\
        {
            1: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            2: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            3: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            4: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            5: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        }
        # 创建一个logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)


        # 创建一个handler，用于写入日志文件
        fh = logging.FileHandler(logname)
        fh.setLevel(logging.DEBUG)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # 定义handler的输出格式
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = format_dict[int(loglevel)]
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # 给logger添加handler
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
    def getlog(self):
        return self.logger
class rabbitmq():
    def __init__(self,username,password,host,port,virtual_host='/'):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.virtual_host =virtual_host

    # logger = Logger(
    #     logname='log_' + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d') + '.txt',
    #     loglevel=1,
    #     logger='Alarm history').getlog()
    #logger =Logger(logname='log_' + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d') + '.txt',loglevel=1,logger='rabbitmq_send').getlog()

    def __sendmessage__(self,exchange,exchange_type,durable,cursor,bytelength,routing_key=[]):
        cf = configparser.ConfigParser()
        cf.read("./config.ini")
        interval = cf.get("send", "interval")
        credentials = pika.PlainCredentials(self.username, self.password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port, virtual_host=self.virtual_host,credentials=credentials))
        i=0
        while True:
            i=i+1
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host, port=self.port, virtual_host=self.virtual_host,
                                          credentials=credentials))
            for item in routing_key:
                time.sleep(float(interval))
                timestamp = time.time()
                datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
                message = (
                    ":count={a},timestamp={b},datetime={c},exchange={d},exchange_type={e},routing_key={f},durable={g}".
                        format(a=i, b=timestamp, c=datetime, d=exchange, e=exchange_type, f=item, g=durable)).zfill(bytelength)
                try:
                    channel = connection.channel()
                    channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=durable)
                    channel.basic_publish(exchange=exchange, routing_key=item, body=message)
                    excute_sql = "INSERT INTO rabbitmq_sender VALUES('%(exchange)s','%(exchange_type)s','%(timestamp)s','%(datetime)s','%(routing_key)s','%(count)s','%(durable)s')" % {
                        "exchange": exchange, "exchange_type": exchange_type, "timestamp": timestamp, "datetime": datetime,"routing_key": item, "count": i,"durable":durable}
                    cursor.execute(excute_sql)
                    print excute_sql
                    print(" [x] Sent %r:%r" % (item, message))
                except Exception as e:
                    logger =Logger(logname='log_' + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d') + '.txt',loglevel=1,logger='rabbitmq_send').getlog()
                    logger.error('Error:{a}'.format(a=e))
            connection.close()





    #timer = threading.Timer(60, fun_timer)  # 60秒调用一次函数
    # 　　  # 定时器构造函数主要有2个参数，第一个参数为时间，第二个参数为函数名
    #
    # 　　timer.start()  # 启用定时器
    #
    # 　　
    #
    # timer = threading.Timer(1, fun_timer)  # 首次启动
    # timer.start()












