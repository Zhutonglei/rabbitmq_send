# -*- coding: utf-8 -*-

from rabbitmq import emit_mq
import sys
import time
import datetime
import configparser
import threading
import pymysql as mysqldb

cf = configparser.ConfigParser()
cf.read("./config.ini")
# exchange = cf.get("send", "exchange1")
# exchange1 = cf.get("send", "exchange2")
# exchange2 = cf.get("send", "exchange3")
# exchange3 = cf.get("send", "exchange4")
# exchange4 = cf.get("send", "exchange5")
# exchange5 = cf.get("send", "exchange6")
routecount = cf.get("send", "routecount")
routecount = int(routecount)
durbale = cf.get("send", "durable")
exchangecount=cf.get("send","exchangecount")
exchangecount= int (exchangecount)
# flag1=cf.get("send", "flag1")
# flag2=cf.get("send", "flag2")
# flag3=cf.get("send", "flag3")
# flag4=cf.get("send", "flag4")
# flag5=cf.get("send", "flag5")
# flag6=cf.get("send", "flag6")
bytelength = cf.get("send", "zfill")
bytelength = int(bytelength)
host = cf.get("send","host")

if durbale=='True':
    durbale=True
else:
    durbale=False

emit_mq_instance = emit_mq.rabbitmq(username='zntx',password='1qaz@WSX',host='192.168.21.136',port=5672)
routing_key_list =[]
threads=[]

for i in range(0,routecount):
    routing_key_list.append('routing_'+str(i))

def sendMsg(Exchange,Durable,routingkeylist):
    def on_message():
        config = {
            'host': host,
            'port': 3306,
            'user': 'root',
            'passwd': '1qaz@WSX',
            'db': 'rabbitmq',
            'charset': 'utf8'
        }
        # 写入数据库
        conn = mysqldb.connect(**config)
        cursor = conn.cursor()
        conn.autocommit(1)
        return cursor
    cursor = on_message()
    emit_mq_instance.__sendmessage__(exchange=Exchange,exchange_type='topic',durable=Durable,routing_key=routingkeylist,cursor=cursor,bytelength=bytelength)
    cursor.close()

for i  in range(0,exchangecount):
    t = threading.Thread(target=sendMsg,args=("exchange_{a}".format(a=str(i)),durbale,routing_key_list))
    threads.append(t)
for k in range(0,len(threads)):
    threads[k].start()
#
for  k in range(0, len(threads)):
    threads[k].join()
