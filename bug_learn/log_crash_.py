# -*- coding:utf-8 -*-
"""
@author: gxjun
@file: log_crash_.py
@time: 18-5-20 下午9:42
"""
# -*- coding:utf-8 -*-
"""
@author: gxjun
@file: statistics.py
@time: 18-5-14 下午10:20
"""
from pymongo import *
from bson import json_util
import json
import pandas as pd
import re
import difflib
import numpy as np


def clear_data(line):
    line = line.strip()
    line = re.sub("[\s+\.\!\;\:\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf8"), " ".decode("utf8"),
                  line.decode("utf-8"))
    line = ' '.join(line.split())
    return line.strip()


def diffs(sentence, collections):
    for sents in collections:
        if difflib.SequenceMatcher(None, sentence, sents).ratio() > 0.85:
            return sents
    return None


#customer_df = pd.read_csv('customer_new.csv')
#mydict = dict(zip(customer_df['customer'], customer_df['parent company']))
#for text in mydict:
#    if mydict[text] is np.nan:
#        mydict[text] = text


def clear_data(line):
    line = line.encode('utf-8').strip()
    line = re.sub("[\s+\.\!\;\:\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+", " ",
                  line.decode("utf-8"))
    line = ' '.join(line.split())
    return line.encode('utf-8').strip()


def dict_count(dict_s, title, title_list):
    flag = False
    for _ti in title_list:
        dict_s[_ti] = dict_s.get(_ti, 0)
        if re.search(_ti, title):
            dict_s[_ti] += 1
            flag = True
    return flag


def parse_from_Mongo():
    # create contion
    client = MongoClient("10.65.9.99", 27017)
    # get db
    db = client["bugzilla"]
    # get table
    table = db['bug_table']
    # get new csv
    directory = list()
    title_list = {'crash', 'kernel', 'hung', 'watchdog'}
    coment_list = {'lr is', 'call_trace', 'kernel panic'}
    dict_tilte = {}
    cursor = table.find()
    cnt = 5
    for doc in cursor:
        comp = doc['component'].lower()
        comp = clear_data(comp)
        flag = 0
        cnt += 1
        if comp in ['ap-platform', 'ap platform', 'applatform']:
            comp = comp + '#' + doc['platform'].lower()

            title = doc['summary'].lower()
            dict_tilte[comp] = dict_tilte.get(comp, {})
            flag = dict_count(dict_tilte[comp], title=title, title_list=title_list)
            coments = doc['comments']
            for coment in coments:
                new_coment = coment['text'].lower()
                flag = dict_count(dict_tilte[comp], title=new_coment, title_list=coment_list)
            dict_tilte[comp]['comp'] = comp
            print(cnt)

    for dict_ti in dict_tilte:
        directory.append(dict_tilte[dict_ti])
    df = pd.DataFrame(directory)
    df.to_csv('log_crash.csv', index=False)


    # with  open('log_crash.json', 'wb') as fileObject:
    #     json.dump(customer_comps, fileObject)


def load_from_file(file_name, tag):
    context = list()
    for line in open(file_name, mode='rb'):
        line = line.strip()
        line = clear_data(line)
        context.append(line)

    context.sort()
    cont_dict = [{tag: cont} for cont in context]
    df = pd.DataFrame(cont_dict)
    df.to_csv(tag + '.csv', index=False)


def loads_from_file(file_name, tag):
    context = list()
    with  open('jsonFile.json', 'rb') as fileObject:
        customer_comps = json.load(fileObject)

    for line in customer_comps:
        arr = line.split('##')
        customer = clear_data(arr[0])
        component = clear_data(arr[1])
        obj = customer_comps[line]
        context.append(
            {'customer': customer, 'component': component, 'bug_num': obj['bug_num'], 'bugids': obj['bugIds']})

    df = pd.DataFrame(context)
    df.to_csv(tag + '.csv', index=False)


parse_from_Mongo()
# loads_from_file('/home/gxjun/PycharmProjects/aruba/utils/jsonFile.json', 'customer_comps')
# load_from_file('/home/gxjun/PycharmProjects/aruba/utils/comp.txt', 'component')
# load_from_file('/home/gxjun/PycharmProjects/aruba/utils/customer.txt', 'customer')
