from pymongo import MongoClient
import re
from textblob import TextBlob
from nltk.stem import SnowballStemmer,WordNetLemmatizer
import pandas as pd
from nltk.corpus import stopwords
class BugLearn():
    def __init__(self,mongodb_host=None):
        if mongodb_host is None:
            self.mongodb_host = '10.65.9.99'
        else:
            self.mongodb_host = mongodb_host
        self.mongo_conn = MongoClient(self.mongodb_host, 27017)
        self.db = self.mongo_conn.bugzilla
        self.bug_table = self.db.bug_table3
        self.bugs = self.bug_table.find({'id': {'$gt': 100000}})
        self.ap_types = pd.read_csv('ap_type.csv', header=0, names=['name', 'type'], index_col=1)['name']
        self.ap_type_list =self.ap_types.index.values.tolist()
        self.clean_str1=re.compile('ap-')
        self.clean_str2=re.compile('[^.\w]')
        self.clean_str3=re.compile('\s+')
        self.stem = SnowballStemmer('english')
        self.lemma = WordNetLemmatizer()
        self.summary_word_list=[]
        self.summarys = {}

    def parse_from_Mongo(self):
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

    def get_summary(self):
        lengs=self.bugs.count()
        i=0
        percent=0
        for bug in self.bugs:
            self.summarys[bug['id']] = self.clean_format(bug['summary'])
            #self.summarys[bug['id']] = bug['summary']
            self.summary_word_list += self.summarys[bug['id']]

            i+=1
            if int(i*10000/lengs)>percent:
                print('finished %d'%(percent))
                percent+=1
    def get_description(self,id_list):
        description={}
        for bug in self.bugs:
            if bug['id'] in id_list:
                description[bug['id']]= self.clean_format(bug['comments'][0]['text'])
        return description     
    def get_ap_related_bugs(self):
        self.ap_alias_name_list=[]
        ap_relate_bug={}
        for ap_alias in self.ap_types.values.tolist():
            self.ap_alias_name_list.append(self.stem.stem(self.lemma.lemmatize(self.lemma.lemmatize(ap_alias, pos='v'))))
        self.ap_alias_name_list=set(self.ap_alias_name_list)
        for id in self.summarys.keys():
            values= self.summarys[id]
            try:
                diff_set=set(values[:3]) & self.ap_alias_name_list
                if len(diff_set):
                    ap_relate_bug[id]=diff_set.pop()

            except:
                pass
        ap_relate_s=pd.Series(ap_relate_bug)
        ap_summary_s=pd.Series(self.summarys,index=ap_relate_s.index)
        self.ap_info=pd.DataFrame([ap_relate_s,ap_summary_s],index=['ap_type','summary']).T

    def generate_labled_data(self):
        pass
    def clean_format(self,string):
        word_list = []
        for word in TextBlob(self.clean_str1.sub('ap',string.lower())).words:
            if word in stopwords.words('english') or len(word) < 1:
                continue
            word=self.stem.stem(self.lemma.lemmatize(self.lemma.lemmatize(word,pos='v')))
            if word in self.ap_type_list:
                word = self.ap_types[word]
            word = self.stem.stem(self.lemma.lemmatize(self.lemma.lemmatize(word, pos='v')))
            word_list.append(word)
        return word_list
