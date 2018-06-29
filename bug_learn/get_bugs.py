import bugzilla
from pymongo import MongoClient
import logging
import os
import requests
import urllib
import math
import copy
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import urllib
import ssl
import re
import threading
from datetime import datetime
class GetBug():
    def __init__(self, bugzilla_url=None,mongodb_host=None):
        super(GetBug, self).__init__()
        if bugzilla_url is None:
            self.bugzilla_url='bugzilla.arubanetworks.com'
        else:
            self.bugzilla_url = bugzilla_url
        if mongodb_host is None:
            self.mongodb_host='10.65.9.99'
        else:
            self.mongodb_host=mongodb_host
        self.mongo_conn=MongoClient(self.mongodb_host,27017)
        self.db=self.mongo_conn.bugzilla
        self.bug_table=self.db.bug_table3
        self.bzapi = bugzilla.Bugzilla(self.bugzilla_url)
        self.user_table=self.db.user_table
        self.logger = logging.getLogger("simple_example")
        self.logger.setLevel(logging.INFO)
        current=datetime.now().strftime("%d%m%y-%H%M%S")
        fh = logging.FileHandler('get_bug'+current+'.log')
        fh.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)

    def get_latest_bug_id(self):
        pass
    def insert_bug(self,bug_dict):
        pass
    def get_user_name_by_mail(self,user):
        result=self.user_table.find_one({'email':user})
        if result:
            return result['real_name']
        else:
            print('query user %s'%(user))
            try:
                user_info=self.bzapi.getuser(user)
            except:
                user_info=None
            if user_info is not None:
                user_dict={
                    'email':user_info.email,
                    'real_name':user_info.real_name,
                    'user_id':user_info.userid
                }
                self.user_table.insert_one(user_dict)
                return user_info.real_name
            else:
                return None
    def get_user_name_by_mail1(self,user):
        result=self.user_table.find_one({'email':user+'@arubanetworks.com'})
        if result:
            return result['real_name']
        else:
            print('query user %s'%(user))
            try:
                user_info=self.bzapi.getuser(user)
            except:
                user_info=None
            if user_info is not None:
                user_dict={
                    'email':user_info.email,
                    'real_name':user_info.real_name,
                    'user_id':user_info.userid
                }
                self.user_table.insert_one(user_dict)
                return user_info.real_name
            else:
                return None
    def get_user_id_by_name(self,name):
        result=self.user_table.find_one({'real_name':name})
        if result:
            return result['user_id']
        else:
            return 1


    def get_bug_from_bugzilla(self,bug_id):
        try:
            bug_info = self.bzapi.getbug(bug_id)
        except:
            bug_info=None
        if bug_info is None:
            return None
        bug_dict={'id':bug_id,
                  'component':bug_info.component,
                  'product':bug_info.product,
                  'report_id':bug_info.reporter_id,
                  'version':bug_info.version,
                  'platform':bug_info.platform,
                  'priority':bug_info.priority,
                  'severity':bug_info.severity,
                  'target_milestone':bug_info.target_milestone,
                  'creation_time':bug_info.creation_time,
                  'status':bug_info.status['value'],
                  'resolution':bug_info.resolution,
                  'summary':bug_info.summary,

                  'assign_to':bug_info.assigned_to
            }
        try:
            history_raw=bug_info.get_history_raw()['bugs'][0]['history']
            for  his in history_raw:
                his['when']=his['when'].value
                his['who']=self.get_user_name_by_mail(his['who'])

        except:
            history_raw=self.get_raw_history(bug_id)
        error_fount = False
        try:
            comments=bug_info.getcomments()
        except Exception as e:
            error_fount=True
        if error_fount:
            comments = self.get_comments(bug_id)
        for comment in comments:
            if type(comment['time'])!=str:
                comment['time']=comment['time'].value
            if re.search('@',comment['author']):
                comment['author']=self.get_user_name_by_mail(comment['author'])
        bug_dict['comments']=comments
        bug_dict['history_raw']=history_raw
        if hasattr(bug_info,'cf_customers'):
            if isinstance(bug_info.cf_customers,str):
                bug_dict['customers']=bug_info.cf_customers
            elif isinstance(bug_info.cf_customers,int):
                bug_dict['customers'] = str(bug_info.cf_customers)
            else:
                bug_dict['customers'] = bug_info.cf_customers.data.decode('utf8')
        else:
            bug_dict['customers'] = ''
        if hasattr(bug_info, 'cf_tac_tickets'):
            bug_dict['tac_tickets'] = bug_info.cf_tac_tickets
        else:
            bug_dict['tac_tickets'] = ''
        if hasattr(bug_info, 'cf_tac_tickets'):
            bug_dict['tac_tickets'] = bug_info.cf_tac_tickets
        else:
            bug_dict['tac_tickets'] = ''
        if hasattr(bug_info, 'cf_fixed_versions'):
            bug_dict['fixed_versions'] = bug_info.cf_fixed_versions
        else:
            bug_dict['fixed_versions'] = ''
        if hasattr(bug_info, 'qa_contact'):
            bug_dict['qa_contract_id'] = bug_info.qa_contact
        else:
            bug_dict['qa_contract_id'] = 858
        return bug_dict
    def get_bug_by_http(self,bug_id):
        ssl._create_default_https_context = ssl._create_unverified_context
        url = 'https://'+self.bugzilla_url+'/show_bug.cgi?id='+str(bug_id)
        content = BeautifulSoup(requests.get(url).text, 'lxml')
        qa_contract = self.get_user_id_by_name(content.find(attrs={'for': 'qa_contact'}).parent.parent.find_all(attrs={'class': 'vcard'})[0].text.strip())
        component= content.find(attrs={'for': 'component'}).parent.parent.find_all('td')[1].text.strip()
        product = content.find(attrs={'id':'field_container_product'}).text.strip()
        report_id=self.get_user_id_by_name(content.find(self.get_reporter,attrs={'class':'fn'}).text.strip())
        assign_to = self.get_user_id_by_name(content.find(self.get_assign, attrs={'class': 'fn'}).text.strip())
        version=content.find(attrs={'for': 'version'}).parent.parent.find_all('td')[1].text.strip()
        platform=content.find(attrs={'for': 'rep_platform'}).parent.parent.find_all('td')[1].text.strip().split()
        importance=content.find(attrs={'for': 'priority'}).parent.parent.find_all('td')[1].text.split()
        milestone = content.find(attrs={'for': 'target_milestone'}).parent.parent.find_all('td')[1].text.strip()
        status=content.find(attrs={'id':'static_bug_status'}).text.strip().split()
        summary=content.find(attrs={'id':'short_desc_nonedit_display'}).text.strip()
        customers=content.find(attrs={'id':'field_container_cf_customers'}).text.strip()
        fixed_versions = content.find(attrs={'id': 'field_container_cf_fixed_versions'}).text.strip()
        tac_tickets = content.find(attrs={'id': 'field_container_cf_fixed_versions'}).text.strip()
        creation_time=re.sub('([\d-]+\s+[\d:]+)(.*)', '\g<1>', content.find(attrs={'class': 'bz_first_comment_head'}).find(attrs={'class': 'bz_comment_time'}).text.strip())
        if len(status)==1:
            status.append('None')
        bug_dict={'id':bug_id,
                  'component':component,
                  'product':product,
                  'report_id':report_id,
                  'version':version,
                  'platform':platform[0],
                  'priority':importance[0],
                  'severity':importance[1],
                  'target_milestone':milestone,
                  'creation_time':datetime.strptime(creation_time,  '%Y-%m-%d %H:%M:%S').strftime('%Y%m%dT%H:%M:%S'),
                  'status':status[0],
                  'resolution':status[1],
                  'summary':summary,
                  'assign_to':assign_to,
                  'qa_contract_id':qa_contract,
                  'customers':customers,
                  'fixed_versions':fixed_versions,
                  'tac_tickets':tac_tickets
            }

        history_raw = self.get_raw_history(bug_id)
        comments = self.get_comments(bug_id)
        for comment in comments:
            if type(comment['time']) != str:
                comment['time'] = comment['time'].value
            if re.search('@', comment['author']):
                comment['author'] = self.get_user_name_by_mail(comment['author'])
        bug_dict['comments'] = comments
        bug_dict['history_raw'] = history_raw
        return bug_dict
    def get_comments(self,bug_id):
        ssl._create_default_https_context = ssl._create_unverified_context
        url = 'https://'+self.bugzilla_url+'/show_bug.cgi?id='+str(bug_id)
        comments_list = BeautifulSoup(requests.get(url).text,'lxml').find_all(class_='bz_comment')
        new_comments_list=[]
        for comment in comments_list:
            times=re.sub('([\d-]+\s+[\d:]+)(.*)', '\g<1>', comment.find_all(class_='bz_comment_time')[0].text.strip())
            new_comments_list.append({
                'author':comment.find_all(class_='fn')[0].text.strip(),
                'time':datetime.strptime(times,  '%Y-%m-%d %H:%M:%S').strftime('%Y%m%dT%H:%M:%S'),
                'text':comment.find_all(class_='bz_comment_text')[0].text.strip(),
                'id':bug_id,
                'bug_id': bug_id,
                'is_private':'false'
            })
        return new_comments_list
    def get_user_name(self,bug_id):
        ssl._create_default_https_context = ssl._create_unverified_context
        url = 'https://' + self.bugzilla_url + '/show_bug.cgi?id=' + str(bug_id)
        content=BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml')
        qa_contract=content.find(attrs={'for': 'qa_contact'}).parent.parent.find_all(attrs={'class': 'vcard'})[0].text
        assign_to=content.find(get_assign,attrs={'class':'fn'}).text
        reported=content.find(get_reporter,attrs={'class':'fn'}).text
        return {'qa_contract':qa_contract,'assign_to':assign_to,'reported':reported}

    def get_reporter(self,tag):
        try:
            result = bool(re.search('Reported', tag.parent.parent.parent.text))
        except:
            result = False
        return result

    def get_assign(self,tag):
        try:
            result = bool(re.search('Assigned To', tag.parent.parent.parent.text))
        except:
            result = False
        return result
    def get_raw_history(self,bug_id):
        url = 'https://' + self.bugzilla_url+'/show_activity.cgi?id='+str(bug_id)
        hist_tables = html_tables(url).read()[1].drop(0)
        hist_tables.columns=['who','when','field_name','removed','added']
        hist_tables=hist_tables.fillna(method='ffill').set_index(['who','when'])
        row_history=[]
        for index in hist_tables.index.drop_duplicates().values:
            stable=hist_tables.xs(index)
            from pandas import Series
            if isinstance(stable,Series):
                stable=stable.to_frame().T
            changes=[]
            for i in range(0,stable.shape[0]):

                changes.append(stable.iloc[i].to_dict())

                times = re.sub('([\d-]+\s+[\d:]+)(.*)', '\g<1>',
                               index[1].strip())
                whens=datetime.strptime(times, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%dT%H:%M:%S')
                whos=self.get_user_name_by_mail1(index[0])
            row_history.append({'who':whos,'when':whens,'changes':changes})
        return row_history

    def set_bug_to_db(self,start_id=1,end_id=200000,idlist=None):
        continues_wrong_count = 0
        fail_dict={}
        if idlist is None:
            idlist= range(start_id,end_id)
        for bug_id in idlist:
            if continues_wrong_count==1000:
                break
            try:
                self.logger.info('id %s begin to search from bugzilla reset' % (bug_id))
                bug_dict=self.get_bug_from_bugzilla(bug_id)
                #bug_dict = self.get_bug_by_http(bug_id)
            except Exception as e:
                bug_dict = None
                self.logger.info('id %s search 1 faild, the faild reason is %s' % (bug_id, e))
                fail_dict[str(bug_id)] = e
            if bug_dict is None:
                try:
                    self.logger.info('id %s begin to search from bugzilla http' % (bug_id))
                    bug_dict = self.get_bug_by_http(bug_id)
                except Exception as e:
                    bug_dict = None
                    self.logger.info('id %s search 2 faild, the faild reason is %s' % (bug_id, e))
                    fail_dict[str(bug_id)] = e
                if bug_dict is None:
                    self.logger.info('id %s bugzilla is none failed' % (bug_id))
                    continues_wrong_count = continues_wrong_count + 1
                    continue
            continues_wrong_count=0
            try:
                self.logger.info('id %s begin to insert to db' % (bug_id))
                self.bug_table.insert_one(bug_dict)
                self.logger.info('id %d insert success'%(bug_id))
            except Exception as e:
                fail_dict[str(bug_id)]=e
                self.logger.info('id %s insert faild, the failed reason is %s' % (bug_id,e ))
        for failed_bug in fail_dict.keys():
            print('id %s insert faild, the failed reason is %s'%(failed_bug,fail_dict[failed_bug]))
    def delete_duplicate(self):
        b1 = self.bug_table.find({'id': {'$gt': 0}}, ['id'])
        bug_list = [bug['id'] for bug in b1]
        for i in bug_list:
            if bug_list.count(i) > 1:
                print(i)
                self.bug_table.delete_one({'id':i})

class html_tables(object):
    def __init__(self, url):

        self.url = url
        self.r = requests.get(self.url)
        self.url_soup = BeautifulSoup(self.r.text)

    def read(self):

        self.tables = []
        self.tables_html = self.url_soup.find_all("table")

        # Parse each table
        for n in range(0, len(self.tables_html)):

            n_cols = 0
            n_rows = 0

            for row in self.tables_html[n].find_all("tr"):
                col_tags = row.find_all(["td", "th"])
                if len(col_tags) > 0:
                    n_rows += 1
                    if len(col_tags) > n_cols:
                        n_cols = len(col_tags)

            # Create dataframe
            df = pd.DataFrame(index=range(0, n_rows), columns=range(0, n_cols))

            # Create list to store rowspan values
            skip_index = [0 for i in range(0, n_cols)]

            # Start by iterating over each row in this table...
            row_counter = 0
            for row in self.tables_html[n].find_all("tr"):

                # Skip row if it's blank
                if len(row.find_all(["td", "th"])) == 0:
                    next

                else:

                    # Get all cells containing data in this row
                    columns = row.find_all(["td", "th"])
                    col_dim = []
                    row_dim = []
                    col_dim_counter = -1
                    row_dim_counter = -1
                    col_counter = -1
                    this_skip_index = copy.deepcopy(skip_index)

                    for col in columns:

                        # Determine cell dimensions
                        colspan = col.get("colspan")
                        if colspan is None:
                            col_dim.append(1)
                        else:
                            col_dim.append(int(colspan))
                        col_dim_counter += 1

                        rowspan = col.get("rowspan")
                        if rowspan is None:
                            row_dim.append(1)
                        else:
                            row_dim.append(int(rowspan))
                        row_dim_counter += 1

                        # Adjust column counter
                        if col_counter == -1:
                            col_counter = 0
                        else:
                            col_counter = col_counter + col_dim[col_dim_counter - 1]

                        while skip_index[col_counter] > 0:
                            col_counter += 1

                        # Get cell contents
                        cell_data = col.get_text().strip()

                        # Insert data into cell
                        df.iat[row_counter, col_counter] = cell_data

                        # Record column skipping index
                        if row_dim[row_dim_counter] > 1:
                            this_skip_index[col_counter] = row_dim[row_dim_counter]

                # Adjust row counter
                row_counter += 1

                # Adjust column skipping index
                skip_index = [i - 1 if i > 0 else i for i in this_skip_index]

            # Append dataframe to list of tables
            self.tables.append(df)

        return (self.tables)

if __name__ == '__main__':
    test = GetBug()
    bugs = test.bug_table.find({'id': {'$gt': 0}}, ['id'])
    bug_list = [bug['id'] for bug in bugs]
    full_list = [i for i in range(min(bug_list), max(bug_list) + 1)]
    diff_list = list(set(full_list).difference(set(bug_list)))
    length = int(len(diff_list) / 20)
    print(len(full_list))
    print(len(bug_list))
    print(diff_list)
    test.set_bug_to_db(0,0,diff_list)
    #test.delete_duplicate()
    for i in range(0, 20):
        #t = threading.Thread(target=test.set_bug_to_db,args=(1+i*4000,4000*(1+i)))
        start = i * length
        if i == 19:
            end = len(diff_list) - 1
        else:
            end = i * length + length - 1
        print("%d,%d"%(start,end))
        #t = threading.Thread(target=test.set_bug_to_db, args=(0, 0, diff_list[start:end]))
        #t.start()
    #tes    t.set_bug_to_db(start_id=169130,end_id=169135)
