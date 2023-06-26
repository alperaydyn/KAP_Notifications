# -*- coding: utf-8 -*-
"""
    This module is responsible for scraping KAP (Public Disclosure Platform) website to get the stock market company details and the notifications that are broadcasted continiously.
    
    Module consists of two main classes:
    - database: for handling the collected data in a sqlite3 database
    - reader: class responsible for collecting the data from target website
    
    #################################################
    # USAGE (Module)
    #################################################
    
    import kap_reader
    kapdb = kap_reader.database()
    kaprd = kap_reader.reader()

    kapdb.create_database()                   # drop database and recreate
    kapdb.get_conn()
    kapdb.test_db()    
    kapdb.get_company('AVOD')                 # get single company from database

    comps = kaprd.read_companies()            # read summary table
    kaprd.read_company_detail(comps[0][-1])   # read a single company from summary table
    kaprd.read_companies(comps)               # read all the company details for each company in summary (~4min)
    kaprd.get_missing_notifications()         # read missing notifications that are between max and previous notifications

    ret = kapdb.get_last_notification()
    print(ret)

    ntf = kaprd.get_notification(1089669)     # read a single notification and SAVES automatically
    kapdb.save_notification(ntf)              # save notification, not required
    
    
    #################################################
    # USAGE (terminal)
    #################################################
    
    $ python kap_reader.py -c         // truncate insert company table, reader.read_companies()
    $ python kap_reader.py -z         // read last notification from db, database.get_last_notification()
    $ python kap_reader.py -n 100     // loop web for notifs from last to last+100, reader.get_notifications(limit={limit}) 
    $ python kap_reader.py -g 1089123 // read single notification with the given notification_id and saves automatically
    $ python kap_reader.py -m         // read missing notifications, reader.get_missing_notifications()
    
    optional arguments:
      -h, --help            show this help message and exit
      -c, --companies       truncate insert company table
                            (reader.read_companies()), est. ~4 minutes (default:
                            False)
      -z, --last_notif      read last notification from db
                            (database.get_last_notification()) (default: False)
      -n READ_NOTIFS, --read_notifs READ_NOTIFS
                            read notifications from web starting from max to
                            max+limit (reader.get_notifications(limit={limit})).
                            -1 for no limit (default: 0)
      -g READ_NOTIF, --read_notif READ_NOTIF
                            read single notification
                            (reader.read_notification(id)) (default: None)
      -m, --missing_notifs  read missing notifications
                            (reader.get_missing_notifications()) (default: False)
    
"""

import os          # drop if database exists
import sys         # add api key from config file
import requests    # web scraping
from bs4 import BeautifulSoup
import sqlite3     # local database tasks
import unicodedata # normilize notification explanations
import time 
from wrapt_timeout_decorator import timeout # managing timeout hanging from openai
import argparse    # running with parameters from console


# summarization api ------------------------------------------
import openai   
if not '/data/home/alperayd/Projects/OpenAI/' in sys.path:
    sys.path.append('/data/home/alperayd/Projects/OpenAI/')
import CONFIG as cfg
import imp

# set openai api key with the config file api key
openai.api_key = cfg.OPENAI_API_KEY


API_SUMMARIZATION_PROMPT = """
    Aşağıdaki dokümanda önemli konular firmaların faaliyet alanları, kişi bilgileri, ad soyad bilgileri, firmadaki görevleri, yatırım bilgileri gibi önemli bilgiler yer almaktadır. Aşağıdaki dokümandaki önemli noktaları vurgulayarak en kısa şekilde Türkçe olarak özetle
    """
API_SUMMARIZATION_PROMPT = """
You are a 10 year experienced banking key account manager working with public companies. You read their public disclosure messages everyday and extract useful information to identify the company's strengths and weaknesses. You are explicitly focused on their activities and investments such as technology or green energy, their growth strategies and investment policies. You are also alert on the names in the documents so that you can track the changes in board members or company representatives. According to this information summarize the following message in Turkish, step by step, as short as possible without omitting any important information.
"""
API_ENGINE = "gpt-3.5-turbo"
API_TEMPERATURE = 0.05
API_TOP_P = 0.1
API_MAX_CONTEXT_LENGTH = 4097
API_TIMEOUT = 15
# ------------------------------------------------------------


# assign proxy as environment variable -----------------------
proxy = 'http://tekprxv2:80'
os.environ['http_proxy'] = proxy
os.environ['HTTP_PROXY'] = proxy
os.environ['https_proxy'] = proxy
os.environ['HTTPS_PROXY'] = proxy
# ------------------------------------------------------------


class database():
    """
        Database class to handle scraped data from target web site
        
        Consists of following functions:
        
        Database Tasks
        --------------
        - create_database(): drops and creates new db file, 
                executes create table predefined queries provided as class attributes
        - get_conn(): connects and returns the connection to db
        
        Notification Tasks
        ------------------
        - get_last_notification(): returns max notification id and publish date from KAP_NOTIFICATIONS table
        - delete_notification(int): deletes the notification with the given id, in order to keep ids unique
        - save_notification(dict): saves scraped notification, provided in dictionary format
        
        Company Tasks
        -------------
        - get_company(str): read the company information with the given company_id, returns db column values as tuple
        - delete_company(str): deletes the company with the given company_id
        - save_company(list): deletes existing company and inserts new values provided as a list
    """
    
    sql_create_tbl_companies = """
        create table KAP_COMPANIES 
        (
            CODE varchar(8), COMPANY_NAME varchar(128), PROVINCE varchar(64), COMPANY_URL varchar(128),
            TAX_NO varchar(10), REG_NO varchar(16), SCOPE varchar(4000), 
            EMAIL varchar(128), WEBADDRESS varchar(128), ADDRESS varchar(2000), SECTOR TEXT
        )
    """
    sql_create_tbl_notifs = """
        create table KAP_NOTIFICATIONS
        (CODE varchar(8), NOTIFICATION_ID INT, PUBLISH_DATE TEXT, DISCLOSURE_TYPE TEXT, YEAR TEXT, PERIOD TEXT,
         SUMMARY_INFO TEXT, RELATED_COMPANIES, EXPLANATIONS BLOB,
         EXPLANATION_SUMMARY BLOB
        )
    """
        
    def __init__(self, db_path='kap.db'):
        self.db_path = db_path
        pass
    
    # database tasks ---------------------------------------
    def create_database(self, db_path='kap.db'):
        """
            creates an empty database (removes if file already exists) and creates the required tables 
            with the given sql statements in the class attributes:
            - sql_create_tbl_companies
            - sql_create_tbl_notifs
        """
        
        self.db_path = db_path
        
        if os.path.isfile(self.db_path):
            os.remove(self.db_path)

        conn = sqlite3.connect(self.db_path)
        conn.execute(self.sql_create_tbl_companies)
        conn.commit()

        conn.execute(self.sql_create_tbl_notifs)
        conn.commit()

        conn.close()

        print('db created')
        
    def get_conn(self):
        """
            Returnes the connection to the desired sqlite3 database.
            reads the database path from class attribute (db_path) which can be set at class init.
            
            Returns
            -------
                connection
                    Sqlite3 connection 
        """
        
        conn = sqlite3.connect(self.db_path)
        return conn
    
    def read_data(self, sql):
        """
            reads data from the sqlite3 database and returns as a list of dicts having 'data' and 'columns' keys.
            
            Parameters
            ----------
                sql: string
                SQL statement that contains the data desired. 
                If multiple tables are required semicolon can be used as seperator.
                
            Returns
            -------
                list of dicts
                    Returns a list containing the results of each sql statements with each result is stored as a 
                    dictionary that contains 'columns' key for column names and 'data' key for storing thre returned data
            
            Example
            -------

                > kd = kap_reader.database()
                > ret = kd.read_data('companies')
                > dfs = [pd.DataFrame(r.get('data'), columns=r.get('columns')) for r in ret]
                > df = dfs[0]            
        """
        
        if sql=='all':
            sql = "select * from KAP_COMPANIES;select * from KAP_NOTIFICATIONS"
        elif sql=='companies':
            sql = "select * from KAP_COMPANIES"
        elif sql=='notifications':
            sql = "select * from KAP_NOTIFICATIONS"
            
        conn = self.get_conn()
        curr = conn.cursor()
        sqls = sql.split(';')
        ret = []
        for sql in sqls:
            curr.execute(sql)
            columns = list(map(lambda x: x[0], curr.description))
            data = curr.fetchall()
            ret.append({'columns': columns, 'data': data})
            curr.close()
        conn.close()
            
        return ret
    
    # notification tasks -----------------------------------
    def get_notification(self, notification_id):
        """
            Returns the desired notification from KAP_NOTIFICATIONS table
        """
        
        sql = f"select * from KAP_NOTIFICATIONS where NOTIFICATION_ID={notification_id}"
        ret = self.read_data(sql)
        return ret
    
    def get_last_notification(self):
        """
            Returns the max NOTIFICATION_ID from KAP_NOTIFICATIONS table
        """
        
        conn = self.get_conn()
        sql = """
            select NOTIFICATION_ID, PUBLISH_DATE from KAP_NOTIFICATIONS 
            where NOTIFICATION_ID=(select max(NOTIFICATION_ID) as max_id from KAP_NOTIFICATIONS)
        """
        curr = conn.cursor()
        curr.execute(sql)
        ret = curr.fetchall()
        if ret==[(None,None)]: ret=-1
        return ret
    
    def delete_notification(self, notification_id):
        """
            Deleted the given notification_id from KAP_NOTIFICATIONS table.
            Used for removing existing notification in order to prevent duplicated notifications.
        """
        
        conn = self.get_conn()
        curr = conn.cursor()
        curr.execute(f"""DELETE FROM KAP_NOTIFICATIONS WHERE NOTIFICATION_ID={notification_id}""")
        conn.commit()
        curr.close()
        conn.close()
    
    def save_notification(self, notification):
        """
            delete if notification_id exists and save notification to db
            because of the  latency in summarization explanation_summay may be passed empty string
            and updated later.
            
            Parameters:
            -----------
            notification: dict:
                ret = {
                    'code': code,
                    'notification_id': id,
                    'publish_date':publish_date,
                    'disclosure_tpye':disclosure_tpye, 
                    'year': year, 'period':period, 
                    'summary': summary,
                    'related': related,
                    'explanations':explanations,
                    'explanation_summary': explanation_summary
                }                
        """
        try:
            conn = self.get_conn()
            curr = conn.cursor()
            self.delete_notification(notification.get('notification_id',-1))
                                
            curr.execute("""INSERT INTO KAP_NOTIFICATIONS 
                            (CODE, NOTIFICATION_ID, PUBLISH_DATE, DISCLOSURE_TYPE, YEAR, PERIOD,
                             SUMMARY_INFO,RELATED_COMPANIES, EXPLANATIONS, 
                             EXPLANATION_SUMMARY
                            )
                            VALUES (?,?,?,?,?,?,?,?,?,?)
                         """, list(notification.values()))
            conn.commit()
            curr.close()
            conn.close()
            return 1, 'ok'
        except Exception as ex:
            return 0, str(ex)
    
    def get_missing_notifications(self):
        """
            Generate a list of unused IDs that can be used for new notifications in the kap_notifications table. 
            Create a sequence of IDs from the minimum to maximum notification_id in the table and then filter out 
            any IDs that are already present in the table.        
        """
        sql = """
            WITH RECURSIVE c(x) AS (
             VALUES(1)
             UNION ALL
             SELECT x+1 FROM c WHERE x<1000000
           )
           select a.CID
           from (
               SELECT x+min_id as CID 
               FROM c,
                   (select min(notification_id) as min_id, max(notification_id) as max_id 
                    from kap_notifications
                   ) d
               where x < max_id-min_id
            ) a
            left join (select notification_id from kap_notifications) b on a.CID=b.notification_id
            where b.notification_id is null
        """
        ret = self.read_data(sql)
        return ret
        
    def update_notification(self, notification_id, column_name, value):
        """
            updates existing record on db. required for asynchronous summarization task
        """
        try:
            sql = f"""
                update KAP_NOTIFICATIONS
                set {column_name}=?
                where NOTIFICATION_ID=?
            """
            conn = self.get_conn()
            conn.execute(sql, 
                         (value, notification_id)
                        )
            conn.commit()
            conn.close()
            return 1, 'ok'
        
        except Exception as ex:
            return 0, str(ex)
    
    # company tasks ----------------------------------------
    def get_company(self, company_id):
        """
            read the company information with the given company_id
            
            Parameters
            ----------
            company_id: int
                Company stock market code
                
            Returns:
            --------
            array_like
                Tuple value returned from sqlite3 cursor containing following column values
                CODE, COMPANY_NAME, PROVINCE, COMPANY_URL, TAX_NO, REG_NO, SCOPE, EMAIL, WEBADDRESS, ADDRESS, SECTOR
        """
        conn = self.get_conn()
        curr = conn.cursor()
        curr.execute(f"""select * from KAP_COMPANIES WHERE CODE='{company_id}'""", )
        ret = curr.fetchall()
        curr.close()
        conn.close()
        return ret[0] if len(ret)>0 else []
        
    def delete_company(self, company_id):
        """
            Deletes existing company from KAP_COMPANIES table
        """
        
        conn = self.get_conn()
        curr = conn.cursor()
        curr.execute(f"""DELETE FROM KAP_COMPANIES WHERE CODE='{company_id}'""", )
        conn.commit()
        curr.close()
        conn.close()
        
    def save_company(self, company):
        """
            deletes existing company and inserts new values with 'company' parameter provided as list.
            
            Parameters:
            -----------
            company = list:
                [code, name, province, url, tax_no, reg_no, scope, email, web, address, sector]
                
        """
        try:
            conn = self.get_conn()
            curr = conn.cursor()
            self.delete_company(company[0])
                                
            curr.execute("""INSERT INTO KAP_COMPANIES 
                            (CODE, COMPANY_NAME, PROVINCE, COMPANY_URL, TAX_NO, REG_NO,
                             SCOPE, EMAIL, WEBADDRESS, ADDRESS, SECTOR
                            )
                            VALUES (?,?,?,?,?,?,?,?,?,?,?)
                         """, company)
            conn.commit()
            curr.close()
            conn.close()
            return 1, 'ok, company saved'
        except Exception as ex:
            return 0, str(ex)
    
    
class reader():
    """
        Reader class to handle web scraping commands.
        
        Consists of following functions;
        
        General
        -------
        - has_hidden_parent():
        
        Company Functions
        -----------------
        - read_companies_list():
        - read_company_detail(url, lang='tr'):
        - read_companies():
        
        Notification Functions
        ----------------------
        - get_last_notification(): !NOT WORKING!
        - get_notification(id, lang, return_soup):
        - get_notifications(limit):
        - get_missing_notifications():
        - display_random_notifs(n, lang):
    """
    
    root_url = "http://kap.org.tr/tr"
    headers={'user-agent': 'kap/0.1.1'}
    proxies={"http":"http://tekprxv2:80", "https":"http://tekprxv2:80"}
    notification_initial_id = 1083300 # 2022/12/01
    
    def __init__(self, db_path='kap.db'):
        self.db = database()
    
    def has_hidden_parent(self,e):
        """
            recursively searches and finds hidden parent of the given tag item 'e'.
            returns True if finds at any level
        """
        try:   p = e.parent
        except Exception as ex: return False

        try:    pstyle = p['style'] 
        except: pstyle = ''

        if 'display: none' in pstyle:  return True
        else: return self.has_hidden_parent(p)    
    
    def read_companies_list(self):
        """
            read all companies from summary table at https://www.kap.org.tr/tr/bist-sirketler
        """

        url = "https://www.kap.org.tr/tr/bist-sirketler"
        parent_div = "printAreaDiv"
        container_divs = "column-type7 wmargin"
        data_divs = "w-clearfix w-inline-block comp-row"
        href_prefix = "https://www.kap.org.tr"

        # make request and get the page content
        req = requests.get(url, headers=self.headers, proxies=self.proxies)

        # parse page content
        soup = BeautifulSoup(req.content, "html.parser")

        # find the main container (page specific)
        pdiv = soup.find(id=parent_div)

        # find the rows to read inside the container
        rdivs = pdiv.find_all("div", {'class':data_divs})

        # store row data
        companies = []

        # rdivs header:
        # Code | Company Name | Province | Independent Audit Company
        print('start reading company list')
        for rdiv in rdivs:
            # get only first three cells of the row
            cdivs = rdiv.find_all("div")[:3]

            # read url from the anchor in the first cell
            href = href_prefix + cdivs[0].find('a',href=True)["href"]

            # append url to the first three cell
            cells = [cell.text.replace('\n','') for cell in cdivs] + [href]

            companies.append(cells)
        print(f'completed, {len(companies)} company found')
        return companies
    
    def read_company_detail(self, url, lang='tr'):
        """
            read a single company and save to db
        """
        
        if lang=="en":
            tax_text = " Tax Number "
            registration_text = " Registration Number "
            scope_text = " Scope of Activities of Company "
            email_text = "E-mail Adress"
            web_text = " Web-site "
            address_text = " Head Office Address "
            sector_text = " Sector of Company "
        else:
            tax_text = " Vergi No "
            registration_text = " Ticaret Sicil Numarası "
            scope_text = " Şirketin Faaliyet Konusu "
            email_text = "Elektronik Posta Adresi"
            web_text = " İnternet Adresi "
            address_text = " Merkez Adresi "
            sector_text = " Şirketin Sektörü "
            
        
        
        req = requests.get(url.replace('ozet','genel') , headers=self.headers, proxies=self.proxies)
        soup = BeautifulSoup(req.content, "html.parser")

        name = soup.find(id='companyOrFundNameArea').text.strip()
        code = soup.find('h6').text.strip()
        
        parent_div = "printAreaDiv"
        pdiv = soup.find(id=parent_div)

        # find the caption div and read the next value div text
        tax_no = pdiv.find_all("div", string=tax_text)[0].findNext("div", {"class":"exportDiv"}).text.strip()

        # find registration number
        reg_no = pdiv.find_all("div", string=registration_text)[0].findNext("div", {"class":"exportDiv"}).text.strip()

        # company scope : Scope of Activities of Company
        scope = pdiv.find_all("div", string=scope_text)[0].findNext("div", {"class":"exportDiv"}).text.strip()

        # email:
        email = pdiv.find_all("div", string=email_text)[0].findNext("div", {"class":"exportDiv"}).text.strip()

        # web-site:
        web = pdiv.find_all("div", string=web_text)[0].findNext("div", {"class":"exportDiv"}).text.strip()

        # address :  Head Office Address 
        address = pdiv.find_all("div", string=address_text)[0].findNext("div", {"class":"exportDiv"}).text.strip()
        province = address.split('/')[-1].split(' ')[-1]

        # web-site:
        sector = ''
        try: sector = pdiv.find_all("div", string=sector_text)[0].findNext("div", {"class":"exportDiv"}).text.strip()
        except: pass
        
        # aggregate all
        company = [code, name, province, url, tax_no, reg_no, scope, email, web, address, sector]
        
        # save to db
        self.db.delete_company(code)
        #print('saving company')
        ret = self.db.save_company(company)
        #print('save company result:', ret)
        
        return company
    
    def read_companies(self, comps=None):
        """
            loop through all the companies in comps which is the return value of self.read_companies_list().
            if not provided, calls read_companies_list() and reads the list from website.
            
            Using this list, this function iteratively calls self.read_company_detail() 
            and saves the company each time
        """
        if isinstance(comps, type(None)):
            compsd = self.read_companies_list()
        else:
            compsd = comps.copy()

        len_companies = len(compsd)
        print(f'read company details for {len_companies} companies started.')
        for i,c in enumerate(compsd):
            # read company details and save to db
            detail = self.read_company_detail(c[-1])
            time.sleep(0.1)
            if i>0 : print('.', end='', flush=True)
            if i>0 and i%10==0: print('|', end='', flush=True)
            if i>0 and i%50==0: print(f'|{i}/{len_companies}') # 50 for console display
        
        print(f'\nread companies completed, {i} company information updated')
        return compsd    
    
    def get_last_notification(self):
        """
            input_tag = soup.find_all(attrs={"name" : "stainfo"})
            mydivs = soup.find_all("div", {"class": "stylelistrow"})
        """
        req = requests.get(self.root_url, headers=self.headers, proxies=self.proxies)
        soup = BeautifulSoup(req.content)
        
        all_divs = soup.find_all("div", href=True)
        for div in all_divs:
            # notification id is stored in href attribute in div
            # <div class="w-inline-block notifications-column _12" href="/tr/Bildirim/1089617" ...
            div_href = div.get_attr("href")

    def get_notification(self, id, lang='tr', return_soup=False):
        """
            Reads the required notification from the following we address with the given parameters. 
            Default language mode is 'tr' because the keywords that are used for addressing the 
            correct positions of the data is in Turkish language.
            
            https://www.kap.org.tr/{lang}/Bildirim/{id}
            
            Parameters
            ----------
            
            id: int
                Notification id 
                
            lang: str=tr
                site language
                
            return_soup: boolean=False
                returns BeautifulSoup parsed data only. Following lines after parsing are not executed.
        """
        
        try:
            req = requests.get(f"https://www.kap.org.tr/{lang}/Bildirim/{id}", headers=self.headers, proxies=self.proxies)
            
        except Exception as ex:
            #print(ex)
            return {'error':'request error'}
        
        if req.status_code!=200:
            #print(f'notification {id} not found. {req.status_code}')
            return {'error':'not found'}
            
        else:
            soup = BeautifulSoup(req.content, "html.parser")
            if return_soup:
                return soup

            # code ----------------------------------------------------------------------
            code = soup.find_all('div',{'class':'modal-headertext'})[0].find_all('div',{'class':'type-medium bi-dim-gray'})[0].text
            code = (code.split(',')[1] if ',' in code else code).strip() 

            # summary block -------------------------------------------------------------
            divx = soup.find_all('div', {'class':'w-row modal-briefsummary'})[0] \
                       .find_all('div', {'class','type-medium bi-sky-black'})

            # summary items -------------------------------------------------------------
            publish_date = divx[0].text.strip()
            disclosure_tpye = divx[1].text.strip()
            year = divx[2].text.strip()
            period = divx[3].text.strip()

            # explanations --------------------------------------------------------------
            disclaimer_text = """Yukarıdaki açıklamalarımızın, Sermaye Piyasası Kurulunun yürürlükteki Özel Durumlar Tebliğinde yer alan esaslara uygun olduğunu, bu konuda/konularda tarafımıza ulaşan bilgileri tam olarak yansıttığını, bilgilerin defter, kayıt ve belgelerimize uygun olduğunu, konuyla ilgili bilgileri tam ve doğru olarak elde etmek için gerekli tüm çabaları gösterdiğimizi ve yapılan bu açıklamalardan sorumlu olduğumuzu beyan ederiz."""
            exclusion_text = ['A','+','-','İmza','Özet Bilgi']
            container = soup.find_all('div',{'class':'modal-info'})[0]

            explanations = ';'.join([
                s.strip().replace(disclaimer_text,'{disclaimer}').replace(';',' ').replace(u'\xa0', u' ')
                for s in container.findAll(text=True) 
                if s.strip()!='' 
                and not '{{' in s 
                and not 'oda_' in s
                and s.strip() not in exclusion_text
                and not self.has_hidden_parent(s)
                #and not s.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']
            ])

            # related companies ---------------------------------------------------------
            related_text = 'İlgili Şirketler'

            rows = soup.find_all('tr')
            related = '[]'
            try:
                related = [row.find_all('td')[1].text.strip() for row in rows if related_text in row.text][0]
            except: pass

            # summary info --------------------------------------------------------------
            summary_text = 'Özet'

            summary = ''.join([
                span.findNext('div').text.strip() 
                for span in soup.find_all("span") 
                if summary_text in span.text.strip()]
            )

            # return values -------------------------------------------------------------
            ret = {
                'code': code,
                'notification_id': id,
                'publish_date':publish_date,
                'disclosure_tpye':disclosure_tpye, 
                'year': year, 'period':period, 
                'summary': summary,
                'related': related,
                'explanations':explanations
            }

            # save to db ----------------------------------------------------------------
            self.db.save_notification(ret)
            
            return ret 

    def get_notifications(self, limit=100):
        """
            Reads the notifications starting from the last notification found in the KAP_NOTIFICATIONS table
            and loops incrementally until the limit size or no page found with the next iteration
        """
        last_notif_id, last_notif_date = self.db.get_last_notification()[0]
        if last_notif_id==-1: last_notif_id = self.notification_initial_id
        
        print(f'start reading notifications from: {last_notif_id} / {last_notif_date}, limit={limit}')
        try:
            for i,id in enumerate(range(last_notif_id+1, last_notif_id+1+limit)):
                # read next notification
                notif = self.get_notification(id)

                # if requested notification not found, break
                if isinstance(notif, type(None)):
                    raise StopIteration

                # add sleep time to prevent blocking
                time.sleep(0.2)

                # display loop progress
                if i>0 : print('.', end='', flush=True)
                if i>0 and i%10==0: print('|', end='', flush=True)
                if i>0 and i%50==0: print(f'|{i}/{limit} : {notif.get("publish_date")}') # 50 for console display
                    
        except StopIteration:
            # if notif==None and loop terminated with StopIteration raise
            print('\nfinal page reached')
        
        else:
            # if loop competed with the given limit parameter
            # display completed message
            print('')
            print(f'limit parameter ({limit}) reached')
            try:
                print(f'last notif: {notif.get("notification_id")}:{notif.get("publish_date")}')
            except:
                print('last notif not found. get_notifications() loop completed without assigning any notification')
    
    def get_missing_notifications(self):
        ret = self.db.get_missing_notifications()[0]
        for i in ret['data']:
            print(i[0])
            self.get_notification(i[0])
    
    def _refresh_all_notifications(self, start=None, stop=None):
        """
            Re-runs all the notifications existing in the database and updates all records.
            Start and stop notification_ids are retrieved from KAP_NOTIFICATIONS table if not provided, 
            else given parameters is used.
            
            iterations are stored in a log file in order to track. (Not necessary, debugging purpose only)
        """
        
        kdf = self.db.read_data(sql='select min(NOTIFICATION_ID), min(PUBLISH_DATE), count(*) from KAP_NOTIFICATIONS')[0]
        print(kdf)

        log = []
        start = start or kdf['data'][0][0]
        stop = stop or kdf['data'][0][0]+kdf['data'][0][2]+1
        print(f'Start refreshing all notifications:{start}-{stop}')
        
        for i,j in enumerate(range(start, stop)):
            logr = f'{i};{j};{time.time():.0f};'

            ret = self.get_notification(j)
            if ret.get('error','') == 'request error':
                # write waiting
                if i>0: print('~', end='', flush=True)

                while ret.get('error','') == 'request error':
                    logr = logr + f'~'
                    time.sleep(10)
                    ret = kr.get_notification(j)
                logr = logr + ';'
            else:
                logr = logr + f'.;'
                if i>0 : print('.', end='', flush=True)

            log.append(logr)

            if i>0 and i%10 == 0: print('|', end='', flush=True)   
            if i>0 and i%50 == 0: print('|', i, j, end='\n', flush=True) 
            if i>0 and i%50 == 0:
                with open('log.txt','a') as f:
                    f.write('\n'.join(log)+'\n')
                log = []
            time.sleep(0.5)
        print('\nrefreshing all notifications completed')
        
    def display_random_notifs(self, n, lang='tr'):
        """
            Display top n random records
        """
        kdf = self.db.read_data(sql=f"""
                select * from KAP_NOTIFICATIONS where NOTIFICATION_ID in (
                    SELECT NOTIFICATION_ID FROM KAP_NOTIFICATIONS ORDER BY RANDOM() LIMIT {n}
                )
            """)[0]
        print(kdf['columns'])
        for r in kdf['data']:
            print('*', r[2], r[0], r[6])
            print('>', r[8])
            print(f'https://www.kap.org.tr/{lang}/Bildirim/{r[1]}')
            print('-'*70)        
    
    @timeout(API_TIMEOUT)
    def summarize_explanation(self, explanation):
        """
            Summarizes an explanation using OpenAI ChatCompletion API.
            prompt is set by the API_SUMMARIZATION_PROMPT config variable followed by 'explanation' parameter.
            explanation is limited with the config variable API_MAX_CONTEXT_LENGTH

            Parameters
            ----------
            explanation : str
                The explanation text to be summarized.

            Returns
            -------
            explanation_summary : str
                The summarized explanation text.
            total_tokens : int
                The total number of tokens used by the API for summarization.
        """
        ret = openai.ChatCompletion.create(
          model = API_ENGINE,
          messages=[
                {"role": "user", "content": API_SUMMARIZATION_PROMPT },
                {"role": "user", "content": explanation[:API_MAX_CONTEXT_LENGTH] }
            ],
            temperature=API_TEMPERATURE,
            top_p=API_TOP_P
        )
        ret
        explanation_summary = ret['choices'][0]['message']['content']
        total_tokens = ret['usage']['total_tokens']
        return explanation_summary, total_tokens
    
    def save_notification_summary(self, notification_id):
        """
            - read notification from db, 
            - summarize using api 
            - update notification record on db
            
            timeout decorator prevents hanging for a long time due to openai server delays.
            
            Returns
            -------
            ret : str
                return value received from db.update_notification
                    which is: 1,'ok' or 0, save error
                if error return None, 0
        """
        notification = self.db.get_notification(notification_id)
        explanation = notification[0]['data'][0][-2]
        try:
            explanation_summary, total_tokens = self.summarize_explanation(explanation)
            ret = self.db.update_notification(notification_id, 'EXPLANATION_SUMMARY', f'{explanation_summary};{total_tokens}')
        except:
            ret = None, 0
            
        return ret
    
    def save_notification_summaries(self, top_n=100):
        """
            summarize all missing notifications in db starting from the latest
        """
        
        # read data from db, order by notification id desc, limit top_n
        sql = f"select * from KAP_NOTIFICATIONS where EXPLANATION_SUMMARY is null order by NOTIFICATION_ID desc LIMIT {top_n}"
        df = self.db.read_data(sql)
        
        # extract notification ids from return data
        notification_ids = [n[1] for n in df[0]['data']]
        
        for i, notification_id in enumerate(notification_ids):
            ret = self.save_notification_summary(notification_id)
            # print(notification_id, ret)
            if ret[0]==1:
                print('.', end='')
            else:
                print('-', end='')
                
            if i%10==0 and i>0:
                print('|', end='')
                
            if i%100==0 and i>0:
                # read token stats
                ret = self.db.read_data("select * from KAP_NOTIFICATIONS where EXPLANATION_SUMMARY is not null")
                tokens = [ int(r[-1].split(';')[-1]) for r in ret[0]['data']]
                print('\t', len(tokens), sum(tokens), sum(tokens)/len(tokens))
        
        return 
    
if __name__ == "__main__":
    cr = reader()
    cd = database()
    
    parser = argparse.ArgumentParser(description="arg parser", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # read companies
    parser.add_argument("-c", "--companies", action="store_true", 
                        help="truncate insert company table (reader.read_companies()), est. ~4 minutes", default=False)
    # read last notification from db
    parser.add_argument("-z", "--last_notif", action="store_true", 
                        help="read last notification from db (database.get_last_notification())", default=False)
    
    # read notifications starting from the last notification_id
    parser.add_argument("-n", "--read_notifs", type=int, 
                        help="read new notifications from web starting from max to max+limit (reader.get_notifications(limit={limit})). -1 for no limit", default=0)
    
    # read notifications starting from the last notification_id
    parser.add_argument("-g", "--read_notif", type=int, 
                        help="read single notification (reader.read_notification(id))")
    
    # read missing notifications that is less then max notification_id
    parser.add_argument("-m", "--missing_notifs", action="store_true", default=False,
                        help="read missing notifications (reader.get_missing_notifications())")
    
    # refresh existing notifications
    parser.add_argument("-r", "--refresh_existing", nargs=2, metavar=('start','stop'),
                        help="refresh existing notifications between start and stop parameters. -1,-1 for all")
    
    # display random notifications
    parser.add_argument("-d", "--display_random", type=int,
                        help="display given number of random notifications")
    
    args = parser.parse_args()
    config = vars(args)
    #print(config)
    
    if config.get('companies'): cr.read_companies()
    if config.get('last_notif'): print(cd.get_last_notification())
    if config.get('read_notif'): print(cr.get_notification(config.get('read_notif')))
    if config.get('read_notifs'): cr.get_notifications(config.get('read_notifs'))
    if config.get('missing_notifs'): cr.get_missing_notifications()
    if config.get('refresh_existing'): cr._refresh_all_notifications(
                                            int(config.get('refresh_existing')[0]),int(config.get('refresh_existing')[1]))
    if config.get('display_random'): cr.display_random_notifs(int(config.get('display_random')))
    
