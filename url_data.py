import sys, os, sets, hashlib, binascii, lmdb, copy, json, ast, datetime, sqlite3, time, MySQLdb
import urllib,urllib2, httplib
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import config

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__

class PYAPI():
    def __init__(self):
        self.config = config.Config

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst        
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def validate_login(self, ijson):
        import login.user_info as login
        obj = login.Login(self.config)
        return obj.validate_login(ijson)


    def data_path_method_url_execution(self, ijson):
        import url_execution as ue
        u_Obj = ue.Request()
        path = ijson['path']
        http_method = ijson.get('method', 'GET')
        data = ijson['data']
        print 'HHHHHHHHHHHHHHHHHHHHH'
        if http_method  == 'GET':
            data = json.dumps(data)
            url_info = ''.join([path, data])
            txt, txt1   = u_Obj.load_url(url_info, 120)
            res     = json.loads(txt1)
            if not isinstance(res, list):
                res = [res]
            return res
        elif http_method == 'POST':
            splt_ar = path.split("://")
            path_host_lst = splt_ar[1].split("?")[0].split("/")
            path_host  = path_host_lst[0]
            extention = '/' + path_host_lst[1]
            data = json.dumps(data)
            params = {'full_data':data}
            params = urllib.urlencode(params)
            headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
            conn  = httplib.HTTPConnection(path_host, timeout=12000)
            conn.request("POST",  extention, params, headers)
            response = conn.getresponse()
            d_info = response.read()
            conn.close()
            res     = json.loads(d_info)
            if not isinstance(res, list):
                res = [res]
            return res


