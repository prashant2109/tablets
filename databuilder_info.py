import os, sys, json, sqlite3
import config
from itertools import chain
import db.get_conn as get_conn
conn_obj    = get_conn.DB()

class INC_DataBuilder(object):
    def mysql_connection(self, db_data_lst):
        import MySQLdb
        host_address, user, pass_word, db_name = db_data_lst
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def get_company_name(self, project_id, deal_id):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn  = sqlite3.connect(db_path)
        cur  =  conn.cursor()
        read_qry = 'select company_name from company_info where project_id="%s" and toc_company_id="%s" ;'%(project_id, deal_id)
        cur.execute(read_qry)
        table_data = cur.fetchone()
        conn.close()
        company_name = table_data[0]
        return company_name 
        
    def insert_table_type_global(self, ijson):
        type_flg     = ijson['type']
        description  = ijson['description']
        table_type   = ''.join([e.lower().capitalize() for e in description.split()])
        m_cur, m_conn = conn_obj.MySQLdb_conn(config.Config.classify_db)
        insert_stmt = """ INSERT INTO all_table_types(description, table_type, user_name, type) VALUES('%s', '%s', '%s', '%s'); """%(description, table_type, 'TAS-System', type_flg)
        try:
            m_cur.execute(insert_stmt)
            m_conn.commit()
        except:
            m_conn.close()
            return [{'message':'Table Type already exists'}]
        m_conn.close()
        return  [{'message':'done'}] 
        
    def read_distinct_table_types(self, ijson):
        type_flg = ijson['type']
        print config.Config.classify_db
        m_cur, m_conn = conn_obj.MySQLdb_conn(config.Config.classify_db) 
        read_qry = """ SELECT row_id, table_type, description FROM all_table_types WHERE type='{0}'; """.format(type_flg)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()

        res_lst = []
        for ix, row in  enumerate(t_data, 1):
            row_id, table_type, description = row
            data_dct = {'k':table_type, 'n':table_type, 'rid':row_id, 'desc':description, 'sn':ix} 
            res_lst.append(data_dct)
        res = [{'message':'done', 'data':res_lst}]
        return  res
                        
    def save_classification_info(self, ijson):
        company_id = ijson['company_id']
        data = ijson['data']
        user_name = ijson['user']
        insert_rows = []
        for row in data:
            doc_id, page_no, grid_id, class_id = row['doc_id'], row['page_no'], row['grid_id'], row['row_id']
            insert_rows.append((doc_id, page_no, grid_id, class_id))   
        if insert_rows:        
            db_path    = config.Config.data_builder_db%(company_id)
            conn, cur  = conn_obj.sqlite_connection(db_path)
            crt_stmt = """ CREATE TABLE IF NOT EXISTS classification(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER NOT NULL, page_no INTEGER NOT NULL, table_id VARCHAR(256) NOT NULL, user_name VARCHAR(32), date_time DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL)  """ 
            cur.execute(crt_stmt)
            insert_stmt = """ INSERT INTO classification(doc_id, page_no, table_id, user_name) VALUES(?, ?, ?, ?) """
            cur.executemany(insert_stmt)
            conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    def read_inc_grid_sys_usr_data(self, doc, pageno, grid, dbname):
        db_data_lst = ['172.16.20.52', 'root', 'tas123', dbname] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT sdata, udata FROM db_data_mgmt_grid_slt WHERE docid =%s AND pageno=%s AND groupid=%s AND groupid <1000 """%(doc, pageno, grid)
        m_cur.execute(read_qry)
        tdata = m_cur.fetchone()
        m_cur.close()
        m_conn.close()
        
        sinf, uinf = tdata
        if uinf:
            g_data = json.loads(uinf)
        else:
            g_data = json.loads(sinf)
        ddict = g_data.get('data', {})
        r_cs = ddict.keys()
        r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
        rc_d    = {}
        for r_c in r_cs:
            row, col = int(r_c.split('_')[0]), int(r_c.split('_')[1])  
            rc_d.setdefault(row, {})[col]   =  ddict[r_c]
        rows = rc_d.keys()
        rows.sort()
        row_lst = []
        col_dct = {}
        inf_map = {}
        for sn, row in enumerate(rows, 1):
            cols    = rc_d[row].keys()
            cols.sort()
            row_dct = {'sn':sn, 'rid':sn, 'cid':sn}
            for col in cols:    
                cell    = rc_d[row][col]
                stype   = cell['ldr']
                if stype == 'value': 
                   stype =  'GV'
                elif stype == 'hch': 
                   stype =  'HGH'
                elif stype == 'vch': 
                   stype =  'VGH'
                elif stype in ['gh', 'g_header']: 
                   stype =  'GH'
                else:
                   stype =  ''
                chref   = cell.get('chref', '')
                if chref:
                    xml_id =  cell['xml_ids'] #'#'.join(map(lambda x:x+'@'+chref, filter(lambda x:x.strip(), cell['xml_ids'].split('$$')[:])))
                else:
                    xml_id =  cell['xml_ids'] #'#'.join(filter(lambda x:x.strip(), cell['xml_ids'].split('$$')[:]))
                xml_lst = cell['xml_ids'].split('$$')
                bbox = cell['bbox']
                txt  = cell['data']
                rs   = cell['rowspan']
                cs   = cell['colspan']
                clr_class = ''
                clr_class = 'heat_map_lg_%s'%(stype) 
                dd = {'v':txt, 'x':xml_id, 'cs':cs, 'rs':rs}
                if clr_class:
                    dd  = {'v':txt, 'x':xml_id, 'cs':cs, 'rs':rs, 'bg':clr_class}
                col_dct[col] = 1 
                bl = []
                bbox_lst = bbox.split('$$')
                for bx in bbox_lst:#xmin_ymin_xmax_ymax
                    if bx:
                        b = map(lambda x:int(x), bx.split('_'))
                        x   = b[0]
                        y   = b[1]
                        w   = b[2] - b[0]
                        h   = b[3] - b[1]
                        bl.append([x, y, w, h])
                inf_map['%s_%s'%(sn, col)] = {'ref':[{'xml_list':xml_lst, 'pno':pageno,'d':doc,'bbox':bl}], 'cs':cs, 'rs':rs, 'row':row, 'col':col, 'section_type':stype}
                row_dct[col] = dd
            row_lst.append(row_dct)
        col_def = []
        for cl in col_dct:
            d_dct = {'k':cl, 'n':cl}
            col_def.append(d_dct)
        res_dct = {'data':row_lst, 'col_def':col_def, 'map':inf_map}
        #print res_dct, '\n'
        return res_dct 
        
    def read_doc_page_grid(self, dbname, doc_id):
        db_data_lst = ['172.16.20.52', 'root', 'tas123', dbname]
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT docid, pageno, groupid FROM db_data_mgmt_grid_slt WHERE docid =%s AND groupid <1000  limit 5; """%(doc_id)
        m_cur.execute(read_qry)
        tdata = m_cur.fetchall()
        m_cur.close()
        m_conn.close()
        r_lst = []
        for row in tdata:
            doc, page, grid = map(str, row)
            r_lst.append('_'.join((doc, page, grid)))
        return r_lst
        
    def read_des_doc_page_grid_info(self, company_id, doc_id, table_str=''):
        db_path = config.Config.doc_wise_db.format(company_id, doc_id) 
        if company_id in ('20_118', 'TestDB'):
            db_path = config.Config.test_db.format(doc_id)
        print db_path
        conn, cur = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT score, tableid FROM well_connecteddb; """ 
        if table_str:
            read_qry = """ SELECT score, tableid FROM well_connecteddb WHERE tableid in (%s); """%(table_str) 
        try:
            cur.execute(read_qry)
            t_data = cur.fetchall()
        except:t_data = ()
        conn.close()
        dt_dct = {}
        for row_data in t_data:
            score, tableid_str = row_data
            #doc_id, page_no, grid_id = tableid_str.split('_')
            dt_dct[tableid_str] = score
        return dt_dct
    
    def multi_table_module_info(self, ijson):
        db_name = ijson.get('db_name', 'AECN_INC')
        company_id = ijson.get('company_id', '20_139')
        doc_id  = ijson.get('doc_id')
        #data_lst = self.read_doc_page_grid(db_name, doc_id)
        data_dct =  self.read_des_doc_page_grid_info(company_id, doc_id)
        print data_dct
        res_lst = []
        inf_map = {}
        for row_str, score in data_dct.iteritems():
            doc_id, page_no, grid_id = map(int, row_str.split('_'))
            data_info = self.read_inc_grid_sys_usr_data(doc_id, page_no, grid_id, db_name) 
            dt_dct = {'k':row_str, 'n':'%s (Score: %s)'%(row_str, score)}
            res_lst.append(dt_dct)
            inf_map[row_str] = data_info
        return [{'message':'done', 'data':res_lst, 'map':inf_map}]

    def read_gh_info(self, doc, pageno, grid, dbname):
        db_data_lst = ['172.16.20.52', 'root', 'tas123', dbname] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT sdata, udata FROM db_data_mgmt_grid_slt WHERE docid =%s AND pageno=%s AND groupid=%s AND groupid <1000 """%(doc, pageno, grid)
        m_cur.execute(read_qry)
        tdata = m_cur.fetchone()
        m_cur.close()
        m_conn.close()
        
        sinf, uinf = tdata
        if uinf:
            g_data = json.loads(uinf)
        else:
            g_data = json.loads(sinf)
        ddict = g_data.get('data', {})
        r_cs = ddict.keys()
        r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
        rc_d    = {}
        for r_c in r_cs:
            row, col = int(r_c.split('_')[0]), int(r_c.split('_')[1])  
            rc_d.setdefault(row, {})[col]   =  ddict[r_c]
        rows = rc_d.keys()
        rows.sort()
        res_lst = []
        for sn, row in enumerate(rows, 1):
            cols    = rc_d[row].keys()
            cols.sort()
            row_dct = {'sn':sn, 'rid':sn, 'cid':sn}
            for col in cols:    
                cell    = rc_d[row][col]
                stype   = cell['ldr']
                if stype in ['gh', 'g_header']: 
                   stype =  'GH'
                chref   = cell.get('chref', '')
                if chref:
                    xml_id =  cell['xml_ids'] #'#'.join(map(lambda x:x+'@'+chref, filter(lambda x:x.strip(), cell['xml_ids'].split('$$')[:])))
                else:
                    xml_id =  cell['xml_ids'] #'#'.join(filter(lambda x:x.strip(), cell['xml_ids'].split('$$')[:]))
                xml_lst = cell['xml_ids'].split('$$')
                txt  = cell['data']
                #print 'RRRRRRR', txt
                res_lst.append(txt)
        return res_lst 
    
    def doc_wise_well_connected_tables(self, ijson):
        db_name = ijson.get('db_name', 'AECN_INC')
        company_id = ijson.get('company_id', 'TestDB')
        doc_id  = ijson.get('doc_id')
        data_dct =  self.read_des_doc_page_grid_info(company_id, doc_id)
        data_lst = []
        inf_map  = {}
        sorted_keys = sorted(data_dct.keys(), key=lambda x:int(data_dct[x]), reverse=True)
        for sn, row_str in enumerate(sorted_keys, 1):
            score = data_dct[row_str]
            doc_id, page_no, grid_id = map(int, row_str.split('_'))
            data_info = self.read_gh_info(doc_id, page_no, grid_id, db_name)
            grid_header = ' '.join(data_info) 
            row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn, 'score':{'v':score}, 'table_id':{'v':row_str}, 'GH':{'v':grid_header}}
            if company_id == 'TestDB':
                row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn, 'score':{'v':score}, 'table_id':{'v':'_'.join(row_str.split('#')[:-1])}, 'GH':{'v':grid_header}}
            data_lst.append(row_dct)
            inf_map['%s_%s'%(sn, 'table_id')]  = {'ref_k':row_str}
            inf_map['%s_%s'%(sn, 'score')]  = {'ref_k':row_str}
            inf_map['%s_%s'%(sn, 'GH')]  = {'ref_k':row_str}
            inf_map['%s_%s'%(sn, 'sn')]  = {'ref_k':row_str}
        col_def_lst = [{'k':'checkbox', 'n':'checkbox', 'v_opt':3}, {'k':'sn', 'n':'S.No', 'type':'SL', 'w':80}, {'k':'table_id', 'n':'Table Id', 'w':120}, {'k':'score', 'n':'Score', 'type':'SL', 'w':60}, {'k':'GH', 'n':'Description'}]
        return [{'message':'done', 'data':data_lst, 'map':inf_map, 'col_def':col_def_lst}]

    def doc_wise_well_connected_tables_ib(self, ijson):
        db_name = ijson.get('db_name', 'AECN_INC')
        company_id = ijson.get('company_id', 'TestDB')
        doc_id  = ijson.get('doc_id')
        data_dct =  self.read_des_doc_page_grid_info(company_id, doc_id)
        data_lst = []
        inf_map  = {}
        sorted_keys = sorted(data_dct.keys(), key=lambda x:int(data_dct[x]), reverse=True)
        for sn, row_str in enumerate(sorted_keys, 1):
            score = data_dct[row_str]
            if company_id in ('20_118', 'TestDB'):
                doc_id, page_no, grid_id, dtp_type = row_str.split('#')
            else:   
                doc_id, page_no, grid_id = row_str.split('_')
            data_info = self.read_gh_info(doc_id, page_no, grid_id, db_name)
            grid_header = ' '.join(data_info) 
            row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn, 'score':{'v':score}, 'table_id':{'v':row_str}, 'GH':{'v':grid_header}}
            if company_id in ('TestDB', '20_118'):
                row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn, 'score':{'v':score}, 'table_id':{'v':'_'.join(row_str.split('#')[:-1])}, 'GH':{'v':grid_header}}
            data_lst.append(row_dct)
            inf_map['%s_%s'%(sn, 'table_id')]  = {'ref_k':row_str}
            inf_map['%s_%s'%(sn, 'score')]  = {'ref_k':row_str}
            inf_map['%s_%s'%(sn, 'GH')]  = {'ref_k':row_str}
            inf_map['%s_%s'%(sn, 'sn')]  = {'ref_k':row_str}
        col_def_lst = []
        if data_dct: 
            col_def_lst = [{'k':'checkbox', 'n':'checkbox', 'v_opt':3}, {'k':'sn', 'n':'S.No', 'type':'SL', 'w':80}, {'k':'table_id', 'n':'Table Id', 'w':120}, {'k':'score', 'n':'Score', 'type':'SL', 'w':60}, {'k':'GH', 'n':'Description'}]
        return [{'message':'done', 'data':data_lst, 'map':inf_map, 'col_def':col_def_lst}]

    def data_according_to_table_id(self, ijson):
        db_name    = ijson.get('db_name', 'AECN_INC')
        company_id = ijson.get('company_id', '20_139')
        doc_id     = ijson.get('doc_id')
        data_lst   = ijson.get('data')
        table_str  = ', '.join(['"'+e+'"'for e  in data_lst])
        data_dct =  self.read_des_doc_page_grid_info(company_id, doc_id, table_str)
        res_lst = []
        inf_map = {}
        for row_str, score in data_dct.iteritems():
            if company_id in ('20_118', 'TestDB'):
                doc_id, page_no, grid_id = map(int, row_str.split('#')[:-1])
            else:
                doc_id, page_no, grid_id = map(int, row_str.split('_'))
            data_info = self.read_inc_grid_sys_usr_data(doc_id, page_no, grid_id, db_name) 
            dt_dct = {'k':row_str, 'n':'%s (Score: %s)'%(row_str, score)}
            res_lst.append(dt_dct)
            inf_map[row_str] = data_info
        return [{'message':'done', 'data':res_lst, 'map':inf_map}]
        
    def read_all_docs(self, company_id):
        project_id, deal_id = company_id.split('_')
        company_name = self.get_company_name(project_id, deal_id)
        db_path = config.Config.tas_company_db%(company_name, project_id)
        conn, cur = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id  FROM company_meta_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall() 
        conn.close()
        doc_dct = {}
        for row_data in t_data:
            doc_dct[row_data[0]]  = 1
        return doc_dct   
        
    def table_id_poss_with_across_docs(self, ijson):
        company_id = ijson['company_id']
        doc_ids = self.read_all_docs(company_id)
        table_id   = ijson['table_id']
        within     = ijson.get('within')
        if within:
            return [{'message':'done', 'data':[], 'col_def':[], 'map':{}, 'no_poss':[], 'one_poss':[], 'mul_poss':[]}]
        did     = ijson.get('doc_id')
        db_path    = config.Config.table_poss.format(company_id)
        try:
            conn, cur  = conn_obj.sqlite_connection(db_path)
            read_qry = """ SELECT seq_id, doc_page_grid FROM pair_info WHERE seq_id in (SELECT seq_id FROM pair_info WHERE doc_page_grid='%s') AND doc_page_grid != '%s'; """%(table_id, table_id)
            cur.execute(read_qry)
            t_data = cur.fetchall() 
            conn.close()
        except:t_data = ()
        
        dcid = table_id.split('_')[0]
        
        doc_wise_poss_dct = {}
        for row in t_data:
            seq_id, doc_page_grid  = row
            doc, page, grid = doc_page_grid.split('_')
            if within:
                if doc != did:continue 
            doc_wise_poss_dct.setdefault(doc, {})[doc_page_grid] =  seq_id
            
        #print doc_wise_poss_dct
        data_lst = []    
        inf_map  = {}
        no_poss_docs  = []
        ml_poss_docs  = []
        one_poss_dcos = []
        for sn, doc_id in enumerate(doc_ids, 1):
            table_dct = doc_wise_poss_dct.get(str(doc_id), {})
            if not table_dct:
                if str(doc_id) == dcid:continue
                row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn, 'color':{'v':'R'}, 'doc_id':{'v':doc_id}}
                no_poss_docs.append(row_dct)
            if len(table_dct) == 1:
                color = 'G'
            elif len(table_dct) > 1:
                color = 'O'
            for tab, score in table_dct.iteritems():
                row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn, 'color':{'v':color}, 'doc_id':{'v':doc_id}, 'table_id':{'v':tab}, 'score':{'v':score}}
                if color == 'G':
                    one_poss_dcos.append(row_dct)
                elif color == 'O':
                    ml_poss_docs.append(row_dct)
        if one_poss_dcos:
            one_poss_dcos.sort(key=lambda x:x['score']['v'], reverse=True)
        if ml_poss_docs:
            ml_poss_docs.sort(key=lambda x:x['score']['v'], reverse=True)
        col_def_lst = [{'k':'checkbox', 'n':'', 'type':'SL', 'v_opt':3}, {'k':'sn', 'n':'S.No', 'type':'SL'}, {'k':'doc_id', 'n':'Document Id'}, {'k':'table_id', 'n':'Table Id'}, {'k':'score', 'n':'Score'}, {'k':'color', 'n':'Status', 'type':'SL', 'v_opt':2, 'pin':'pinnedRight'}]
        res = [{'message':'done', 'data':data_lst, 'col_def':col_def_lst, 'map':inf_map, 'no_poss':no_poss_docs, 'one_poss':one_poss_dcos, 'mul_poss':ml_poss_docs}]
        return res

    def read_well_collected_table_info(self, ijson):    
        db_name = ijson.get('db_name', 'AECN_INC')
        company_id = ijson.get('company_id', 'TestDB')
        doc_id  = ijson.get('doc_id')
        table1 = ijson.get('table1')
        db_path = config.Config.doc_wise_db.format(company_id, doc_id)
        if company_id in ('20_118', 'TestDB'):
            db_path = config.Config.test_db.format(doc_id)
        conn, cur = conn_obj.sqlite_connection(db_path)
        read_qry =  """ SELECT t2, score FROM well_connecte_detailsddb WHERE t1='%s'; """%(table1)
        try:
            cur.execute(read_qry)
            t_data = cur.fetchall()
        except:t_data = ()
        conn.close()
        data_lst = []
        inf_map  = {}
        
        for sn, (row_str, score) in enumerate(t_data, 1):
            if company_id in ('20_118', 'TestDB'):
                doc_id, page_no, grid_id, dtp_type = row_str.split('#')
            else:   
                doc_id, page_no, grid_id = row_str.split('_')
            data_info = self.read_gh_info(doc_id, page_no, grid_id, db_name)
            grid_header = ' '.join(data_info) 
            row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn, 'score':{'v':score}, 'table_id':{'v':row_str}, 'GH':{'v':grid_header}}
            if company_id in ('TestDB', '20_118'):
                row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn, 'score':{'v':score}, 'table_id':{'v':'_'.join(row_str.split('#')[:-1])}, 'GH':{'v':grid_header}}
            data_lst.append(row_dct)
            inf_map['%s_%s'%(sn, 'table_id')]  = {'ref_k':row_str}
            inf_map['%s_%s'%(sn, 'score')]  = {'ref_k':row_str}
            inf_map['%s_%s'%(sn, 'GH')]  = {'ref_k':row_str}
            inf_map['%s_%s'%(sn, 'sn')]  = {'ref_k':row_str}
        col_def_lst = []
        if t_data:
            col_def_lst = [{'k':'checkbox', 'n':'checkbox', 'v_opt':3, 'type':'SL'}, {'k':'sn', 'n':'S.No', 'type':'SL', 'w':80}, {'k':'table_id', 'n':'Table Id', 'w':120}, {'k':'score', 'n':'Score', 'type':'SL', 'w':60}, {'k':'GH', 'n':'Description'}]
        return [{'message':'done', 'data':data_lst, 'map':inf_map, 'col_def':col_def_lst}]    
        
    def read_realtion_between_tables(self, ijson):
        company_id = ijson.get('company_id', 'TestDB')
        doc_id  = ijson.get('doc_id')
        table1 = ijson.get('table1')
        table2 = ijson.get('table2')
        db_path = config.Config.doc_wise_db.format(company_id, doc_id)
        if company_id in ('20_118', 'TestDB'):
            db_path = config.Config.test_db.format(doc_id)
        conn, cur = conn_obj.sqlite_connection(db_path)
        read_qry =  """ SELECT detail_info FROM well_connecte_detailsddb WHERE t1='%s' AND t2='%s'; """%(table1, table2)
        try:
            cur.execute(read_qry)
            t_data_str = cur.fetchone()[0]
            t_data_dct = eval(t_data_str)
        except:t_data_dct = {}
        conn.close()
        t1_data = t_data_dct.get(table1, [])
        t2_data = t_data_dct.get(table2, [])
        
        res_dct = {}
        for ix, row_lst in enumerate(t1_data):
            t1_xml = row_lst[2]
            t2_inf = t2_data[ix]
            t2_xml = t2_inf[2]
            res_dct.setdefault(table1, {}).setdefault(t1_xml, []).append({'t':table2, 'x':t2_xml})
        
        res_info_dct = {}
        clr_key = 1
        for t1_t2, inf_dct in res_dct.iteritems():
            for xml_inf, t_inf_lst in  inf_dct.iteritems():
                clr_class = 'heat_map_lg_%s'%(clr_key)
                clr_key += 1
                if clr_key == 52:
                    clr_key = 1
                res_info_dct.setdefault(t1_t2, {})[xml_inf] = {'ref':t_inf_lst, 'clr':clr_class}
                for tt_dct in t_inf_lst:
                    t2_i = tt_dct['t']
                    t2_x = tt_dct['x']
                    res_info_dct.setdefault(t2_i, {})[t2_x]  = {'ref':[{'t':t1_t2, 'x':xml_inf}], 'clr':clr_class}                    
        return [{'message':'done', 'data':res_info_dct}]
    
        
    
    
        
        

            
if __name__ == '__main__':          
    i_Obj = INC_DataBuilder()
    ijson = {'type':'MT', 'doc_id':5709, 'company_id':'20_139', 'table_id':'5709_160_1'}
    #print i_Obj.table_id_poss_with_across_docs(ijson)
    #print i_Obj.data_according_to_table_id(ijson)
    #print i_Obj.doc_wise_well_connected_tables(ijson)
    #print i_Obj.read_distinct_table_types(ijson)
    #print i_Obj.multi_table_module_info(ijson)
