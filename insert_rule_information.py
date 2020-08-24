import os, sys, json, datetime, copy, shelve
import json, binascii

import config
import db.get_conn as get_conn
conn_obj    = get_conn.DB()

class RuleInfo:
        
    def read_doc_type_info(self, company_id):
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'document_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, doc_type FROM document_meta_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        doc_dct = {}
        for row_data in t_data:
            doc_id, doc_type = row_data
            doc_dct[str(doc_id)] = doc_type
        return doc_dct
    
    def insert_rule_populate_info(self, ijson):
        populate_type = ijson.get('type', 'Rule')
        company_id    = ijson['company_id']
        project_id    = ijson['project_id']
        classified_id = ijson['table_type']
        user_name     = ijson['user']
        db_dct = config.Config.populate_db
        for classified_id in [classified_id]:
            m_cur, m_conn = conn_obj.MySQLdb_conn(db_dct)        
            read_qry = """ SELECT row_id, status FROM doc_rule_populate WHERE company_id='{0}' and rule_id={1} and rule_type='RULE'; """.format(company_id, classified_id)
            m_cur.execute(read_qry)
            t_data = m_cur.fetchone()
            print t_data
            r_status    = 'N'
            if t_data and t_data[0]:
                r_status = t_data[1]
            else:   
                t_data  = ()
            if not t_data:
                insert_stmt = """ INSERT INTO doc_rule_populate(company_id, project_id, status, rule_id, rule_type, stage1, stage2, stage3, queue_status, queue_time, user_name) VALUES('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', 'N', Now(), '%s'); """%(company_id, project_id, 'N', classified_id, 'RULE', 'N', 'N', 'N', user_name) 
                print insert_stmt
                m_cur.execute(insert_stmt)
                m_conn.commit()
            elif t_data and (r_status not in ('Y', 'E')):
                insert_stmt = """ UPDATE doc_rule_populate SET queue_status='Q', queue_time=Now(), user_name='%s'  WHERE row_id='%s' """%(user_name, t_data[0])
                m_cur.execute(insert_stmt)
                m_conn.commit()
                print insert_stmt
            elif t_data and (r_status in ('Y', 'E')):
                insert_stmt = """ UPDATE doc_rule_populate SET queue_status='N', status='N', queue_time=Now(), user_name='%s'  WHERE row_id='%s' """%(user_name, t_data[0])
                m_cur.execute(insert_stmt)
                print insert_stmt
                m_conn.commit()
            m_conn.close()
        res = [{'message':'done'}]
        return res
 
                
    def insert_doc_populate_info(self, ijson):
        return self.insert_company_doc_mgmt(ijson)
        company_id    = ijson['company_id']
        doc_lst       = ijson['doc_list']
        user_name     = ijson['user']
        populate_type = ijson.get('type', 'Doc')
        db_dct = config.Config.populate_db
        m_cur, m_conn = conn_obj.MySQLdb_conn(db_dct)        
        
        
        insert_rows_doc = []
        for doc in doc_lst:
            insert_rows_doc.append((company_id, json.dumps(list(set([str(doc)]))), 'N', user_name, populate_type)) 
             
        if insert_rows_doc:
            for r in insert_rows_doc:
                insert_stmt = """ INSERT INTO company_rule_mgmt(company_id, doc_set, status, user_name, populate_type) VALUES('%s', '%s', '%s', '%s', '%s'); """%r
                m_cur.execute(insert_stmt)
            m_conn.commit()

        read_qry = """ SELECT status FROM rule_populate_status WHERE company_id='{0}'; """.format(company_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()
        if t_data:
            r_status = t_data[0]
        if not t_data:
            insert_stmt = """ INSERT INTO rule_populate_status(company_id, status, queue_status, process_time, user_name) VALUES('%s', '%s', '%s', Now(), '%s'); """%(company_id, 'N', 'N', user_name) 
            m_cur.execute(insert_stmt)
        elif t_data and (r_status not in ('Y', 'E')):
            insert_stmt = """ UPDATE rule_populate_status SET queue_status='Q', process_time=Now(), user_name='%s' WHERE company_id='%s' """%(user_name, company_id)
            m_cur.execute(insert_stmt)
        elif t_data and (r_status in ('Y', 'E')):
            insert_stmt = """ UPDATE rule_populate_status SET status='N', queue_status='N', stage1='N', process_time=Now(), user_name='%s' WHERE company_id='%s' """%(user_name, company_id)
            m_cur.execute(insert_stmt)
        m_conn.commit()
        m_conn.close()
        return [{'message':'done'}]


    def insert_company_doc_mgmt(self, ijson):   
        company_id    = ijson['company_id']
        doc_lst       = ijson['doc_list']
        user_name     = ijson['user']
        db_dct = config.Config.populate_db
        m_cur, m_conn = conn_obj.MySQLdb_conn(db_dct)        
        for doc in doc_lst:
            m_cur, m_conn = conn_obj.MySQLdb_conn(db_dct)        
            read_qry = """ SELECT row_id, status FROM doc_rule_populate WHERE company_id='{0}' and rule_id={1} and rule_type='DOC'; """.format(company_id, doc)
            m_cur.execute(read_qry)
            t_data = m_cur.fetchone()
            print t_data
            r_status    = 'N'
            if t_data and t_data[0]:
                r_status = t_data[1]
            else:   
                t_data  = ()
            if not t_data:
                insert_stmt = """ INSERT INTO doc_rule_populate(company_id, status, rule_id, rule_type, stage1, stage2, stage3, queue_status, queue_time, user_name) VALUES('%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', Now(), '%s'); """%(company_id, 'N', doc, 'DOC', 'N', 'N', 'N', 'N', user_name) 
                print insert_stmt
                m_cur.execute(insert_stmt)
                m_conn.commit()
            elif t_data and (r_status not in ('Y', 'E')):
                insert_stmt = """ UPDATE doc_rule_populate SET queue_status='Q', queue_time=Now(), user_name='%s', stage1='N', stage2='N', stage3='N'  WHERE row_id='%s' """%(user_name, t_data[0])
                m_cur.execute(insert_stmt)
                m_conn.commit()
                print insert_stmt
            elif t_data and (r_status in ('Y', 'E')):
                insert_stmt = """ UPDATE doc_rule_populate SET queue_status='N', status='N', queue_time=Now(), user_name='%s', stage1='N', stage2='N', stage3='N'  WHERE row_id='%s' """%(user_name, t_data[0])
                m_cur.execute(insert_stmt)
                print insert_stmt
                m_conn.commit()
            m_conn.close()
        res = [{'message':'done'}]
        return res 

    def insert_company_doc_mgmt_page(self, ijson):   
        company_id    = ijson['company_id']
        doc_page_dct  = ijson.get('doc_page_d', {})
        doc_lst       = ijson.get('doc_list', doc_page_dct.keys())
        doc_page_dct   = {}
        user_name     = ijson['user']
        db_dct = config.Config.populate_db
        m_cur, m_conn = conn_obj.MySQLdb_conn(db_dct)        
        for doc in doc_lst:
            page_dct = doc_page_dct.get(doc, {})
            meta_dct   = {"pages":page_dct}
            meta_json  = json.dumps(meta_dct) 
            m_cur, m_conn = conn_obj.MySQLdb_conn(db_dct)        
            read_qry = """ SELECT row_id, status FROM doc_rule_populate WHERE company_id='{0}' and rule_id={1} and rule_type='DOC'; """.format(company_id, doc)
            m_cur.execute(read_qry)
            t_data = m_cur.fetchone()
            print t_data
            r_status    = 'N'
            if t_data and t_data[0]:
                r_status = t_data[1]
            else:   
                t_data  = ()
            if not t_data:
                insert_stmt = """ INSERT INTO doc_rule_populate(company_id, status, rule_id, rule_type, stage1, stage2, stage3, queue_status, queue_time, user_name, meta_data) VALUES('%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', Now(), '%s', '%s'); """%(company_id, 'N', doc, 'DOC', 'N', 'N', 'N', 'N', user_name, meta_json) 
                print insert_stmt
                m_cur.execute(insert_stmt)
                m_conn.commit()
            elif t_data and (r_status not in ('Y', 'E')):
                insert_stmt = """ UPDATE doc_rule_populate SET queue_status='Q', queue_time=Now(), user_name='%s', stage1='N', stage2='N', stage3='N'  WHERE row_id='%s' """%(user_name, t_data[0])
                m_cur.execute(insert_stmt)
                m_conn.commit()
                print insert_stmt
            elif t_data and (r_status in ('Y', 'E')):
                insert_stmt = """ UPDATE doc_rule_populate SET queue_status='N', status='N', queue_time=Now(), user_name='%s', stage1='N', stage2='N', stage3='N'  WHERE row_id='%s' """%(user_name, t_data[0])
                m_cur.execute(insert_stmt)
                print insert_stmt
                m_conn.commit()
            m_conn.close()
        res = [{'message':'done'}]
        return res 
    
if __name__ == '__main__':
    r_Obj = RuleInfo()
    ijson = {"company_id":1053729, "doc_list":[30], "user":"demo"}
    #r_Obj.insert_company_doc_mgmt(ijson)
    #r_Obj.insert_doc_populate_info(ijson)




