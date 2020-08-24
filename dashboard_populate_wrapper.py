import os, sys, json, datetime

import config
import db.get_conn as get_conn
conn_obj    = get_conn.DB()

class DashBoard(object):

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def get_list_all_companies(self):
        m_cur, m_conn   = conn_obj.MySQLdb_conn(config.Config.company_info_db)
        read_qry = """ SELECT row_id, company_display_name FROM company_mgmt; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        companyName_companyId_map = {}
        for row_data in t_data:
            company_id, company_display_name = row_data
            companyName_companyId_map[str(company_id)] = company_display_name
        return companyName_companyId_map
        
    def read_all_rules(self, m_conn, m_cur):
        read_qry = """ SELECT company_id, COUNT(rule_id) FROM company_rule_mgmt WHERE status='P' AND populate_type='Rule' GROUP BY company_id; """ 
        print read_qry
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()   
        print t_data
        comp_rule_cnt_dct =  {rw[0]:rw[1] for rw in t_data}
        return comp_rule_cnt_dct   

    def get_running_stats(self):
        get_company_names = self.get_list_all_companies()
        clasified_id_dct = self.read_all_tts()
        m_cur, m_conn   = conn_obj.MySQLdb_conn(config.Config.populate_db)
        read_qry = """SELECT row_id, company_id, project_id, rule_id, rule_type, status, queue_status, stage1, stage2, stage3,  queue_time, process_s_time, user_name, error_message, stage0, stage_1 FROM doc_rule_populate ORDER BY row_id DESC; """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        completed_res_lst = []
        #inprocess_dct = {'1_1':{'user_name':'', 'ptime':'', 'Total':'0/19'%(stg_cnt), 'cn':'', 'company_id':1_1, 's_1':'N', 's_2':'N', 's_3':'N', 's_4':'N', 's_5':'N', 's_6':'N', 'sn':1}}
        inprocess_dct = {}
        sn_in = 1
        sn_c  = 0
        map_d   = {
            0: 's_0',
            1: 's_1',
            2: 's_1',
            3: 's_1',
        }
        c_time  = datetime.datetime.now()
        def get_time_diff(t1, t2):
            d   = t2 - t1
            sec = 24 * 60 * 60
            diff    = divmod(d.days * sec + d.seconds, 60)
            return '%s:%s'%diff
            
        for row in t_data:
            stg_dct = {'s_1':[], 's_2':[], 's_3':[], 's_0':[], 's_4':[], 's_5':[]}
            all_stg_info = { 's_1_m':{}, 's_2_m':{}, 's_3_m':{}, 's_0_m':{}, 's_4_m':{}, 's_5_m':{}}
            row_id, company_id, project_id, rule_id, rule_type, status, queue_status, stage1, stage2, stage3,  queue_time, process_s_time, user_name, error_message, stage0, stage_1 = row
            get_comp_name = get_company_names.get(company_id, '')
            pname   = ''
            if rule_type == 'RULE':
                pname   = str(rule_id)+' - '+clasified_id_dct.get(str(rule_id), '')
            elif rule_type == 'DOC':
                pname   = str(rule_id)
                
            if 0:#status == 'Q':
                sn_in += 1
                st_res_dct = {'ptype':rule_type, 'user_name':user_name, 'pname': pname, 'ptime':get_time_diff(queue_time, c_time), 'Total':'%s/4'%(0), 'cn':get_comp_name, 'company_id':company_id, 's_1':'Q', 's_2':'N', 's_3':'N', 's_4':'N', 's_5':'N', 's_6':'N', 'sn':sn_in}
                continue 
            stg_cnt = 0
            #for idx, stg in enumerate([stage_1, stage0, stage1, stage2, stage3]):
            for idx, stg in enumerate([stage1, stage2, stage3]):
                idx += 1
                if status == 'Q':
                    stg = 'Q'
                stg_dct['s_%s'%(idx)].append(stg)
                all_stg_info['s_%s_m'%(idx)]['s%s'%(idx)] = stg
            st_res_dct = {'ptype':rule_type, 'pname': pname, 'user_name':user_name, 'ptime':get_time_diff(process_s_time, c_time), 'cn':get_comp_name, 'company_id':company_id}
            st_res_dct.update(all_stg_info)
            in_stg_cnt = 0
                
            for st, lst in stg_dct.iteritems():
                if 'P' in lst:
                    st_res_dct[st] = 'P'
                if 'Q' in lst:
                    st_res_dct[st] = 'Q'
                elif 'E' in lst:
                    st_res_dct[st] = 'E'
                elif all(k == 'Y' for k in lst):
                    st_res_dct[st] = 'Y'
                    in_stg_cnt += 1
                elif all(k == 'N' for k in lst):
                    st_res_dct[st] = 'N'
            if status  == 'Y':
                sn_c += 1
                st_res_dct['sn'] = sn_c
                completed_res_lst.append(st_res_dct) 
            elif status != 'Y':
                sn_in += 1
                st_res_dct['sn'] = sn_in
                st_res_dct['Total'] = '%s/4'%(str(in_stg_cnt))
                inprocess_dct.setdefault(rule_type, {})[row_id] = st_res_dct
                #inprocess_dct.setdefault('1_1', {})[row_id] = st_res_dct
        return [{'message':'done', 'complete':completed_res_lst, 'inprocess':inprocess_dct}]
 
    def cmd_get_running_stats(self):
        get_company_names = self.get_list_all_companies()
        m_cur, m_conn   = conn_obj.MySQLdb_conn(config.Config.populate_db)
        comp_rule_cnt_dct = self.read_all_rules(m_conn, m_cur)
        print comp_rule_cnt_dct
        read_qry = """SELECT row_id, company_id, status, queue_status, stage1, stage2, stage3, stage5, process_time, user_name, error_message FROM rule_populate_status ORDER BY row_id DESC; """ 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        completed_res_lst = []
        #inprocess_dct = {'1_1':{'user_name':'', 'ptime':'', 'Total':'0/19'%(stg_cnt), 'cn':'', 'company_id':1_1, 's_1':'N', 's_2':'N', 's_3':'N', 's_4':'N', 's_5':'N', 's_6':'N', 'sn':1}}
        inprocess_dct = {}
        sn_in = 1
        sn_c  = 0
        map_d   = {
            0: 's_0',
            1: 's_1',
            2: 's_1',
            3: 's_1',
        }
        for row in t_data:
            stg_dct = {'s_1':[], 's_2':[], 's_3':[], 's_0':[]}
            all_stg_info = { 's_1_m':{}, 's_2_m':{}, 's_3_m':{}, 's_0_m':{}}
            row_id, company_id, status, queue_status, stage1, stage2, stage3, stage5, process_time, user_name, error_message = row
            rule_cnt = str(comp_rule_cnt_dct.get(company_id, 0))
            get_comp_name = get_company_names.get(company_id, '')
            if status == 'Q':
                sn_in += 1
                st_res_dct = {'user_name':user_name, 'ptime':str(process_time), 'Total':'%s/4'%(0), 'cn':get_comp_name, 'company_id':company_id, 's_1':'Q', 's_2':'N', 's_3':'N', 's_4':'N', 's_5':'N', 's_6':'N', 'sn':sn_in}
                continue 
            stg_cnt = 0
            for idx, stg in enumerate([stage1, stage2, stage3, stage5]):
                if stg == None:
                    stg = 'N'
                if idx in (0,):
                    stg_dct['s_0'].append(stg)
                    all_stg_info['s_0_m']['s%s'%(idx)] = stg
                elif idx in (1,):
                    stg_dct['s_1'].append(stg)
                    all_stg_info['s_1_m']['s%s'%(idx)] = stg
                elif idx in (2,):
                    stg_dct['s_2'].append(stg)
                    all_stg_info['s_2_m']['s%s'%(idx)] = stg
                elif idx in (3,):
                    stg_dct['s_3'].append(stg)
                    all_stg_info['s_3_m']['s%s'%(idx)] = '{0}({1})'.format(stg, rule_cnt) if stg=='P' else stg
                if stg == 'Y':
                    stg_cnt += 1        
            st_res_dct = {'user_name':user_name, 'ptime':str(process_time), 'Total':'%s/4'%(stg_cnt), 'cn':get_comp_name, 'company_id':company_id}
            st_res_dct.update(all_stg_info)
            in_stg_cnt = 0
            for st, lst in stg_dct.iteritems():
                if 'P' in lst:
                    st_res_dct[st] = 'P'
                if 'E' in lst:
                    st_res_dct[st] = 'E'
                elif all(k == 'Y' for k in lst):
                    st_res_dct[st] = 'Y'
                    in_stg_cnt += 1
                elif all(k == 'N' for k in lst):
                    st_res_dct[st] = 'N'
            if status  == 'Y':
                sn_c += 1
                st_res_dct['sn'] = sn_c
                completed_res_lst.append(st_res_dct) 
            elif status != 'Y':
                sn_in += 1
                st_res_dct['sn'] = sn_in
                st_res_dct['Total'] = '%s/4'%(str(in_stg_cnt))
                inprocess_dct[company_id] = st_res_dct
        return [{'message':'done', 'complete':completed_res_lst, 'inprocess':{'1_1':inprocess_dct}}]
        
    def read_table_types(self):
        db_path = config.Config.gl_db
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT row_id, table_type FROM all_table_types; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        table_name_dct = {}
        for row_id, table_type in t_data:
            table_name_dct[str(row_id)] = table_type
        return table_name_dct
        
    def read_all_tts(self):
        db_path = '/mnt/eMB_db/company_management/global_info.db' 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        r_qry = """ SELECT row_id, table_type, short_form FROM all_table_types; """
        cur.execute(r_qry)
        mst_data = cur.fetchall()
        conn.close()
        clasified_id_dct = {} 
        for row_data in mst_data:
            row_id, table_type, short_form = row_data
            clasified_id_dct[str(row_id)] = table_type
        return clasified_id_dct
        
    def rule_stats(self, ijson):
        company_id = ijson['company_id']
        clasified_id_dct = self.read_all_tts()
        m_cur, m_conn   = conn_obj.MySQLdb_conn(config.Config.populate_db)
        read_qry = """ SELECT row_id, rule_id, stage1, stage2, stage3 FROM company_rule_mgmt WHERE company_id='{0}' AND (stage1 in ('P', 'E', 'N', 'Y') or stage2 in ('P', 'E', 'N', 'Y') or stage3 in ('P', 'E', 'N', 'Y')); """.format(company_id)
        read_qry = """ SELECT row_id, rule_id, stage1, stage2, stage3 FROM company_rule_mgmt WHERE company_id='{0}'; """.format(company_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        
        rule_info_dct = {}
        r_d = {}
        for row_data in t_data:
            row_id, rule_id, stage1, stage2, stage3 = row_data
            if stage3 == 'Y':continue
            r_d.setdefault(rule_id, []).append(row_data)
        #for row_data in t_data:
        #    row_id, rule_id, stage1, stage2, stage3 = row_data
        #    if stage3 == 'Y':continue
        for k, v in r_d.items():
            if len(v) == 1:
                v   = v[0]
            else:
                pv  = filter(lambda x: 'Y' in x[2:], v)
                if pv:
                    v   = pv[-1]
                else:
                    v   = v[-1]
            row_id, rule_id, stage1, stage2, stage3 =  v
            #print k, v
            rule_info_dct[rule_id] = (stage1, stage2, stage3)
            #rule_info_dct.setdefault(rule_id, {})['stage1'] = stage1
            #rule_info_dct.setdefault(rule_id, {})['stage2'] = stage2
            #rule_info_dct.setdefault(rule_id, {})['stage3'] = stage3
            
        rule_stage_dct = {}
        for rl, stg_tup in rule_info_dct.iteritems():
            rule_id = rl
            st1 , st2, st3 = stg_tup
            #if st1 == 'Y' and st2 == 'Y' and st3 == 'Y':continue
            rule_stage_dct.setdefault(rule_id, {'S':{}})['S']['s1'] = st1
            rule_stage_dct.setdefault(rule_id, {})['S']['s2'] = st2
            rule_stage_dct.setdefault(rule_id, {})['S']['s3'] = st3
            if 'E' in  (st1, st2, st3):
                rule_stage_dct[rule_id]['o'] = 0
            elif 'P' in  (st1, st2, st3):
                rule_stage_dct[rule_id]['o'] = 1
            else:
                rule_stage_dct[rule_id]['o'] = 3
            rule_stage_dct.setdefault(rule_id, {})['a_s'] = (st1, st2, st3)
 
        data_lst = []
        sn = 1        
        r_ids   = rule_stage_dct.keys()
        r_ids.sort(key=lambda x: rule_stage_dct[x]['o'])
        for r_id in r_ids: #, stage_dct in rule_stage_dct.iteritems():
            stage_dct   = rule_stage_dct[r_id]
            class_txt = clasified_id_dct.get(r_id, '')
            row_dct = {'rid':sn, 'cid':sn, 'sn':{'v':sn}, 'rule_id':{'v':'{0}-{1}'.format(r_id, class_txt)}}
            for stage, status in stage_dct['S'].iteritems():
                if status == 'Y':
                    status = 'G'
                elif status == 'P':
                    status = 'O'
                elif status == 'E':
                    status = 'R'
                row_dct[stage] = {'v':status}
            data_lst.append(row_dct)
            sn += 1

        map_inf = {}
        col_def_lst = [{'k':'sn', 'n':'S.No', 'pin':'pinnedLeft', 'type':'SL'}, {'k':'rule_id', 'n':'Rule Id'}, {'k':'s1', 'n':'Stage1', 'v_opt':2}, {'k':'s2', 'n':'Stage2', 'v_opt':2}, {'k':'s3', 'n':'Stage3', 'v_opt':2}]    
        res = [{'message':'done', 'data':data_lst, 'col_def':col_def_lst, 'map':map_inf}]
        return res

    def doc_stats(self, ijson):
        company_id = ijson['company_id']
        m_cur, m_conn   = conn_obj.MySQLdb_conn(config.Config.populate_db)
        read_qry = """ SELECT row_id, doc_set, stage1, stage2, stage3 FROM company_doc_mgmt WHERE company_id='{0}' AND (stage1 in ('P', 'E', 'N') or stage2 in ('P', 'E', 'N') or stage3 in ('P', 'E', 'N')); """.format(company_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        
        doc_info_dct = {}
        for row_data in t_data:
            row_id, doc_set, stage1, stage2, stage3 = row_data
            doc_lst = json.loads(doc_set)
            for doc in doc_lst:
                doc_info_dct.setdefault(doc, {})['stage1'] = stage1
                doc_info_dct.setdefault(doc, {})['stage2'] = stage2
                doc_info_dct.setdefault(doc, {})['stage3'] = stage3
 
        data_lst = []
        sn = 1        
        for doc_id, stage_dct in doc_info_dct.iteritems():
            row_dct = {'rid':sn, 'cid':sn, 'sn':{'v':sn}, 'doc_id':{'v':doc_id}}
            for stage, status in stage_dct.iteritems():
                if status == 'Y':
                    status = 'G'
                elif status == 'P':
                    status = 'O'
                elif status == 'E':
                    status == 'R'
                row_dct[stage] = {'v':status}
            data_lst.append(row_dct)
            sn += 1

        map_inf = {}
        col_def_lst = [{'k':'sn', 'n':'S.No', 'pin':'pinnedLeft', 'type':'SL'}, {'k':'doc_id', 'n':'Doc Id'}, {'k':'stage1', 'n':'Stage1', 'v_opt':2}, {'k':'stage2', 'n':'Stage2', 'v_opt':2}, {'k':'stage3', 'n':'Stage3', 'v_opt':2}]    
        res = [{'message':'done', 'data':data_lst, 'col_def':col_def_lst, 'map':map_inf}]
        return res
        
if __name__ == '__main__':
    obj = DashBoard() 
    #print obj.get_running_stats()
    ijson = {'company_id':1053729}
    print obj.doc_stats(ijson)
