import os, sys, json
import config

import db.get_conn as get_conn
conn_obj    = get_conn.DB()

class INC_Company_Mgmt(object):
        
    def __init__(self):
        self.tt_order = {
                        "IS":1,
                        "CF":2,
                        "BS":3, 
                        "RBS":4,
                        "RBG":5    
                    }    
    def read_country(self, m_cur, row_id):
        read_qry = """ SELECT country FROM country WHERE id=%s; """%(row_id)
        m_cur.execute(read_qry)
        mt_data = m_cur.fetchone()
        country = ''
        if mt_data:
            country = mt_data[0]
        return country

    def read_company_list(self):
        m_cur, m_conn = conn_obj.MySQLdb_conn(config.Config.company_info_db) 
        read_qry = """ SELECT cm.row_id, cm.company_display_name, tk.ticker FROM company_mgmt AS cm INNER JOIN Ticker AS tk ON cm.row_id=tk.company_id; """
        m_cur.execute(read_qry)
        mt_data = m_cur.fetchall()
        data = [{'k':es[0], 'n':es[1], 't':es[2], 'c':self.read_country(m_cur, es[0])} for es in mt_data]   
        m_conn.close()
        res = [{'message':'done', 'data':data}]
        return res
        
    def read_distinct_table_types_data(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        cid_path = config.Config.company_id_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT distinct(classified_id) FROM table_mgmt WHERE doc_id='%s'; """%(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        #cid_path = config.Config.company_id_path.format(company_id)
        #db_path = os.path.join(cid_path, 'table_info.db')
        tt_ids_str = ', '.join([str(es[0]) for es in t_data if es[0]]) 

        db_path = '/mnt/eMB_db/company_management/global_info.db' 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        r_qry = """ SELECT row_id, table_type, short_form FROM all_table_types WHERE row_id in (%s); """%(tt_ids_str)
        cur.execute(r_qry)
        mst_data = cur.fetchall()
        conn.close()
        #data = [{'k':est[0], 'n':est[2], 'ex':est[1]} for est in mst_data]
        #data.sort(key=lambda x:self.tt_order.get('n', 999999))
        max_value = max(self.tt_order.values()) + 1
        data_lst = []
        for ix, est in enumerate(mst_data, max_value):
            k, ex, n = est
            ordr = self.tt_order.get(n, ix)
            dt_dct = {'k':k, 'ex':ex, 'n':n, 'ix':ordr}
            data_lst.append(dt_dct)
        data_lst.sort(key=lambda x:x['ix'])   
        res = [{'message':'done', 'data':data_lst}]
        return res
        
    def ref_path_info_workspace(self):
        path1   = '/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/'
        ref_path    = {
                        'ref_html':'%s/{0}/html/{1}_celldict.html'%(path1.replace('/var/www/html', '')),
                        'ref_pdf':'/pdf_canvas/viewer.html?file=%s{0}/pages/{1}.pdf'%(path1),
                        }
        return ref_path
        
    def read_document_meta_data(self, ijson):
        company_id = ijson['company_id']
        cid_path = config.Config.company_id_path.format(company_id)
        db_path = os.path.join(cid_path, 'document_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, doc_name, doc_type, period_type, period, filing_type FROM document_meta_info;  """        
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        doc_lst = []
        for row_data in t_data:
            doc_id, doc_name, doc_type, period_type, period, filing_type = row_data
            ph  = '{0}{1}'.format(period_type, period)
            nm = '%s-%s'%(doc_id, doc_name)
            dt_dct = {'k':doc_id, 'n':nm, 'dt':doc_type, 'ph':ph, 'ft':filing_type}
            doc_lst.append(dt_dct)
        res = [{'message':'done', 'data':doc_lst}]
        return res
        
    def read_json_file(self, table_id, company_id):
        json_dir = config.Config.table_json_path.format(company_id)
        json_file_path = os.path.join(json_dir, '{0}.json'.format(table_id))        
        json_dct = {}
        with open(json_file_path, 'r') as j:
            json_dct = json.load(j)
        return json_dct

    def table_wise_grid_info(self, row_no, table_id, company_id, pageno, doc, snip_dct, res_formula_dct, gridid, ref_path, hglt_dct):
        print hglt_dct
        tid = '_'.join(map(str, (doc, pageno, gridid)))
        g_data = self.read_json_file(tid, company_id)
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
        rc_cnt = 0
        for row in rows:
            cols    = rc_d[row].keys()
            cols.sort()
            row_dct = {'sn':row_no, 'cid':row_no, 'rid':row_no}
            level_id = 0
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
                if not level_id:
                    lid = cell.get('level_id', 0)
                    if lid:
                        level_id = len(lid.split('.')) - 1
                xml_lst = cell['xml_ids'].split('$$')
                bbox = cell['bbox']
                txt  = cell['data']
                rs   = cell['rowspan']
                cs   = cell['colspan']
                if stype == 'HGH' and not txt:continue
                if stype not in ('HGH', 'GV'):continue
                dd  = {'v':txt}
                rc_st = '%s_%s'%(row, col)
                snip_inf = snip_dct.get(rc_st, [])
                table1_res_inf = res_formula_dct.get(rc_st, '')
                if table1_res_inf:
                    dd['f'] = 'Y' 
           
                col_dct[col] = 1
                inf_map['%s_%s'%(row_no, col)] = {'ref':[{'xml_list':xml_lst,'pno':pageno,'d':doc,'bbox':self.get_bbox(bbox)}], 'cs':cs, 'rs':rs, 'row':row, 'col':col, 'st':stype, 'ref_k':(doc, pageno, gridid, table_id, '%s_%s'%(row, col))}
                if snip_inf:
                    dd['sr'] = 'Y' 
                    dd['ax'] = 'Y'
                    inf_map['%s_%s'%(row_no, col)]['grp_lst'] = snip_inf
                hd_clr  =  hglt_dct.get('%s_%s'%(row, col), 0)
                if hd_clr:
                    dd['cls'] = 'Equality' 
                 
                row_dct[col] = dd
            if 0 not in row_dct:continue
            row_dct['$$treeLevel'] = level_id
            row_lst.append(row_dct)
            row_no += 1
        return row_lst, inf_map, col_dct, row_no
        
    def get_bbox(self, bbox): 
        bbox_lst = bbox.split('$$') 
        bl = []
        for bx in bbox_lst:#xmin_ymin_xmax_ymax
            if bx:
                b = map(lambda x:int(x), bx.split('_'))
                x   = b[0]
                y   = b[1]
                w   = b[2] - b[0]
                h   = b[3] - b[1]
                bl.append([x, y, w, h])
        return bl
        
    def find_alp(self, dec_val):
        if(dec_val<26):
            return chr(65+dec_val)
        if(dec_val > 25 and dec_val < 52):
            return chr(65)+chr(65+(dec_val-26))
        if(dec_val > 51 and dec_val < 78):
            return chr(66)+chr(65+(dec_val-52))
        if(dec_val > 77 and dec_val < 104):
            return chr(67)+chr(65+(dec_val-78))
        return "@@"
             
    def read_all_axis_snipet_info(self, company_id, doc_id, group_lst):
        tab_lets_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(tab_lets_path, '{0}.db'.format(doc_id))
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT row, col, groupid, celltype, gridids, res_opr, datatype FROM rawdb WHERE datatype='Table1'; """  
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        r_map_dct = {}
        r_formula_dct = {}
        for row_data in t_data:
            row, col, groupid, celltype, gridids, res_opr, datatype = row_data
            did, page, grid, dmy = gridids.split('#')
            did, page, grid = map(int, (did, page, grid))
            if (datatype=='Table1') and (res_opr=='R'):
                r_formula_dct.setdefault((did, page, grid), {})[celltype] = groupid
        return {}, r_formula_dct 
        
    def read_left_formula_info(self, company_id, doc_id, group_lst):
        tab_lets_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(tab_lets_path, '{0}.db'.format(doc_id))
        conn, cur   = conn_obj.sqlite_connection(db_path) 
        read_qry = """ SELECT table_id1, r_c1, fid1 FROM equalityInfo WHERE vType1='R'; """
        if group_lst:
            read_qry = """ SELECT table_id2, r_c2, fid2 FROM equalityInfo WHERE vType2='R'; """
        cur.execute(read_qry)
        t_data = cur.fetchall() 
        conn.close()
        r_formula_dct = {}
        for row_data in t_data:
            (did, page, grid), r_c, fid = map(int, row_data[0].split('#')), row_data[1], row_data[2]  
            r_formula_dct.setdefault((did, page, grid), {})[r_c] = fid
        return {}, r_formula_dct
            
    def read_only_axis_snipet(self, cur, conn, doc, page, grid):
        dpg_str = '#'.join(map(str, [doc, page, grid]))
        read_qry = """ SELECT r_c1, eid FROM equalityInfo WHERE table_id1='%s'; """%(dpg_str)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        snipet_dct = {}
        for row_data in t_data:
            rc, eid = row_data
            snipet_dct.setdefault(rc, []).append(eid)
        return snipet_dct   
        
    def read_desired_tables(self, a_conn, a_cur, doc_id, page_no, grid_no):
        read_qry = """ SELECT row_id FROM table_mgmt WHERE doc_id=%s AND page_no=%s AND grid_id=%s;  """%(doc_id, page_no, grid_no) 
        a_cur.execute(read_qry)
        t_data = a_cur.fetchone()
        row_id = 0
        if t_data:
            row_id = t_data[0]
        return row_id             
    
    def read_axis_data(self, grp_lst, a_cur, a_conn, company_id, doc_id):
        equality_path  = config.Config.equality_path.format(company_id)
        db_path = os.path.join(equality_path, '{0}.db'.format(doc_id))
        conn, cur   = conn_obj.sqlite_connection(db_path)
        page_coord_dct = self.read_page_coods(cur)
        grp_str = ', '.join(['"'+str(es)+'"' for es in grp_lst])
        read_qry = """ SELECT table_id2, r_c2 FROM equalityInfo WHERE eid in (%s); """%(grp_str)
        print read_qry
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        table_dct = {}
        table_lst = []
        table_set = set()
        for row_data in t_data:
            (did, page, grid), rc = map(int, row_data[0].split('#')), row_data[1]   
            table_dct.setdefault((did, page, grid), {})[rc] = 1
            table_id = self.read_desired_tables(a_conn, a_cur, did, page, grid)
            table_set.add((table_id, did, page, grid)) 
            table_lst.append((table_id, did, page, grid))
        print 'TTTTTTTTTTTTTTTTT', table_lst
        return table_dct, table_set, page_coord_dct
        
    def read_page_coods(self, cur):
        read_qry = """ SELECT pageno, pagesize FROM pagedet; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        page_dct = {}
        for row_data in t_data:
            page_no, coords = row_data
            coords = eval(coords)
            page_dct[page_no] = coords
        return page_dct
 
    def read_grid_information(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        table_type_id = ijson['tt_id']
        grp_lst   = ijson.get('axis_key', [])
        right_table_hglt_dct = {}
        cid_path = config.Config.company_id_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        if not grp_lst:
            read_qry = """ SELECT row_id, doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id=%s AND classified_id=%s; """%(doc_id, table_type_id)
            cur.execute(read_qry)
            t_data = cur.fetchall()
        if grp_lst:
            right_table_hglt_dct, t_data, page_coord_dct = self.read_axis_data(grp_lst, cur, conn, company_id, doc_id)
        conn.close()
        
        #snipet_dct, res_formula_dct = self.read_all_axis_snipet_info(company_id, doc_id, grp_lst)
        snipet_dct, res_formula_dct  = self.read_left_formula_info(company_id, doc_id, grp_lst)
        ref_path = self.ref_path_info_workspace()
        
        if not grp_lst: 
            equality_path  = config.Config.equality_path.format(company_id)
            db_path = os.path.join(equality_path, '{0}.db'.format(doc_id))
            a_conn, a_cur   = conn_obj.sqlite_connection(db_path)
            page_coord_dct = self.read_page_coods(a_cur)
        
        all_rows_lst = [] 
        all_col_def = {}
        all_row_inf = {'ref_path':ref_path}
        row_number = 1
        for row_data in t_data:
            table_id, doc_id, page_no, grid_id = row_data
            emp_dct = {'sn':row_number, 'cid':row_number, 'rid':row_number, 0:{'v':'%s-(%s, %s, %s)'%(table_id, doc_id, page_no, grid_id), 'cls':'Header'}}
            all_rows_lst.append(emp_dct)
            row_number += 1
            snip_dct = {}
            if not grp_lst:
                snip_dct = self.read_only_axis_snipet(a_cur, a_conn, doc_id, page_no, grid_id)
                 
            formula_dct = res_formula_dct.get((doc_id, page_no, grid_id), {})
            hglt_dct = right_table_hglt_dct.get((doc_id, page_no, grid_id), {})
            row_lst, inf_map, col_dct, row_number = self.table_wise_grid_info(row_number, table_id, company_id, page_no, doc_id, snip_dct, formula_dct, grid_id, ref_path, hglt_dct)
            all_rows_lst += row_lst
            all_row_inf.update(inf_map)
            all_col_def.update(col_dct)
            emp_dct = {'sn':row_number, 'cid':row_number, 'rid':row_number}
            all_rows_lst.append(emp_dct)
            row_number += 1
        if not grp_lst:
            a_conn.close() 

        col_def_lst = [{'k':'checkbox', 'n':'', 'v_opt':3}] 
        all_col_def = sorted(all_col_def, key=lambda x:int(x))
        for col_id in all_col_def:
            col_name = self.find_alp(int(col_id)-1)
            dt_dct = {'k':col_id, 'n':col_name, 'col_type':'GV', 'w':80}     
            if col_id == 0:
                dt_dct = {'k':col_id, 'n':'Description', 'col_type':'HGH', 'w':265, 'v_opt':1}     
            col_def_lst.append(dt_dct)  
        all_row_inf['coords'] = page_coord_dct
        res = [{'message':'done', 'data':all_rows_lst, 'col_def':col_def_lst, 'map':all_row_inf}] 
        return res
                
if __name__ == '__main__':
    ic_Obj = INC_Company_Mgmt()
    ##ijson = {"company_id":"1015", "db_name":"AECN_INC", "doc_ids":[13499, 2313, 2285, 2298]}    
    ijson = {"company_id":"1117", "db_name":"AECN_INC", "doc_id":5131, "tt_id":3}    
    print ic_Obj.read_grid_information(ijson)
    ##print ic_Obj.read_document_meta_data(ijson)
    ##print ic_Obj.read_distinct_table_types(ijson)
    ##print ic_Obj.populate_table_information(ijson)
    ##print ic_Obj.read_company_list() 
