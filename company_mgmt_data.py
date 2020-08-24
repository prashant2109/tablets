import os, sys, json, datetime, copy, shelve
import json, binascii

import MySQLdb

import config
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
from collections import OrderedDict as OD, defaultdict
from itertools import permutations

import report_year_sort as rys
        
import redis_api as pre

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__


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

    def connect_to_sqlite(self, db_path):
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        return conn, cur

    def read_company_list(self):
        m_cur, m_conn = conn_obj.MySQLdb_conn(config.Config.company_info_db) 
        read_qry = """ SELECT cm.row_id, cm.company_display_name, '' FROM company_mgmt AS cm INNER JOIN client_details AS tk ON cm.row_id=tk.company_id and tk.project_id not in (3,4); """
        m_cur.execute(read_qry)
        mt_data = m_cur.fetchall()
        data = [{'k':es[0], 'n':es[1], 't':es[2], 'c':self.read_country(m_cur, es[0])} for es in mt_data]   
        #read_qry = """ SELECT row_id, company_display_name, '' FROM company_mgmt where row_id not in (select company_id from client_details); """
        #m_cur.execute(read_qry)
        #mt_data = m_cur.fetchall()
        #data = [{'k':es[0], 'n':es[1], 't':es[2], 'c':self.read_country(m_cur, es[0])} for es in mt_data]   
        m_conn.close()
        res = [{'message':'done', 'data':data}]
        return res
        
    def read_distinct_table_types_data(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        project_id  = ijson['project_id']
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT distinct(classified_id) FROM table_mgmt WHERE doc_id='%s' AND highly_connected='Y'; """%(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        tt_ids_str = ', '.join([str(es[0]) for es in t_data if es[0]]) 

        db_path = '/mnt/eMB_db/company_management/global_info.db' 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        r_qry = """ SELECT row_id, table_type, short_form FROM all_table_types WHERE row_id in (%s); """%(tt_ids_str)
        cur.execute(r_qry)
        mst_data = cur.fetchall()
        conn.close()
        max_value = max(self.tt_order.values()) + 1
        data_lst = []
        for ix, est in enumerate(mst_data, max_value):
            k, ex, n = est
            ordr = self.tt_order.get(n, ix)
            dt_dct = {'k':k, 'ex':n, 'n':ex, 'ix':ordr}
            data_lst.append(dt_dct)
        data_lst.sort(key=lambda x:x['ix'])   
        res = [{'message':'done', 'data':data_lst}]
        return res
        
    def ref_path_info_workspace(self, company_id):
        if str(company_id) == '1117':
            path1   = '/var_html_path/WorkSpaceBuilder_DB/34/1/pdata/docs/'
        else:
            path1   = '/var_html_path/WorkSpaceBuilder_DB/%s/1/pdata/docs/'%(company_id)
        ref_path    = {
                        #'ref_html':'%s/{0}/html_output/{1}.html'%(path1.replace('/var/www/html', '')),
                        'ref_html':'%s/{0}/html/{1}.html'%(path1.replace('/var/www/html', '')),
                        'ref_pdf':'/pdf_canvas/viewer.html?file=%s{0}/pages/{1}.pdf'%(path1),
                        }
        return ref_path
        
    
    def read_page_coods_34(self, company_id, doc_id):
        db_path = config.Config.equality_db_path.format(company_id, doc_id) 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT pageno, pagesize FROM pagedet; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        page_dct = {}
        for row_data in t_data:
            page_no, coords = row_data
            coords = eval(coords)
            page_dct[page_no] = coords
        return page_dct
        

    def read_document_meta_data(self, ijson):
        company_id = ijson['company_id']
        inc_project_id  = '34'
        if str(company_id) not in {'1117':1}:
            inc_project_id  = company_id
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'document_info.db')
        try:
            conn, cur   = conn_obj.sqlite_connection(db_path)
            read_qry = """ SELECT doc_id, doc_name, doc_type, period_type, period, filing_type FROM document_meta_info;  """        
            cur.execute(read_qry)
            t_data = cur.fetchall()
            conn.close()
        except:t_data = ()
        doc_lst = []
        all_phs = {}
        for row_data in t_data:
            doc_id, doc_name, doc_type, period_type, period, filing_type = row_data
            try:
                page_coords = self.read_page_coods_34(company_id, doc_id)
            except:page_coords = {}
            ph  = '{0}{1}'.format(period_type, period)
            all_phs[ph] = 1
            nm = '%s-%s'%(doc_id, doc_name)
            if doc_type and 'html' in doc_type.lower():
                doc_type    = 'html'
            dt_dct = {'k':doc_id, 'n':nm, 'dt':doc_type, 'ph':ph, 'ft':filing_type, 'p_c':page_coords}
            #if doc_type == 'html':
            #    dt_dct['path']  = '/var_html_path/WorkSpaceBuilder_DB/%s/1/pdata/docs/{0}/html/{0}_body.html'%(inc_project_id)
            #elif doc_type.lower() == 'csv':
            #    dt_dct['path']  = '/var_html_path/WorkSpaceBuilder_DB/%s/1/pdata/docs/{0}/sieve_input/{0}.csv'%(inc_project_id)
            dt_lwr = doc_type.lower()
            if dt_lwr in ('html', 'csv', 'xls', 'xlsx'):
                dt_dct['path']  = '/var_html_path/WorkSpaceBuilder_DB/%s/1/pdata/docs/{0}/sieve_input/{0}.%s'%(inc_project_id, dt_lwr)
            else:
                dt_dct['path']  = '/var_html_path/WorkSpaceBuilder_DB/%s/1/pdata/docs/{0}/sieve_input/{0}.pdf'%(inc_project_id)
            doc_lst.append(dt_dct)
    
        ph_lst = all_phs.keys()
        ph_sort = rys.year_sort(ph_lst)
        doc_lst.sort(key=lambda x:ph_sort.index(x['ph']))
        doc_lst.reverse()
        ref_path = self.ref_path_info_workspace(company_id)
        res = [{'message':'done', 'data':doc_lst, 'ref_path':ref_path}]
        return res
        
    def read_json_file(self, table_id, company_id):
        json_dir = config.Config.table_json_path.format(company_id)
        json_file_path = os.path.join(json_dir, '{0}.json'.format(table_id))        
        json_dct = {}
        with open(json_file_path, 'r') as j:
            json_dct = json.load(j)
        return json_dct

    def table_wise_grid_info(self, row_no, table_id, company_id, pageno, doc, snip_dct, res_formula_dct, gridid, ref_path, hglt_dct, rel_info, clr_max, cell_fids, formula_dist_rcs={}):
        #g_data = self.read_json_file(table_id, company_id)
        d_p_g = '{0}_{1}_{2}'.format(doc, pageno, gridid)
        #print 'PPPPPPPPPPPPPPP', d_p_g
        g_data = self.read_json_file(d_p_g, company_id)
        #print 'RRRRRRRRRRRRRRRRR', rel_info
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
        col_da_d    = {}
        for row in rows:
            cols    = rc_d[row].keys()
            cols.sort()
            row_dct = {'sn':row_no, 'cid':row_no, 'rid':row_no}
            level_id = 0
            rt_dct = {}
            l_id = 0
            c_i_dct = {}
            for col in cols:    
                print [row, col]
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
                #if stype == 'HGH' and not txt:continue
                if stype not in ('HGH', 'GV'):continue
                dd  = {'v':txt}
                print 'RRRRRRRRRRRRRRRRRRRRRR', [row, col, dd]
                rc_st = '%s_%s'%(row, col)
                snip_inf = snip_dct.get(rc_st, [])
                table1_res_inf = res_formula_dct.get(rc_st, '')
                fids = cell_fids.get(rc_st, '')
                if fids:
                    fids = '~'.join(fids)
                rel_type = rel_info.get(rc_st, {})
                if table1_res_inf:
                    dd['f'] = 'Y' 
                if rel_type:
                    dd['rt'] = rel_type
                    rt_dct.update(rel_type)
                    row_dct['da']   = 'Y'
                    col_da_d[col]   = 1
           
                col_dct[col] = 1
                c_i_dct[col] = 1
                inf_map['%s_%s'%(row_no, col)] = {'bbox':self.get_bbox(bbox), 'st':stype, 'ref_k':(doc, pageno, gridid, table_id, '%s_%s'%(row, col)), 'g':gridid, 'rc':'%s_%s'%(row, col), 'p':pageno,'d':doc, 'ctype':''}
                if snip_inf:
                    dd['sr'] = 'Y' 
                    dd['ax'] = 'Y'
                    inf_map['%s_%s'%(row_no, col)]['grp_lst'] = snip_inf
                hd_clr  =  hglt_dct.get('%s_%s'%(row, col), 0)
                if hd_clr:
                    dd['cls'] = 'Equality' 
                lid = cell.get('level_id', 0)
                if rc_st in formula_dist_rcs:
                    get_cls = dd.get('cls')
                    if get_cls:
                        crt_cls = ' '.join([get_cls, 'resultant-formula'])
                        dd['cls'] = crt_cls
                    else:
                        dd['cls'] = 'resultant-formula'
                if lid:
                    l_id = lid
                    level_id = len(lid.split('.')) - 1
                row_dct[col] = dd
            if 0 not in row_dct:continue
            row_dct['$$treeLevel'] = level_id
            if rt_dct:
                row_dct['rt'] = rt_dct
                rel_cnt_set = {es.split('_')[0] for es in rt_dct}
                clr_max = max(clr_max, len(rel_cnt_set)) 
                
            for c_ol in c_i_dct:
                inf_map['%s_%s'%(row_no, c_ol)]['l_id'] = level_id 
            row_lst.append(row_dct)
            row_no += 1
        return row_lst, inf_map, col_dct, row_no, clr_max, col_da_d
        
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
        
    def read_left_formula_info(self, company_id, doc_id, group_lst, table_id):
        tab_lets_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(tab_lets_path, '{0}.db'.format(doc_id))
        print db_path
        conn, cur   = conn_obj.sqlite_connection(db_path) 
        print 'IN 1'
        try:
            alter_stmt = """ ALTER TABLE equalityInfo ADD COLUMN fid1 TEXT; """
            cur.execute(alter_stmt)
            alter_stmt = """ ALTER TABLE equalityInfo ADD COLUMN fid2 TEXT; """
            cur.execute(alter_stmt)
        except:s = ''
            
        t_data  = ()
        if group_lst:
            for tid in group_lst:
                read_qry = """ SELECT table_id1, r_c1, fid1, relationship_Type, vType1 FROM equalityInfo where table_id1='%s'"""%(tid)
                try:
                    cur.execute(read_qry)
                    tmp_data = cur.fetchall() 
                    t_data  += tmp_data
                except:tmp_data = ()
        else:
            read_qry = """ SELECT table_id1, r_c1, fid1, relationship_Type, vType1 FROM equalityInfo where table_id1='%s'"""%(table_id)
            print read_qry
            try:
                cur.execute(read_qry)
                tmp_data = cur.fetchall() 
                t_data  = tmp_data
            except:tmp_data = ()
        conn.close()
        r_formula_dct = {}
        rel_type_dct = {}
        collect_fids = {}
        print 'IN L ', len(t_data)
        for row_data in t_data:
            (did, page, grid), r_c, fid, relationship_Type, vType2 = map(int, row_data[0].split('#')), row_data[1], row_data[2], row_data[3], row_data[4]
            if not relationship_Type:continue
            if group_lst:
                relationship_Type = ''  
                #if fid.split('_')[1].lower() == 's':continue 
            rel_type_dct.setdefault((did, page, grid), {}).setdefault(r_c, {})[relationship_Type] = 1
            collect_fids.setdefault((did, page, grid), {}).setdefault(r_c, []).append(fid)
            if vType2 == 'R': 
                r_formula_dct.setdefault((did, page, grid), {})[r_c] = fid
        print 'IN L ', len(t_data)
        return {}, r_formula_dct, rel_type_dct, collect_fids
            
    def read_only_axis_snipet(self, cur, conn, doc, page, grid):
        dpg_str = '#'.join(map(str, [doc, page, grid]))
        read_qry = """ SELECT r_c1, eid FROM equalityInfo WHERE table_id1='%s'; """%(dpg_str)
        try:
            cur.execute(read_qry)
            t_data = cur.fetchall()
        except:t_data = ()
        snipet_dct = {}
        for row_data in t_data:
            rc, eid = row_data
            snipet_dct.setdefault(rc, []).append(eid)
            #snipet_dct.setdefault(rc, [])
            #if eid not in snipet_dct[rc]:
            #    snipet_dct[rc].append(eid)
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
        #print 'TTTTTTTTTTTTTTTTT', table_lst
        #return table_dct, table_set, page_coord_dct
        return table_dct, table_lst, page_coord_dct
        
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
        
    def read_rawdb_resultant_info(self, company_id, doc_id, page_no, grid_id):
        cid_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(cid_path, '{0}.db'.format(doc_id))
        print 'KKKKKKKKKKKKKKKKKKK', db_path
        conn, cur   = conn_obj.sqlite_connection(db_path)
        table_like = '{0}#{1}#{2}'.format(doc_id, page_no, grid_id)
        read_qry = """ SELECT celltype FROM rawdb WHERE gridids like '%{0}%' AND res_opr='R'; """.format(table_like)
        print read_qry
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        dist_rcs = {}
        for row_data in t_data:
            dist_rcs[str(row_data[0])] = 1
        return dist_rcs

    def read_grid_information(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        doc_id     = ijson['doc_id']
        table_type_id = ijson.get('tt_id', 0)
        grp_lst   = ijson.get('axis_key', [])
        hct_flg = 0 
        if ijson.get('hct') == 'Y':
            hct_flg = 1
        right_table_hglt_dct = {}
        cid_path = config.Config.comp_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        print db_path
        conn, cur   = conn_obj.sqlite_connection(db_path)
        print '1'
        rel_type_dct = {}
        if ijson.get('i_table_ids'):
            t_data = ijson['i_table_ids']
        
        elif not grp_lst:
            if not hct_flg:
                read_qry = """ SELECT row_id, doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id=%s AND classified_id=%s AND highly_connected='Y'; """%(doc_id, table_type_id)
            elif hct_flg:
                tab_id = ijson.get('table_id') 
                dc, pg, gr = tab_id.split('#')
                read_qry = """ SELECT row_id, doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id=%s AND page_no=%s AND grid_id=%s; """%(dc, pg, gr)
            #print read_qry
            #sys.exit()
            cur.execute(read_qry)
            #print 'READ'
            t_data = cur.fetchall()
            #print 'FETCH'
        if grp_lst:
            right_table_hglt_dct, t_data, page_coord_dct = self.read_axis_data(grp_lst, cur, conn, company_id, doc_id)
        conn.close()
        if ijson.get('ONLY_TABLE') != 'Y': # FOR SPEED
            snipet_dct, res_formula_dct, rel_type_dct, collect_fids  = self.read_left_formula_info(company_id, doc_id, grp_lst, ijson.get('table_id'))
        else:
            snipet_dct, res_formula_dct, rel_type_dct, collect_fids  = {}, {}, {}, {}
        print '2.'
        ref_path = self.ref_path_info_workspace(company_id)
        if not grp_lst: 
            equality_path  = config.Config.equality_path.format(company_id)
            db_path = os.path.join(equality_path, '{0}.db'.format(doc_id))
            a_conn, a_cur   = conn_obj.sqlite_connection(db_path)
            page_coord_dct = self.read_page_coods(a_cur)
        print '3.'
        
        all_rows_lst = [] 
        all_col_def = {}
        all_row_inf = {'ref_path':ref_path}
        row_number = 1
        row_idx_capture = []
        clr_max = 0
        col_da_d    = {}
        all_grids_d   = {}
        dis_doc_dct = {}
        for row_data in t_data:
            table_id, doc_id, page_no, grid_id = row_data
            dist_rcs    = {}
            if 1: # FOR SPEED
                dist_rcs = self.read_rawdb_resultant_info(company_id, doc_id, page_no, grid_id)
            #print 'DDDDDDDDDDDDDDDD', dist_rcs
            all_grids_d['%s#%s#%s'%( doc_id, page_no, grid_id)]   = {}
            emp_dct = {'sn':row_number, 'cid':row_number, 'rid':row_number, 0:{'v':'%s-(%s, %s, %s)'%(table_id, doc_id, page_no, grid_id)}, 'cls':'hdr_rw'}
            all_rows_lst.append(emp_dct)
            row_idx_capture.append(row_number-1)
            row_number += 1
            snip_dct = {}
            if not grp_lst: #FOR SPEED
                snip_dct = self.read_only_axis_snipet(a_cur, a_conn, doc_id, page_no, grid_id)
                 
            formula_dct = res_formula_dct.get((doc_id, page_no, grid_id), {})
            cell_fids   = collect_fids.get((doc_id, page_no, grid_id), {})
            rel_info = rel_type_dct.get((doc_id, page_no, grid_id), {})
            hglt_dct = right_table_hglt_dct.get((doc_id, page_no, grid_id), {})
            row_lst, inf_map, col_dct, row_number, clr_max, tmp_col_da_d = self.table_wise_grid_info(row_number, table_id, company_id, page_no, doc_id, snip_dct, formula_dct, grid_id, ref_path, hglt_dct, rel_info, clr_max, cell_fids, dist_rcs)
            col_da_d.update(tmp_col_da_d)
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
                dt_dct = {'k':col_id, 'n':'Description', 'col_type':'HGH', 'w':265, 'v_opt':1, 'pin':'pinnedLeft'}     
            elif col_id not  in col_da_d:
                dt_dct['visible']    = False
            col_def_lst.append(dt_dct)  
        all_row_inf['coords'] = page_coord_dct
        for table_hdr in row_idx_capture:
            for cl_df in col_def_lst:
                k_inf = cl_df['k'] 
                if k_inf == 0:continue
                all_rows_lst[table_hdr][k_inf] = {'v':''}
        #if clr_max:
        if ijson.get('ONLY_TABLE') != 'Y':
            col_def_lst.insert(2, {'k':'nav', 'n':'', 'w':30, 'mx':clr_max, 'pin':'pinnedLeft'})
        res = [{'message':'done', 'data':all_rows_lst, 'col_def':col_def_lst, 'map':all_row_inf, 'table_ids':all_grids_d.keys()}] 
        return res
        
    def read_all_tt_info(self, cur):
        read_qry = """ SELECT row_id, table_type, short_form FROM all_table_types; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        res_dct = {}
        for row in t_data:
            classified_id, table_type, short_form = row
            res_dct.setdefault(short_form, []).append(table_type)
            res_dct[('ID', short_form.lower(), table_type.lower())] = classified_id
        return res_dct

    def insert_table_type_info(self, table_name, short_tt, conn, cur):
        
        diff_set = {(table_name, short_tt)}
        all_tts = self.read_all_tt_info(cur)
        get_des_tt = all_tts.get(short_tt, []) 
        classified_id   = all_tts.get(('ID', short_tt.lower(), table_name.lower()))
        if classified_id:
            return classified_id, table_name, short_tt
        if (get_des_tt) and (table_name in get_des_tt):
            return 
        elif (get_des_tt) and (table_name not in get_des_tt):
            tt_str_lst  = table_name.split()
            tt_str_lst = [es[1] for es in tt_str_lst]
            short_tt = ''.join(tt_str_lst)
            short_tt = short_tt.upper()
            diff_set = {(table_name, short_tt)}
        
        if diff_set:
            insert_stmt = """ INSERT INTO all_table_types(table_type, short_form) VALUES(?, ?); """
            cur.executemany(insert_stmt, diff_set)
            conn.commit()
        read_qry = """ SELECT row_id  FROM all_table_types WHERE table_type="%s" AND short_form="%s"; """%(table_name, short_tt)
        cur.execute(read_qry)
        mst_data = cur.fetchall()
        conn.close()
        rid = mst_data[0]
        return rid, table_name, short_tt
        
    def read_all_table_types(self):
        db_path = '/mnt/eMB_db/company_management/global_info.db'        
        conn, cur   = conn_obj.sqlite_connection(db_path)

        crt_qry = """ CREATE TABLE IF NOT EXISTS all_table_types(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type TEXT, short_form VARCHAR(256)); """ 
        cur.execute(crt_qry)
    
        r_qry = """ SELECT table_type, short_form, row_id FROM all_table_types; """  
        cur.execute(r_qry)
        mt_data = cur.fetchall()      
        check_dct = {(es[0], es[1]):es[2] for es in mt_data}
        return check_dct, conn, cur        
 
    def save_hgh_scoping(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        data_lst = ijson['data']
        
        check_dct, gconn, gcur = self.read_all_table_types() 
        
        cid_path = config.Config.company_id_path.format(company_id)
        if not os.path.exists(cid_path):
            mk_cmd = 'mkdir -p %s'%(cid_path) 
            os.system(mk_cmd) 

        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        crt_table = """ CREATE TABLE IF NOT EXISTS hgh_scoped(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER NOT NULL, page_no INTEGER NOT NULL, grid_id INTEGER NOT NULL, xml_id TEXT, row_info VARCHAR(50), col_info VARCHAR(50), classified_id INTEGER NOT NULL); """
        cur.execute(crt_table)
        
        read_qry = """ SELECT doc_id, page_no, grid_id, xml_id FROM hgh_scoped; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        check_data = {(es[0], es[1], es[2], es[3]) for es in t_data}
           
        insert_rows = [] 
        update_rows = []
        for row_dct in data_lst: 
            table_type_id = row_dct['tt_id']
            page_no   = row_dct['pg']  
            grid_id   = row_dct['g']
            rc        = row_dct['rc'] 
            row, col = rc.split('_')
            xml_id    = row_dct['x']
            if not isinstance(table_type_id, int):
                short_tt = ''
                tt_str_lst = table_type_id.split()
                if len(tt_str_lst) == 1:
                    short_tt = ''.join(tt_str_lst)
                    short_tt = short_tt.upper()
                elif len(tt_str_lst) > 1:
                    short_tt = ''.join([es[0].upper() for es in tt_str_lst])
                tt_id = check_dct.get((table_type_id, short_tt))
                if not tt_id:
                    table_type_id, tt, stf = self.insert_table_type_info(table_type_id, short_tt, gconn, gcur)
                    check_dct[(tt, stf)] = table_type_id

            if (doc_id, page_no, grid_id, xml_id) not in check_data:
                dt_tup = (doc_id, page_no, grid_id, xml_id, row, col, table_type_id)
            elif (doc_id, page_no, grid_id, xml_id) in check_data:
                dt_tup = (table_type_id, doc_id, page_no, grid_id, xml_id)      
                update_rows.append(dt_tup)
        gconn.close()
        if insert_rows:    
            insert_stmt = """ INSERT INTO hgh_scoped(doc_id, page_no, grid_id, xml_id, row_info, col_info, classified_id) VALUES(%s, %s, %s, %s, %s, %s, %s); """

            cur.executemany(insert_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE hgh_scoped SET classified_id=%s WHERE doc_id=%s AND  page_no=%s AND  grid_id=%s AND  xml_id=%s; """
            cur.executemany(update_stmt, update_rows)
        conn.commit()
        conn.close()
        return [{'message':'done'}]
        
    def read_all_tt(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        project_id = ijson['project_id']
        
        check_dct, gconn, gcur = self.read_all_table_types() 
        
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        #print db_path
        try:
            conn, cur   = conn_obj.sqlite_connection(db_path)
            read_qry = """ SELECT distinct(classified_id) FROM table_mgmt WHERE doc_id='%s'; """%(doc_id)
            cur.execute(read_qry)
            t_data = cur.fetchall()
            conn.close()    
        except:t_data = ()
        cmp_chk_dct = {es[0] for es in t_data}
            
        sn = 1
        r_lst = []
        max_value = max(self.tt_order.values()) + 1
        for ((tt, sf), rid) in check_dct.items():
            # {"k": "CAI", "sn": 1, "rid": 1, "n": "Capital Adequacy Information",'g':'Y'}
            dt_dct = {'k':sf, 'sn':sn, 'rid':rid, 'n':tt, 'g':'N'}
            if rid in cmp_chk_dct:dt_dct['g']  = 'Y'
            r_lst.append(dt_dct) 
            sn += 1
        y_filter = ['Y', 'N']
        r_lst.sort(key=lambda x:y_filter.index(x['g']))
        res_lst = [] 
        for sn, ks_dct in enumerate(r_lst, 1):
            ordr = self.tt_order.get(ks_dct['k'], sn+max_value)
            ks_dct['ix'] = ordr 
            res_lst.append(ks_dct)
        res_lst.sort(key=lambda x:x['ix']) 
        rmt_lst = []
        for idx, d_dct in enumerate(res_lst, 1):
            d_dct['sn']= idx
            rmt_lst.append(d_dct) 
        rmt_lst += self.add_hard_coded_tt_40(rmt_lst[-1])
        return [{'message':'done', 'data':rmt_lst}]
        
    def add_hard_coded_tt_40(self, last_row):
        lsn, ix = last_row['sn'], last_row['ix']
        #r1 = {"ix": 1, "g": "N", "k": "IS", "n": "Income Statement", "sn": 1, "rid": 57} 
        lsn += 1
        ix += 1
        rw_lst = []
        for ks in ['Company_Basic_Information', 'Company_Client_Information']:
            row_dct = {"ix": ix, "g": "N", "k": ks, "n": ks, "sn": lsn, "rid":ks}
            rw_lst.append(row_dct)
            lsn += 1        
            ix += 1
        return rw_lst
            
    def read_desired_data_cell_dict(self, cell_dict, r_c):
        chref   = cell_dict.get('chref', '')
        xml_lst = cell_dict.get('xml_ids').split('$$')
        if chref:
            xml_id =  '#'.join(map(lambda x:x+'@'+chref, filter(lambda x:x.strip(), xml_lst)))
        else:
            xml_id =  '#'.join(filter(lambda x:x.strip(), xml_lst))
        return  xml_id
        
    def read_row_dct_data(self, row_dct, company_id):
        # {"d":5131,"p":6,"g":1,"rc":"4_1","fid":"","eid":"","l_id":"","ref_k":[5131,6,1,3589,"4_1"],"ctype":""}
        doc, page, grid, gv_rc, fid, eid, l_id, c_type, tx_rc  = row_dct['d'], row_dct['p'], row_dct['g'], row_dct['rc'], row_dct['fid'], row_dct['eid'], row_dct['l_id'], row_dct['ctype'], row_dct['tx_rc']
        t_path  = row_dct.get('path', '')
        rw, cl = gv_rc.split('_')
        d_p_g = '{0}_{1}_{2}'.format(doc, page, grid)
        g_data = self.read_json_file(d_p_g, company_id)
        ddict = g_data.get('data', {})
        gv_dct   = ddict[gv_rc] 
        gv_xml = self.read_desired_data_cell_dict(gv_dct, gv_rc)
        hgh_rc = '%s_%s'%(rw, 0)
        hgh_dct  = ddict[hgh_rc]
        hgh_xml  = self.read_desired_data_cell_dict(hgh_dct, hgh_rc)
        c_type = 'C'
        return doc, page, grid, fid, eid, l_id, c_type, gv_rc, gv_xml, hgh_rc, hgh_xml, t_path

    def alter_table_coldef(self, conn, cur, table_name, coldef):
        col_info_stmt   = 'pragma table_info(%s);'%table_name
        cur.execute(col_info_stmt)
        all_cols        = cur.fetchall()
        cur_coldef      = set(map(lambda x:str(x[1]), all_cols))
        exists_coldef   = set(coldef)
        new_cols        = list(exists_coldef.difference(cur_coldef))
        col_list = []
        for new_col in coldef:
            if new_col not in new_cols:continue
            col_list.append(' '.join([new_col, 'TEXT']))
        for col in col_list:
            alter_stmt = 'alter table %s add column %s;'%(table_name, col)
            #print alter_stmt
            try:
                cur.execute(alter_stmt)
            except:pass
        conn.commit()
        return 'done'
        
    def insert_into_scoped_table_mgmt(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        tt_id      = ijson['table_type']
        data_lst   = ijson['data']
        d_type   = ijson['type']
        user_name = ijson['user']
        t_path = str(ijson['t_path'])
        project_id = ijson['project_id']
        r_type     = ijson['r_type']
        ts_type    = ijson['ts_type']
        #taxo_info    = ijson.get('taxo_info', '')

        dtime   = str(datetime.datetime.now()).split('.')[0] 
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        os.system("mkdir -p %s"%(cid_path))
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        crt_qry = """ CREATE TABLE IF NOT EXISTS table_mgmt(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER NOT NULL, page_no INTEGER NOT NULL, grid_id INTEGER NOT NULL, classified_id INTEGER NOT NULL, highly_connected varchar(10)); """
        cur.execute(crt_qry)
        read_qry = """ SELECT doc_id, page_no, grid_id FROM table_mgmt; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        check_tm = {(es[0], es[1], es[2]) for es in t_data}
        #print check_tm
        insert_rows = []
        update_rows = []
        del_rows = []            
        scoped_insert = []
        if not data_lst and ijson.get('table_id'):
            d, p, g     = ijson['table_id'].split('#')
            data_lst    = [{'d':d, 'p':p, 'g':g}]
        g_table_ids     = ijson.get('g_table_ids', [])
        grp_map_d   = {}
        for idx, tgrp in enumerate(g_table_ids):
            for t_tid in tgrp:
                grp_map_d[t_tid] = idx 
                
        consider_tables = {}    
        for row_data in data_lst:
            d, p, g = row_data['d'], row_data['p'], row_data['g']
            dpg_tup = tuple(map(int, (d, p, g)))
            #print dpg_tup
            if dpg_tup in check_tm:
                update_rows.append((tt_id, 'Y', d, p, g))
            else:
                insert_rows.append((d, p, g, tt_id, 'Y'))
            t_id = '_'.join(map(str, (d, p, g)))
            tmpts_type  = row_data.get('ts_type')
            if  tmpts_type == None:
                tmpts_type  = ts_type
            scp_tup = (d, p, g, t_id, tt_id, d_type, t_path, user_name, dtime, r_type, tmpts_type, str(row_data.get('taxo_info', [])))
            scoped_insert.append(scp_tup)
            del_rows.append((d, p, g))
            consider_tables['%s_%s_%s'%(d, p, g)] = (len(scoped_insert), d, p, g, t_id, tt_id, d_type, t_path, user_name, dtime, r_type, tmpts_type, row_data.get('taxo_info', []))
        done_grp    = {}
        if 1:
            for table_id, vtup in consider_tables.items():
                if table_id not in grp_map_d:continue
                if grp_map_d[table_id] in done_grp:continue
                done_grp[grp_map_d[table_id]]    = 1
                org_idx, d, p, g, t_id, tt_id, d_type, t_path, user_name, dtime, r_type, tmpts_type, tmp_taxo_info = vtup
                for t_id in g_table_ids[grp_map_d[table_id]]:
                    if t_id in consider_tables:
                        if not tmp_taxo_info or consider_tables[t_id][12]:
                            continue
                    d, p,g = t_id.split('_')
                    #eflg, taxo_info   = self.form_ntable_taxo_info(company_id, t_id, tmp_taxo_info, table_id)
                    taxo_info    = ''
                    #if eflg == 1:
                    #    return [{'message':str(taxo_info)}]
                    #if tmp_taxo_info and (not taxo_info):
                    #    return [{'message':'Error not able form taxo info %s'%(t_id)}]
                        
                    scp_tup = (d, p, g, t_id, tt_id, d_type, t_path, user_name, dtime, r_type, tmpts_type, str(taxo_info))
                    if t_id in consider_tables:
                        scoped_insert[consider_tables[t_id][0]-1] = scp_tup
                    else:
                        scoped_insert.append(scp_tup)
                        del_rows.append((d, p, g))
        if ijson.get('EXIT') == 'Y':
            for r in scoped_insert:
                print r
            sys.exit()

        #cid_path = config.Config.company_id_path.format(company_id, project_id)
        #db_path = os.path.join(cid_path, 'table_info.db')
        #s_conn, s_cur   = conn_obj.sqlite_connection(db_path)

        crt_table = """ CREATE TABLE IF NOT EXISTS scoped_gv(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER NOT NULL, page_no INTEGER NOT NULL, grid_id INTEGER NOT NULL, table_id VARCHAR(256), gv_xmlid TEXT, gv_rc VARCHAR(50), taxonomy_xmlid TEXT, taxonomy_rc VARCHAR(50), classified_id INTEGER NOT NULL, f_type VARCHAR(5), formula_id VARCHAR(50), traverse_path TEXT); """
        cur.execute(crt_table)
        
        self.alter_table_coldef(conn, cur, 'scoped_gv', ['info_type', 'r_type', 'table_type', 'user_name', 'datetime'])
        
        if insert_rows:
            insert_stmt =  """ INSERT INTO table_mgmt(doc_id, page_no, grid_id, classified_id, highly_connected) VALUES(?, ?, ?, ?, ?); """
            cur.executemany(insert_stmt, insert_rows)
        if update_rows:
            update_stmt = """ UPDATE table_mgmt SET classified_id=? AND highly_connected=? WHERE doc_id=? AND page_no=? AND grid_id=?; """
            cur.executemany(update_stmt, update_rows)
        if del_rows:
            del_stmt = """ DELETE FROM scoped_gv WHERE doc_id=? AND page_no=? AND grid_id=? AND info_type='table'; """
            cur.executemany(del_stmt, del_rows) 
        if scoped_insert:
            try:
                cur.execute('alter table scoped_gv add column taxo_info text ')
            except:pass
            si_stmt = """ INSERT INTO scoped_gv(doc_id, page_no, grid_id, table_id, classified_id, info_type, traverse_path, user_name, datetime, r_type, table_type, taxo_info) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);  """
            cur.executemany(si_stmt, scoped_insert)
        conn.commit()
        sql = "select table_id from scoped_gv where classified_id=%s"%(ijson['table_type'])
        cur.execute(sql)
        res = cur.fetchall()
        conn.close()
        if ijson.get('from_scoped') != 'Y' and  ijson.get('hct') != 'Y':
            ijson['table_ids'] = map(lambda x:x[0], res)
            if 0:#ijson.get('hct') == 'Y':
                db_path = os.path.join(config.Config.comp_path.format(company_id), 'table_info.db')
                conn, cur   = conn_obj.sqlite_connection(db_path)
                sql         = "select doc_id, table_id, group_id from group_tables"
                cur.execute(sql)
                res = cur.fetchall()
                conn.close()
                grp_d   = {}
                for r in res:
                    doc_id, table_id, group_id  = r
                    grp_d[table_id] = group_id
                    grp_d.setdefault(('GRP', group_id), {})[table_id]   = 1
                dd  = {}
                for t in ijson['table_ids']:
                    if t in grp_d:
                        for table_id in grp_d[('GRP', grp_d[t])].keys():
                            dd[table_id]    = 1
                    else:
                            dd[t]    = 1
                ijson['table_ids'] = dd.keys()
            import modules.databuilder.scope_info as v2
            v2_obj = v2.Scope()
            res = v2_obj.add_to_scoped_group(ijson)
        else:
            res = [{'message':'done'}]
        self.save_taxo_tagged_info(ijson)
        return res

    def form_ntable_taxo_info(self, company_id, t_id, tmp_taxo_info, org_table):
        if not tmp_taxo_info:return 0, []
        taxo_info   = []
        g_data = self.read_json_file(t_id, company_id)
        ddict = g_data.get('data', {})
        r_cs = ddict.keys()
        r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
        rc_d    = {}
        for r_c in r_cs:
            row, col = int(r_c.split('_')[0]), int(r_c.split('_')[1])  
            rc_d.setdefault(col, {})[row]   =  1
        consider_col    = {}
        e_flg   = 0
        n_ar    = []
        for tinfo in tmp_taxo_info:
            cols     = {}
            for rc in tinfo['v']:
                cols[int(rc.split('_')[1])] = 1
            cols    = cols.keys()
            cols.sort()
            v_ar    = []
            for c in cols:
                if c not in rc_d:
                    e_flg = 1
                    e_ar.append('Error Column %s not exists in  %s'%(c, t_id))
                    continue
                consider_col[c] = 1
                rows    = rc_d[c].keys()
                rows.sort()
                for r in rows:
                    v_ar.append('%s_%s'%(r, c))
            n_ar.append({'k':tinfo['k'], 'v':v_ar})
            pass
        for c in rc_d.keys():
            if c not in consider_col:
                e_flg = 1
                e_ar.append('Error New Column %s not exists in  %s'%(c, org_table))
                
        if e_flg:
            return e_flg, e_ar
        return e_flg, n_ar
   
       
    def save_taxo_tagged_info(self, ijson):
        company_id      = ijson['company_id']
        project_id      = ijson['project_id']
        table_type_id   = ijson['table_type']
        taxo_data       = ijson.get('taxo_info', [])
           
        # [{"n":"RATIO","primary_col":"super_primary_key", "group_col":"range"}]
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        crt_stmt = """ CREATE TABLE IF NOT EXISTS taxo_tagged(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type INTEGER, column_header TEXT, tagged_key TEXT, key_type TEXT); """
        cur.execute(crt_stmt)
        read_qry = """ SELECT table_type, column_header, tagged_key, key_type FROM taxo_tagged; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        check_tagged = {s for s in t_data}    
 
        insert_rows = []
        for row_dct in taxo_data:
            col_header = row_dct['n'] 
            for key_type, tagged_key in row_dct.iteritems():
                if key_type == 'n':continue
                insert_tup = (table_type_id, col_header, tagged_key, key_type) 
                if insert_tup in check_tagged:continue
                insert_rows.append(insert_tup)
        if insert_rows:
            insert_stmt = """ INSERT INTO taxo_tagged(table_type, column_header, tagged_key, key_type) VALUES(?, ?, ?, ?); """
            cur.executemany(insert_stmt, insert_rows)
            conn.commit()
        conn.close()
        return
        
    def save_gv_scoping(self, ijson):
        
        if ijson.get('hct') == 'Y' or ijson.get('tscope') == 'Y':
            return self.insert_into_scoped_table_mgmt(ijson)
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        data_lst = ijson['data']
        project_id = ijson['project_id']
        r_type     = ijson['r_type']
        
        check_dct, gconn, gcur = self.read_all_table_types() 
        
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        if not os.path.exists(cid_path):
            mk_cmd = 'mkdir -p %s'%(cid_path) 
            os.system(mk_cmd) 

        dtime   = str(datetime.datetime.now()).split('.')[0]
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        #sql         = "drop table scoped_gv"
        #cur.execute(sql)
        crt_table = """ CREATE TABLE IF NOT EXISTS scoped_gv(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER NOT NULL, page_no INTEGER NOT NULL, grid_id INTEGER NOT NULL, table_id VARCHAR(256), gv_xmlid TEXT, gv_rc VARCHAR(50), taxonomy_xmlid TEXT, taxonomy_rc VARCHAR(50), classified_id INTEGER NOT NULL, f_type VARCHAR(5), formula_id VARCHAR(50), equality_id varchar(50), traverse_path TEXT, level_id TEXT, user_name TEXt, datetime TEXT); """
        cur.execute(crt_table)
        self.alter_table_coldef(conn, cur, 'scoped_gv', ['info_type', 'r_type'])

        del_table_rc    = []
        insert_rows     = []
        user            = ijson['user']
        table_type_id   = ijson['table_type']
        t_path = str(ijson['t_path'])
        scop_tabs = {}
        for row_dct in data_lst:
            #":[{"d":5131,"p":6,"g":1,"rc":"4_1","fid":"","eid":"","l_id":"","ref_k":[5131,6,1,3589,"4_1"],"ctype":""},{"
            # doc, page, grid, fid, eid, l_id, c_type, gv_rc, gv_xml, hgh_rc, hgh_xml
            did, page_no, grid_id, f_id, eid, l_id, rc_type, gv_rc, gv_xmlid, tx_rc, tx_xmlid, t_p = self.read_row_dct_data(row_dct, company_id)
            if (not did) or (not page_no) or (not grid_id):continue
            table_id    = '_'.join(map(str, (doc_id, page_no, grid_id)))
            scop_tabs[table_id] = 1
            insert_rows.append((doc_id, page_no, grid_id, table_id, gv_xmlid, gv_rc, tx_xmlid, tx_rc, table_type_id, rc_type, f_id, eid, l_id, t_path, user, dtime))
            del_tup = (doc_id, page_no, grid_id, gv_xmlid)
            del_table_rc.append(del_tup)
        if del_table_rc:
            del_stmt = """ DELETE FROM scoped_gv WHERE doc_id=? AND  page_no=? AND grid_id=? AND gv_xmlid=?  """
            cur.executemany(del_stmt, del_table_rc)
            
        #print insert_rows  
        if insert_rows:
            #insert_stmt = """ INSERT INTO scoped_gv(doc_id, page_no, grid_id, table_id, gv_xmlid, gv_rc, taxonomy_xmlid, taxonomy_rc, classified_id, f_type, formula_id, equality_id, level_id, traverse_path, user_name, datetime) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """ 
            for ist in insert_rows:
                insert_stmt = """ INSERT INTO scoped_gv(doc_id, page_no, grid_id, table_id, gv_xmlid, gv_rc, taxonomy_xmlid, taxonomy_rc, classified_id, f_type, formula_id, equality_id, level_id, traverse_path, user_name, datetime) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """ 
                cur.executemany(insert_stmt, [ist])
            conn.commit()
        conn.close()
        
        try:
            group_tables = self.update_project_group_tables(company_id, project_id, scoped_tables)
        except:group_tables = {}
        group_tab_list = group_tables.keys()
        import modules.databuilder.scope_info as v2
        v2_obj = v2.Scope()
        ijson['table_ids'] = group_tab_list
        res = v2_obj.add_to_scoped_group(ijson)
        self.save_taxo_tagged_info(ijson)
        res = [{'message':'done'}]
        return res
        
    def update_project_group_tables(self, company_id, project_id, scoped_tables):
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT table_id FROM group_tables WHERE group_id IN (SELECT group_id FROM group_tables WHERE table_id IN (%s));  """%(', '.join(['"'+e+'"'for e in scoped_tables]))
        cur.xecute(read_qry)
        t_data = cur.fetchall()
        conn.close()
           
        group_tables_dct = {}
        for rd in t_data:
            table_id = rd[0]
            group_tables_dct[table_id] = 1
        return group_tables_dct
        
    def add_table_types(self, ijson):
        table_type = ijson['table_type']
        db_path = '/mnt/eMB_db/company_management/global_info.db' 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        short_tt = ''
        tt_str_lst = table_type.split()
        if len(tt_str_lst) == 1:
            short_tt = ''.join(tt_str_lst)
            short_tt = short_tt.upper()
        elif len(tt_str_lst) > 1:
            short_tt = ''.join([es[0].upper() for es in tt_str_lst])
        n_id  = self.insert_table_type_info(table_type, short_tt, conn, cur)
        conn.close()
        data = self.read_all_tt(ijson)
        data = data[0]['data']
        res = [{'message':'done', 'data':data}]
        if n_id:
            res[0]['ID']    = n_id[0]
        return res

    def get_grid_data(self, rc_dct, d_p_g, row_no, company_id):
        g_data = self.read_json_file(d_p_g, company_id)
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
            rt_dct = {}
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
                col_dct[col] = 1
                inf_map['%s_%s'%(row_no, col)] = {'ref':[{'xml_list':xml_lst,'pno':pageno,'d':doc,'bbox':self.get_bbox(bbox)}], 'cs':cs, 'rs':rs, 'row':row, 'col':col, 'st':stype, 'ref_k':(doc, pageno, gridid, table_id, '%s_%s'%(row, col))}
                 
                row_dct[col] = dd
            if 0 not in row_dct:continue
            row_lst.append(row_dct)
            row_no += 1
        return row_lst, inf_map, col_dct, row_no
        
    def read_scoped_gvs(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        cid_path = config.Config.company_id_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        crt_table = """ CREATE TABLE IF NOT EXISTS scoped_gv(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER NOT NULL, page_no INTEGER NOT NULL, grid_id INTEGER NOT NULL, table_id VARCHAR(256), gv_xmlid TEXT, gv_rc VARCHAR(50), taxonomy_xmlid TEXT, taxonomy_rc VARCHAR(50), classified_id INTEGER NOT NULL, f_type VARCHAR(5), formula_id VARCHAR(50), traverse_path TEXT); """
        cur.execute(crt_table)
        read_qry = """ SELECT row_id, doc_id, page_no, grid_id, gv_xmlid, gv_rc, taxonomy_xmlid, taxonomy_rc FROM scoped_gv; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        try:
            page_coord_dct = self.read_page_coods(cur)
        except:page_coord_dct = {}
        conn.close()
        
        rc_dpg_dct = {}
        for row_data in t_data:
            row_id, doc_id, page_no, grid_id, gv_xmlid, gv_rc, taxonomy_xmlid, taxonomy_rc = row_data
            tab_id = '_'.join(map(str, (doc_id, page_no, grid_id)))
            rc_dpg_dct.setdefault(tab_id, {})[gv_rc] = gv_xmlid
            rc_dpg_dct.setdefault(tab_id, {})[taxonomy_rc] = taxonomy_xmlid
            
        ref_path = self.ref_path_info_workspace(company_id)

        all_rows_lst = []
        all_col_def = {}
        all_row_inf = {'ref_path':ref_path}
        row_number = 1 
        for t_id, rc_dct in rc_dpg_dct.iteritems():
            emp_dct = {'sn':row_number, 'cid':row_number, 'rid':row_number, 0:{'v':'%s'%(t_id)}, 'cls':'hdr_rw'}
            all_rows_lst.append(emp_dct)
            row_number += 1
        
            row_lst, inf_map, col_dct, row_no = self.get_grid_data(rc_dct, t_id, row_number, company_id)
            
            all_rows_lst += row_lst
            all_row_inf.update(inf_map)
            all_col_def.update(col_dct)
            emp_dct = {'sn':row_number, 'cid':row_number, 'rid':row_number}
            all_rows_lst.append(emp_dct)
            row_number += 1

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
        
    def read_scoped_gv_information(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
       
        import modules.databuilder.form_builder as mdf
        m_Obj = mdf.DataBuilder()
        data_dct = m_Obj.read_scope_doc_grid(ijson, [doc_id]) 
        if 'ONLY_TABLES' in data_dct:
            ijson['i_table_ids']    = data_dct['ONLY_TABLES']
            ijson['ONLY_TABLE']    = 'Y'
            return self.read_grid_information(ijson)
            
        rc_data  = data_dct['doc_data'][doc_id]['data']

    
        print 'AAAAAAAAAAAA', rc_data
       
 
        rc_dct = {} 
        for rc in rc_data.keys():
            row, col = map(int, rc.split('_'))
            rc_dct.setdefault(row, {})[col] = 1 
        
        sorted_rows = sorted(rc_dct.keys())
        col_set = set()
        sn = 1
        data_lst = []
        inf_map = {}
        for rw in sorted_rows:
            sorted_cols = rc_dct[rw].keys()
            row_dct = {'sn':sn, 'rid':sn, 'cid':sn, 'del_ids':{}}
            l_id = 'NL'
            for col in sorted_cols:
                col_set.add(col)
                rc_str = '%s_%s'%(rw, col)
                cell_dict = rc_data[rc_str]
                #print 'CCCCCCCCCCCCCCCCCCCCCCC', cell_dict, '\n' 
                if l_id == 'NL':
                    l_id = cell_dict['level_id'] 
                v = cell_dict['data']
                del_id = cell_dict.get('scope_info', ())
                if del_id:
                    row_dct['del_ids'][del_id[0]] = 1 
                    inf_map['{0}_{1}'.format(sn, col)] = {'row_id':del_id[0]}
                dt = {'v':v}
                row_dct[col] = dt
            row_dct['l_id']  = l_id
            row_dct['$$treeLevel']  = int(l_id)
            #print row_dct
            data_lst.append(row_dct) 
            sn += 1
        #print 'FFFFFFFFFFFFFFFFF' ,col_set 
        cols = sorted(list(col_set))
        col_def_lst = [{'k':'checkbox', 'n':'', 'v_opt':3}]
        for cl in cols:
            ph_dct =  {'k':cl, 'n':cl} 
            if cl == 0:
                ph_dct['n'] = 'Description'
                ph_dct['v_opt'] = 1
            col_def_lst.append(ph_dct)
        res = [{'message':'done', 'data':data_lst, 'col_def':col_def_lst, 'map':inf_map}]
        return res 
        
    def read_group_sugg_info(self, conn, cur):
        read_qry = """ SELECT group_id, classified_id FROM group_tables; """        
        cur.execute(read_qry)
        t_data = cur.fetchall()

        class_group = {str(e[1]):str(e[0])for e in t_data}
        
        r_qry = """ SELECT group_id, type, type_msg, table_id FROM group_sugg_info; """
        try:
            cur.execute(r_qry)
            t_data = cur.fetchall()
        except:
            t_data  = []
        group_type = {}
        group_type_flg = {}
        for rd in t_data:
            group_id, type_i, type_msg, table_id = rd
            group_type.setdefault(str(group_id), {}).setdefault(type_i, {}).setdefault(type_msg, {})[table_id]= 1
            group_type_flg.setdefault(str(group_id), {})[type_i] = 1
       
        group_infodct = {}
        for gp, ti_dct in group_type.iteritems():
            for ti, tm_dct in ti_dct.iteritems():
                for tm, tab_dct in tm_dct.iteritems():
                    group_infodct.setdefault(gp, {}).setdefault(ti, []).append({'k':tm, 'n':tm, 'g_t':tab_dct.keys()})
        return class_group, group_infodct, group_type_flg

    def all_scoped_table_types(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        class_group, group_type, group_type_flg = self.read_group_sugg_info(conn, cur)
        #except:class_group, group_type, group_type_flg = {}, {}, {}
        read_qry = """ SELECT distinct(classified_id) FROM scoped_gv; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        #tt_ids_str = ', '.join([str(es[0]) for es in t_data if es[0]]) 
        tt_ids_d    = {}
        for r in t_data:
            tt_ids_d[str(r[0])] = 1

        db_path = '/mnt/eMB_db/company_management/global_info.db' 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        r_qry = """ SELECT row_id, table_type, short_form FROM all_table_types""" # WHERE row_id in (%s); """%(tt_ids_str)
        cur.execute(r_qry)
        mst_data = cur.fetchall()
        conn.close()
        max_value = max(self.tt_order.values()) + 1
        data_lst = []
        for ix, est in enumerate(mst_data, max_value):
            k, ex, n = est
            if str(k) not in tt_ids_d:continue
            ordr = self.tt_order.get(n, ix)
            dt_dct = {'k':k, 'n':ex, 'ex':n, 'ix':ordr}
            flg_grp = class_group.get(str(k))
            grp_typ_dct = group_type_flg.get(flg_grp, {})
            for grp_typ in grp_typ_dct: 
                if grp_typ == 'SUGG':
                    dt_dct['flg'] = 'O'
                if grp_typ == 'ERROR':
                    dt_dct['flg'] = 'R'
            if grp_typ_dct:
                group_info = group_type[flg_grp]
                dt_dct['group_info'] = group_info
                
            data_lst.append(dt_dct)
        data_lst.sort(key=lambda x:x['ix'])   
        #try:
        #    color_dct = self.data_builder_stats(ijson)
        #except:color_dct = {}
        color_dct   = {}
        res = [{'message':'done', 'data':data_lst, 'color':color_dct}]
        return res
        
    def hard_code_class_id_info(self):
        row_dct = {'k':'Company Meta Data', 'ex':'Company Meta Data', 'n':'Company Meta Data', 'g_i': ['Basic Information', 'Client Details', 'SEC Information'] }
        return row_dct
        
    def read_cell_dct_rc_info(self, json_dct, hgh_rc, dpg):
        d, p, g = map(int, dpg.split('_'))
        c_row, c_col = hgh_rc.split('_')
        ddict = copy.deepcopy(json_dct)
        r_cs = ddict.keys()
        r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
        rc_d    = {}
        for r_c in r_cs:
            row, col = r_c.split('_')  
            if row != c_row:continue
            rc_d.setdefault(row, {})[col]   =  ddict[r_c]
        
        o_dct = {}
        col_dct = {}
        c_inf = ()
        for ri, c_dct in rc_d.iteritems():
            ri_dct = {} 
            for ci_str, cel_dct_tup in c_dct.iteritems():
                cel_dct, mrc = cel_dct_tup
                mr, mc = mrc.split('_')
                v = cel_dct['data']
                ci_str = copy.deepcopy(mc)
                tc = '%s-%s-'%(d, ci_str)
                if ci_str != '0':
                    col_dct[(tc, ci_str)] = 1
                    ri_dct[tc] = {'v':v}
                if ci_str == '0':
                    l_id = cel_dct['level_id']
                    c_inf = ((tc, ci_str), {'v':v, 'l_id':l_id})
            o_dct.update(ri_dct)
        #print c_inf
        #sys.exit()
        return o_dct, c_inf, col_dct 
        
    def read_doc_ph_map(self, company_id, project_id):
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'document_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, doc_name, doc_type, period_type, period, filing_type FROM document_meta_info;  """        
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        doc_ph_map = {}
        for row_data in t_data:
            doc_id, doc_name, doc_type, period_type, period, filing_type = row_data
            ph = '{0}{1}'.format(period_type, period) 
            doc_ph_map[doc_id] = ph
        return doc_ph_map
        
    def create_grp_id(self, doc_dct):
        docs = sorted(doc_dct.keys(), reverse=True)
        doc_grp = {}    
        for ix, dc in enumerate(docs, 1):
            doc_grp[dc] = ix 
        return doc_grp
        
    def read_DB_prep_data(self, ijson, doc_dct):
        doc_ids = doc_dct.keys()
        import modules.databuilder.form_builder as mdf
        m_Obj = mdf.DataBuilder()
        data_dct = m_Obj.read_scope_doc_grid(ijson, doc_ids) 
        doc_info  = data_dct['doc_data']
        dpg_wise_rc_dct = {}
        for doc_id in doc_ids:
            try:
                rc_data = doc_info[doc_id]['data']
            except:continue
            for rc, cell_dct in rc_data.iteritems():
                dpg_tup = cell_dct['grid_info']
                dc, pg, grd, xml, act_rc = dpg_tup 
                dpg_str = '{0}_{1}_{2}'.format(dc, pg, grd)
                dpg_wise_rc_dct.setdefault(dpg_str, {})[act_rc] = (cell_dct, rc)
               
         
        #for tid, r_dct in dpg_wise_rc_dct.iteritems():  
        #    for r_c, c_dct in r_dct.iteritems():
        #        print [tid, r_c]
        #        print c_dct, '\n'
        return dpg_wise_rc_dct 

    def latest_doc_inf(self, hgh_lst, doc_grp):
        d_grp = {}
        l_grp = {}
        for hgh_tup in hgh_lst:
            #print 'RRRR', hgh_tup
            hgh_cl_pk, hgh_v_dct = hgh_tup
            did, grd, emp = hgh_cl_pk[0].split('-') 
            did_gp = doc_grp[int(did)]
            d_grp.setdefault(did_gp, []).append(hgh_v_dct)
        sorted_grps = (d_grp.keys())
        grp = sorted_grps[0]
        hg_dct = d_grp[grp][0]
        lvl_id = int(hg_dct['l_id'])
        return hg_dct, lvl_id

    def across_doc_DB(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id, row_col, taxo_relation FROM across_doc_gv_rel; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        doc_dct = {} 
        taxo_wise_dct = {}
        for row_data in t_data:
            doc_id, page_no, grid_id, row_col, taxo_relation = row_data
            d_p_g = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
            taxo_wise_dct.setdefault(taxo_relation, {}).setdefault(d_p_g, []).append(row_col)
            doc_dct[doc_id] = 1             
        
        doc_grp = self.create_grp_id(doc_dct) 

        dpg_json_dct = self.read_DB_prep_data(ijson, doc_dct)
        
        mn_lst = []
        cl_inf_dct = {}
        inf_map = {}
        for t_key, dpg_dct in taxo_wise_dct.iteritems():    
            row_dct = {'sn':t_key, 'cid':t_key, 'rid':t_key, 'hgh_l':[]}
            cb_dct = {}
            for dpg, rc_lst in dpg_dct.iteritems():
                json_dct  = dpg_json_dct.get(dpg, {})
                if not json_dct:continue
                dpg_json_dct[dpg] = json_dct
                for hgh_rc in rc_lst:
                    o_dct, col_inf, col_dct = self.read_cell_dct_rc_info(json_dct, hgh_rc, dpg) 
                    #if not col_inf:continue
                    if col_inf:
                        row_dct['hgh_l'].append(col_inf)
                    row_dct.update(o_dct)
                    cl_inf_dct.update(col_dct)
            #print row_dct
            if not row_dct['hgh_l']:continue
            hgh_v_dct, l_id = self.latest_doc_inf(row_dct['hgh_l'], doc_grp)
            row_dct['desc'] = hgh_v_dct
            row_dct['$$treeLevel'] = l_id
            del row_dct['hgh_l']
            mn_lst.append(row_dct)

        col_def_lst = []
        for cl_i in cl_inf_dct:
            pk, clid  = cl_i
            did_g, grd, emp = pk.split('-')
            dgrp = doc_grp[int(did_g)]
            pk_dct = {'k':pk, 'n':pk, 'grp':dgrp, 'c':int(clid)}
            col_def_lst.append(pk_dct)
        col_def_lst.sort(key=lambda x:(x['grp'], x['c']))
        col_def_lst = [{'k':'desc', 'n':'Description', 'w':265, 'v_opt':1, 'pin':'pinnedLeft'}] + col_def_lst 
        #print col_def_lst
        #for ks in mn_lst:
        #    print ks, '\n'
        res = [{'message':'done', 'data':mn_lst, 'col_def':col_def_lst, 'map':inf_map}]
        return res
        
    def delete_scoped_gv(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        row_ids = ', '.join(map(str, ijson['row_ids']))
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        if row_ids:
            del_stmt = """ DELETE FROM scoped_gv WHERE row_id in (%s); """%(row_ids)
        elif ijson['table_type']:
            del_stmt = """ DELETE FROM scoped_gv WHERE classified_id in (%s); """%(ijson['table_type'])
            sql  = "delete from group_tables where  classified_id in (%s)"%(ijson['table_type'])
            cur.execute(sql)
        cur.execute(del_stmt)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res
        
    def read_tt_wise_docs(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        tt_id = ijson['table_type']
        doc_ph_map = self.read_doc_ph_map(company_id, project_id)
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT distinct(doc_id) from scoped_gv WHERE classified_id=%s; """%(tt_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        res_lst = []
        for row_data in t_data:
            doc_id = row_data[0]
            ph = doc_ph_map.get(doc_id, '')
            dt_dct = {'k':doc_id, 'n':ph}
            res_lst.append(dt_dct)
        res = [{'message':'done', 'data':res_lst}]
        return res
        
    def read_not_classified(self, company_id, project_id):
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        if not os.path.exists(db_path):
            return {}
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id FROM table_mgmt WHERE classified_id != 0;  """ 
        try:
            cur.execute(read_qry)
            t_data = cur.fetchall()
        except:
            t_data  = []
        conn.close()
        check_set = {'#'.join(map(str, e)) for e in t_data}
        return check_set
        
    def read_most_ref_grids(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        project_id = ijson['project_id']
        check_set = {} #self.read_not_classified(company_id, project_id)

        cid_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(cid_path, '{0}.db'.format(doc_id))
        print db_path
        
        try:
            conn, cur   = conn_obj.sqlite_connection(db_path)
            read_qry = """ SELECT table_id, table_score FROM MostReferencedTable; """
            cur.execute(read_qry)
            t_data = cur.fetchall()
            conn.close()
        except:t_data = ()

        data_lst = []
        for row_data in t_data:
            table_id, score = row_data
            #if table_id in check_set:continue
            disp_t_id = '_'.join(table_id.split('#')[1:])
            dt_dct = {'k':table_id, 'n':disp_t_id, 's':score}
            data_lst.append(dt_dct)
        res = [{'message':'done', 'data':data_lst}]
        return res
        
    def read_project_info(self, ijson):
        company_id = ijson['company_id']
        m_cur, m_conn   = conn_obj.MySQLdb_conn(config.Config.company_info_db)
        read_qry = """ SELECT cm.row_id, cm.client_name, cm.display_name FROM client_mgmt as cm INNER JOIN client_details AS ccm ON cm.row_id=ccm.project_id WHERE ccm.company_id=%s; """%(company_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        project_lst = []
        for row_data in t_data:
            row_id, client_name, display_name = row_data
            dt_dct = {'k':row_id, 'n':display_name}
            project_lst.append(dt_dct)
        res = [{'message':'done', 'data':project_lst}]
        return res
        
    def get_traversed_details(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        row_id     = ijson['row_id']
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id, table_id, gv_rc, row_id, traverse_path FROM  scoped_gv WHERE row_id=%s ; """%(row_id)
        cur.execute(read_qry)   
        t_data = cur.fetchone()
        conn.close()

        if t_data:
            doc_id, page_no, grid_no, table_id, gv_rc, row_id, traverse_path = t_data
            traverse_path = eval(traverse_path)
            if not traverse_path:
                return [{'message':'No Traverse Path'}]
            
            if traverse_path:
                ref_k = [doc_id, page_no, grid_no, table_id, gv_rc]
                t_data = traverse_path 
                c_ijson = copy.deepcopy(ijson)
                c_ijson['ref_k'] = ref_k 
                c_ijson['t_data'] = t_data
                c_ijson['rt'] = []
                c_ijson['c_rt'] = []
                hgrid = '{0}#{1}#{2}'.format(doc_id, page_no, grid_no)
                h_info = [[hgrid, gv_rc]] 
                c_ijson['h_info']  = h_info
                import modules.tablets.tablets as tablets
                s_Obj = tablets.Tablets()
                res = s_Obj.get_tablet_cell_ref(c_ijson)
                return res
        res = [{"message":"done", "data":[], 'relation':{}}]
        return res
        
    def read_formula_rawdb(self, company_id, doc_id):
        cid_path = config.Config.equality_path.format(company_id)
        db_path = os.path.join(cid_path, '%s.db'%(doc_id))
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT groupid, celltype, row, col, gridids FROM rawdb; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        #rawdb_dct = {(es[0], es[1]):es[2] for es  in t_data}
        # {u'f2': u'F-406_4', u's_rc': u'', u'rtype': u'ADJ', u't2': u'5131#114#1', u'rc2': u'4_4', u'eq_id': 1476, u'eid': u'F-10_1', u'rc': u'8_1', u'ROOT': [5131, 6, 1, 5465, u'8_1'], u't': u'5131#6#1'}
        rawdb_dct = {}     
        grp_rc_dct = {}
        for es in t_data:
            groupid, celltype, row, col, tablegrids = es
            tablegrids = tablegrids.rsplit('#', 1)[0]
            rawdb_dct[(groupid, celltype)] = row
            grp_rc_dct.setdefault((groupid, row), {})[col] = (celltype, tablegrids)
            
        r_qry = """ SELECT fid, groupid FROM formulainfo;  """
        cur.execute(r_qry)
        t_data = cur.fetchall()
        conn.close()
        formula_dct = {}  #{es[0]:es[1] for es in t_data}
        formula_grp_dct = {}
        for es in t_data:
            eid, gpid = es
            formula_dct[eid] = gpid 
            formula_grp_dct.setdefault(gpid, set()).add(eid)
        return rawdb_dct, formula_dct, grp_rc_dct, formula_grp_dct
    
    def scoped_predictor_info(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        doc_id     = ijson['doc_id']        
        row_id     = ijson['rid']
        rawdb_dct, formula_dct, grp_rc_dct, formula_grp_dct = self.read_formula_rawdb(company_id, doc_id)
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT traverse_path FROM scoped_gv WHERE row_id=%s; """%(row_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()[0]
        conn.close()
        s_lst = json.loads(binascii.a2b_hex(t_data))
        doc_ph_map = self.read_doc_ph_map(company_id, project_id)
        print s_lst
        #sys.exit()
        res_lst = []
        for dct in s_lst:
            feid = dct.get('eid') 
            teid = dct.get('f2') 
            feid = feid.split('-')[1]
            teid = teid.split('-')[1]
            if (not feid) or (not teid):continue
            #feid = feid.split('-')[1]
            #teid = teid.split('-')[1]
            rc1 = dct.get('rc')
            rc2 = dct.get('rc2')
            group1 = formula_dct[feid]
            group2 = formula_dct[teid]
            row1 = rawdb_dct[(group1, rc1)]
            row2 = rawdb_dct[(group2, rc2)]
            stct1 = '{0}-{1}#{2}'.format(doc_id, group1, row1)
            srct2 = '{0}-{1}#{2}'.format(doc_id, group2, row2)
            m_l = [stct1, srct2]    
            res_lst.append(m_l)
        print res_lst
        #sys.exit()
        
        graph_path = os.path.join('/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/dbgraph/'+company_id+'/')
        print graph_path
        sys.path.insert(0, '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')        
        import raw_preview_builder_data_builder as rp 
        r_Obj = rp.raw_preview_builder_data_builder()        
        other_doc_results = r_Obj.find_builder_relationship(graph_path, res_lst) 
        print other_doc_results
        sys.exit()
        
        data_res_lst = []
        doc_ids_info = []
        doc_res_dct = {}
        for doc, info_lst in other_doc_results.iteritems():
            ph = doc_ph_map[doc]
            doc_dct = {'k':doc, 'n':ph} 
            doc_ids_info.append(doc_dct)
            rawdb_dct_eq, formula_dct_eq, grp_rc_dct_eq, formula_grp_dct_eq = self.read_formula_rawdb(company_id)
            for poss_id, arr_poss in enumerate(info_lst, 1):
                child_lst = [] 
                for inf_tup in arr_poss:
                    r_str, r2_str = inf_tup
                    d1, gr1 =  r_str.split('#')
                    grp1, row1 = gr1.split('-')            
                    # from dct list
                    from_lst = self.from_or_to_dct_lst(doc_id, grp1, row1, grp_rc_dct, formula_grp_dct, ft_flg=0)
                    
                    d2, gr2 =  r2_str.split('#')
                    grp2, row2 = gr2.split('-')
                    # to dct list 
                    to_lst = self.from_or_to_dct_lst(doc, grp2, row2, grp_rc_dct_eq, formula_grp_dct_eq, ft_flg=1)
                     
                    # create possible combinations
                    all_poss = self.create_possible_combinations(from_lst, to_lst)
                    child_lst.extend(all_poss)
                doc_pos_dct = {'k':poss_id, 'n':poss_id, 'data':child_lst}
                doc_res_dct.setdefault(doc, []).append(doc_pos_dct) 
        res = [{'message':'done', 'data':doc_ids_info, 'possibility':doc_res_dct}]
        return res
                    
        
    def create_possible_combinations(self, from_lst, to_lst):
        res_lst = []
        for from_dct in from_lst:
            for to_dct in to_lst:
                dt_emp_dct = {}
                dt_emp_dct.update(from_dct)
                dt_emp_dct.update(to_dct)
                res_lst.append(dt_emp_dct)
        return res_lst
    
    def from_or_to_dct_lst(self, doc, grp, row, grp_rc_dct_eq, formula_grp_dct_eq, ft_flg=0):
        #{u'f2': u'F-406_4', u's_rc': u'', u'rtype': u'ADJ', u't2': u'5131#114#1', u'rc2': u'4_4', u'eq_id': 1476, u'eid': u'F-10_1', u'rc': u'8_1', u'ROOT': [5131, 6, 1, 5465, u'8_1'], u't': u'5131#6#1'}
        col_set = formula_grp_dct_eq[grp]
        cel_ct_dct = grp_rc_dct_eq[(grp, row)]
        res_lst = []
        for col, ctype_tup in cel_ct_dct.iteritems():
            gpc = '{0}_{1}'.format(grp, col)
            if gpc not in col_set:continue
            ctype, table_grid = ctype_tup
            f_str = '{0}-{1}'.format('F', gpc)
            if not ft_flg:
                dt_dct = {'rc':ctype, 't':table_grid, 'eid':f_str}
            if ft_flg:
                dt_dct = {'rc2':ctype, 't2':table_grid, 'f2':f_str}
            res_lst.append(dt_dct)
        return res_lst
        
    def read_mapping_info(self, company_id, doc_id, project_id, template_id):
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'mapping_info', str(project_id), '{0}.db'.format(template_id))
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT distinct(table_id) FROM mapping_info WHERE doc_id={0}; """.format(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        table_dct = {}
        for row_data in t_data:
            table_id = row_data[0]
            table_dct[table_id] = 1
        return table_dct

    def read_all_grids_doc_wise(self, ijson):
        company_id   = ijson['company_id']
        doc_id     = ijson['doc_id']
        import redis_api as pre
        rObj = pre.exe("172.16.20.7##6384##0", int(company_id), int(doc_id)) 
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id={0}; """.format(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        project_id = ijson.get('project_id', '5')
        template_id = ijson.get('template_id', '7')
        try:
            table_dct = self.read_mapping_info(company_id, doc_id, project_id, template_id)
        except:table_dct = {}
        row_no = 1 
        row_lst = []
        inf_map = {}
        for row_data in t_data:
            doc_id, page_no, grid_id = row_data
            gh_text = rObj.make_exec(int(page_no), int(grid_id), 'gh')
            #print [row_data, text]
            table_id = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
            row_dct = {'sn':{'v':row_no}, 'cid':row_no, 'rid':row_no, 'p_g':{'v':'%s_%s'%(page_no, grid_id)}, 'tid':'{0}#{1}#{2}'.format(doc_id, page_no, grid_id), 'grid_header':{'v':gh_text}, 'flg':{'v':''}} 
            if  table_id in table_dct:
                row_dct = {'sn':{'v':row_no}, 'cid':row_no, 'rid':row_no, 'p_g':{'v':'%s_%s'%(page_no, grid_id)}, 'tid':'{0}#{1}#{2}'.format(doc_id, page_no, grid_id), 'grid_header':{'v':gh_text}, 'flg':{'v':'G'}} 
            row_lst.append(row_dct)     
            row_no += 1
        col_def_lst = [{'k':'sn', 'n':'S.No', 'type':'SL'}, {'k':'p_g', 'n':'Page-Grid'}, {'n':'flg', 'k':'flag', 'v_opt':2}, {'k':'grid_header', 'n':'Grid header'}]  
        res = [{'message':'done', 'data':row_lst, 'col_def':col_def_lst, 'map':inf_map}]
        return res
        
    def check_scoped_gv_info(self, company_id, doc_id, project_id):
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id, gv_rc FROM scoped_gv WHERE doc_id=%s; """%(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()

        scoped_tables = set()
        for row_data in t_data:
            doc_id, page_no, grid_id, gv_rc = row_data
            table_id = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
            scoped_tables.add(table_id)
        return scoped_tables 

    def read_all_grids_doc_wise_no_grid_header(self, ijson):
        company_id   = ijson['company_id']
        doc_id     = ijson['doc_id']
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id={0}; """.format(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        project_id = ijson.get('project_id', '5')
        template_id = ijson.get('template_id', '7')
        try:
            table_dct = self.check_scoped_gv_info(company_id, doc_id, project_id)
        except:table_dct = {}
        row_no = 1 
        row_lst = []
        inf_map = {}
        for row_data in t_data:
            doc_id, page_no, grid_id = row_data
            #print [row_data, text]
            table_id = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
            row_dct = {'sn':{'v':row_no}, 'cid':row_no, 'rid':row_no, 'p_g':{'v':'%s_%s'%(page_no, grid_id)}, 'tid':'{0}#{1}#{2}'.format(doc_id, page_no, grid_id), 'flg':{'v':''}} 
            if  table_id in table_dct:
                row_dct = {'sn':{'v':row_no}, 'cid':row_no, 'rid':row_no, 'p_g':{'v':'%s_%s'%(page_no, grid_id)}, 'tid':'{0}#{1}#{2}'.format(doc_id, page_no, grid_id), 'flg':{'v':'G'}} 
            row_lst.append(row_dct)     
            row_no += 1
        col_def_lst = [{'k':'sn', 'n':'S.No', 'type':'SL'}, {'k':'p_g', 'n':'Page-Grid'}, {'n':'flg', 'k':'flg', 'v_opt':2}]  
        res = [{'message':'done', 'data':row_lst, 'col_def':col_def_lst, 'map':inf_map}]
        return res

    def read_formula_info_flag_NG(self, conn, cur, formula_table_name):
        read_qry = """ SELECT resultant_info, groupid, formula FROM %s WHERE formula_type='FORMULA NONG' """%(formula_table_name)
        cur.execute(read_qry)   
        t_data = cur.fetchall()
        r_set = set()
        operator_dct = {}
        row_group_operator = {}
        for row in t_data:
            res_info, groupid, formula = row[0], row[1], row[2]
            data_tup = eval(res_info)
            op, (r, c), grpid, ed = data_tup
            d_str = '{0}_{1}_{2}'.format(r, c, groupid)
            r_set.add(d_str)
            row_group_operator.setdefault('{0}_{1}'.format(r, groupid), set()).add(formula)
        return r_set, operator_dct, row_group_operator

    def read_formula_info_flag(self, conn, cur, formula_table_name):
        read_qry = """ SELECT resultant_info, groupid, formula FROM %s WHERE formula_type='FORMULA G' """%(formula_table_name)
        cur.execute(read_qry)   
        t_data = cur.fetchall()
        r_set = set()
        operator_dct = {}
        row_group_operator = {}
        for row in t_data:
            res_info, groupid, formula = row[0], row[1], row[2]
            data_tup = eval(res_info)
            formula_lst = eval(formula)
            #print 'FFFFFFFFFF', [formula_lst, groupid]
            op, (r, c), grpid, ed = data_tup
            d_str = '{0}_{1}_{2}'.format(r, c, groupid)
            r_set.add(d_str)
            operator_dct[d_str] = '='
            row_group_operator.setdefault('{0}_{1}'.format(r, groupid), set()).add('=')
            for fr_tup in formula_lst:  
                opr, (ro, co), o_grpid, o_ed = fr_tup
                d2_str = '{0}_{1}_{2}'.format(ro, co, groupid) 
                operator_dct[d2_str] = opr        
                row_group_operator.setdefault('{0}_{1}'.format(ro, groupid), set()).add(opr)
        #print 'OOOOOOOOOOOOO', operator_dct
        return r_set, operator_dct, row_group_operator
                 
    def read_rawdb_tree(self, ijson):
            
        table_name = 'rawdb'
        formula_table_name = 'formulainfo'

        company_id      = ijson.get('company_id', 'TestDB')
        doc_id          = ijson['doc_id']
        
        equality_path = config.Config.equality_path.format(company_id) 
        db_path = os.path.join(equality_path, '{0}.db'.format(doc_id))
        print db_path
        if not os.path.exists(db_path): 
            return [{'message':'done', 'grid_data':{'data':[], 'col_def':[], 'map':{}}}]
        conn, cur = self.connect_to_sqlite(db_path)
        
        formula_str, operator_dct, row_group_operator = set(), {}, {}
        try:
            formula_str_g, operator_dct_g, row_group_operator_g = self.read_formula_info_flag(conn, cur, formula_table_name)
        except:formula_str_g, operator_dct_g, row_group_operator_g = set(), {}, {}
        formula_str.update(formula_str_g)
        operator_dct.update(operator_dct_g)
        row_group_operator.update(row_group_operator_g)
        try:
            nong_formula_str, nong_operator_dct, nong_row_group_operator = self.read_formula_info_flag_NG(conn, cur, formula_table_name)
        except:nong_formula_str, nong_operator_dct, nong_row_group_operator = set(), {}, {}
        formula_str.update(nong_formula_str)
        operator_dct.update(nong_operator_dct)
        row_group_operator.update(nong_row_group_operator)

        rc_flg= 'Both'
        rc_map_dct = {
                        'Column':['G:C', 'NONG:C'],   
                        'Row'   :['G:R', 'NONG:R'],       
                        'Both'  :['G:C', 'NONG:C', 'G:R', 'NONG:R']
                        }
        rc_flg_lst = rc_map_dct.get(rc_flg, rc_map_dct['Both'])
        rc_flg_str = ', '.join(['"'+e+'"' for e in rc_flg_lst])
        table_id = ijson['table_id'] 
            
        table_info_grids = '#'.join(table_id.split('_'))
        read_qry = """  SELECT row_col_groupid, row, col, groupid, cellph, cellcsvc, cellcsvs, cellcsvv, gvtext, hghtext, datatype, gvxmlid FROM rawdb  WHERE gridids like '%{0}%' AND comp_type in ({1}); """.format(table_info_grids, rc_flg_str)
        print read_qry
        try:
            cur.execute(read_qry)
            t_data = cur.fetchall()
        except:t_data = ()
            
        conn.close()    
        
        group_rc_dct              = OD()
        group_col_dct             = {} 
        group_r_hgh_dct           = {}
        formula_flg_dct           = {}
        group_data_type           = {}
        highlight_col             = {}
        highlight_whole_group     = {}
        collect_distinct_groups   = {}
        for row_data in t_data:
            row_col_group_id, row, col, groupid, cellph, cellcsvc, cellcsvs, cellcsvv, gvtext, hghtext, datatype, gvxmlid = row_data
            try:
                cellph    = eval(cellph)[0]
            except:cellph = ''
            try:
                cellcsvc  = eval(cellcsvc)[0]
            except:cellcsvs = ''
            #cellcsvs  = eval(cellcsvs)[0]
            gvtext    = ' '.join(eval(gvtext))
            hghtext_str   = ' '.join(eval(hghtext))  
            period_type, period = cellph[:-4], cellph[-4:]
            f_flg = 'N'
            if row_col_group_id in formula_str:   
                f_flg = 'Y'
            ref_key      = row_col_group_id
            hgh_ref_key  = row_col_group_id
            gcr_dct = {'v':gvtext, 'title':{'pt':period_type, 'p':period, 'c':cellcsvc, 's':cellcsvs, 'vt':cellcsvv}, 'ref_k':ref_key, 'w':len(gvtext)}
            opr_in = operator_dct.get('{0}_{1}_{2}'.format(row, col, groupid))
            group_rc_dct.setdefault(groupid, OD()).setdefault(row, OD())[col] = gcr_dct 
            group_r_hgh_dct.setdefault(groupid, {}).setdefault(row, {}).setdefault('txt_lst', []).append((hghtext_str, hgh_ref_key, row_col_group_id))
            group_col_dct.setdefault(groupid, OD()).setdefault(col, []).append(cellph)
            highlight_col.setdefault(groupid, OD()).setdefault(col, []).append(ref_key)
            formula_flg_dct.setdefault(groupid, {}).setdefault(row, set()).add(f_flg)
            group_data_type.setdefault(groupid, []).append(datatype)
            highlight_whole_group.setdefault(groupid, []).append(row_col_group_id)

        xml_color_map = {}
        group_colr = {}
        
        #print group_col_dct
        #sys.exit()
        data_lst = []
        inf_map  = {}
        sn = 1
        cl_nw_key_dt =  {}
        for grp_sn, (grp_id, rc_dct) in enumerate(group_rc_dct.iteritems(), 1):
            ck_dct = group_col_dct[grp_id]
            dtype_lst = group_data_type.get(grp_id, [])
            dtype = ''
            if dtype_lst:
                dtype = dtype_lst[0]
            rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, 'desc':{'v':'Group-%s (%s~%s)'%(grp_sn, grp_id, dtype), 'cls':'grid-header'}, '$$treeLevel':0}
            color_info = ''
            group_sn = copy.deepcopy(sn) 
            group_gv_hlgt_dct = {'GV':{}, 'HGH':{}}
            col_map = {}
            col_key_idx = 1
            for clo, ph_lst in ck_dct.iteritems():
                c_key = 'COL-%s'%(col_key_idx)
                col_map[clo] = c_key
                cl_nw_key_dt[c_key] = 0
                col_key_idx += 1
                #print grp_id
                #print clo
                sorted_ph = []
                try:
                    sorted_ph = rys.year_sort(set(ph_lst)) 
                    sorted_ph.reverse()
                except:pass
                cph_str = '~'.join(sorted_ph)
                rw_dct[c_key] = {'v':cph_str, 'cls':'grid-header'}
                gv_rf_ky_lst = highlight_col[grp_id][clo]
                gv_ref_str = '~~'.join(gv_rf_ky_lst)
                gv_ref_str_ctype = '-'.join((gv_ref_str, 'GV'))
                inf_map['%s_%s'%(sn, c_key)]  = {'ref_k':gv_ref_str_ctype}
                group_gv_hlgt_dct['GV'][gv_ref_str] = 1

            data_lst.append(rw_dct)    
            row_fm_dct = formula_flg_dct[grp_id]
            sn += 1
            for rw, clmn_dct in rc_dct.iteritems(): 
                rcw_dct = {'rid':sn, 'sn':sn, 'cid':sn, '$$treeLevel':1}
                rf_set = row_fm_dct[rw]
                fg = 0
                if 'Y' in rf_set:
                    fg = 1
                if fg:
                    rcw_dct['cls'] = 'resultant-formula'
                for cl, dt_inf in clmn_dct.iteritems():  
                    cl_ky = col_map[cl]
                    ref_k_info = dt_inf['ref_k']
                    del dt_inf['ref_k']
                    dt_inf['cls'] = color_info
                    rcw_dct[cl_ky] = dt_inf
                    inf_map['%s_%s'%(sn, cl_ky)] = {'ref_k':'%s-%s'%(ref_k_info, 'GV')}
                    c_width = dt_inf['w']
                    cl_nw_key_dt[cl_ky] = max(cl_nw_key_dt[cl_ky], c_width)
                o_i = row_group_operator.get('{0}_{1}'.format(rw, grp_id), '')
                #print 'WWWWWWWWWWWWWWWWWWWWWW', [o_i]
                if o_i:
                    o_i = ', '.join(o_i)
                    rcw_dct['op_i'] = {'v':o_i}

                hgh_t_lst = group_r_hgh_dct[grp_id][rw]['txt_lst']
                desc_tup    = hgh_t_lst[0]
                desc, rcg, rcg_ref_key  = desc_tup
                group_gv_hlgt_dct['HGH'][rcg_ref_key] = 1
                rcw_dct['desc'] = {'v':desc}
                inf_map['%s_%s'%(sn, 'desc')] = {'ref_k':'%s-%s'%(rcg, 'HGH')}
                data_lst.append(rcw_dct)
                sn += 1
            res_group_hghlt_dct = {}
            for gh_key, vl_dct in group_gv_hlgt_dct.iteritems():
                mrg = '-'.join(('~~'.join(vl_dct), gh_key))
                res_group_hghlt_dct[mrg] = 1
            res_hglt_mrg = '##'.join(res_group_hghlt_dct)
            inf_map['%s_%s'%(group_sn, 'desc')] = {'ref_k':res_hglt_mrg}
            empty_rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, '$$treeLevel':0}
            data_lst.append(empty_rw_dct)
            sn += 1

        col_def = [{'k':'checkbox', 'n':'', 'v_opt':3}, {'k':'desc', 'n':'Description', 'w':265, 'v_opt':1, 'col_type':'HGH'}]
        c_def = []
        for cs, nm in cl_nw_key_dt.iteritems():
            c_alpha_int = int(cs.split('-')[1]) - 1
            csa = self.find_alp(c_alpha_int) 
            dt_dct = {'k':cs, 'n':csa, 'col_type':'GV', 'w':80}
            c_def.append(dt_dct)
        c_def.sort(key=lambda x:int(x['k'].split('-')[1]))
        col_def.extend(c_def)
        #print 'PPPPPPPPPPPPPPPPPPPPPPP', [project_id]
        #ref_path_data = self.ref_path_info_workspace('34', deal_id)
        #ref_path_data = self.ref_path_info(project_id, deal_id)
        #inf_map['ref_path'] = ref_path_data
        #for k in data_lst:
        #    print k
        #sys.exit()
        return [{'message':'done', 'data':data_lst, 'col_def':col_def, 'map':inf_map}]
    
    def get_count_tables(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        doc_id     = ijson['doc_id']
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id=%s; """%(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        all_distinct_tables = set()
        for row_data in t_data:
            doc_id, page_no, grid_id = row_data
            all_distinct_tables.add((doc_id, page_no, grid_id)) 

        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id FROM scoped_gv WHERE doc_id=%s; """%(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()

        scoped_tables = set()
        for row_data in t_data:
            doc_id, page_no, grid_id = row_data
            scoped_tables.add((doc_id, page_no, grid_id)) 
        res = [{'message':'done', 'all_tables':len(all_distinct_tables), 'scoped_tables':len(scoped_tables)}]
        return res

    def read_json_file(self, table_id, company_id):
        json_file_path = '/mnt/eMB_db/company_management/{0}/json_files/{1}.json'.format(company_id, table_id)
        if not os.path.exists(json_file_path):
            return {}
        json_dct = {}
        with open(json_file_path, 'r') as j:
            json_dct = json.load(j)
        return json_dct
        
    def gv_count(self, json_dict):
        if not json_dict:
            return 0
        rc_dct = json_dict['data']
        over_all_gv_cnt = 0
        for rc, cell_dct in rc_dct.iteritems():
            if cell_dct['ldr'] != 'value':continue
            over_all_gv_cnt += 1
        return over_all_gv_cnt
            
    def table_stats(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        doc_id     = ijson['doc_id']
        
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id, gv_rc FROM scoped_gv WHERE doc_id=%s; """%(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        scoped_tables_dct = {}
        for row_data in t_data:
            doc_id, page_no, grid_id, gv_rc = row_data
            tab_id  = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
            scoped_tables_dct.setdefault(tab_id, set()).add(gv_rc)

        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id=%s; """%(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        all_distinct_tables_dct = {}
        overall_scoped = 0
        overall_remaining = 0
        total_gvs = 0
        for row_data in t_data:
            doc_id, page_no, grid_id = row_data
            tid  = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
            json_dict = self.read_json_file(tid, company_id)
            over_gv_cnt = self.gv_count(json_dict)
            scoped_gv_cnt = len(scoped_tables_dct.get(tid, {}))
            remaining_gvs = over_gv_cnt - scoped_gv_cnt
            overall_scoped += scoped_gv_cnt
            overall_remaining += remaining_gvs
            total_gvs += over_gv_cnt
            all_distinct_tables_dct[tid] = (over_gv_cnt, scoped_gv_cnt, remaining_gvs)
        
        res_lst = [{'k':'total_grids', 'n':'Total grids', 'c':len(all_distinct_tables_dct)}, {'k':'Total Value', 'n':'Total Value', 'c':total_gvs},{'k':'scoped', 'n':'Scoped', 'c':overall_scoped}, {'k':'remaining', 'n':'Remaining', 'c':overall_remaining}]
        stats_data = res_lst
        res = [{'message':'done', 'stats_data':stats_data}]
        return res
        
    def read_db_ref_data(self, conn, cur):
        read_qry = """ SELECT rawdb_row_id, xml_id, bbox, page_coords, doc_id, page_no, table_id FROM reference_table; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
    
        ref_data = {}
        all_docs = {}
        for row_data in t_data:
            rawdb_row_id, xml_id, bbox, page_coords, doc_id, page_no, table_id = row_data
            xml_sp = filter(lambda x: x, xml_id.split('#'))
            np = page_no
            if xml_sp:
                xml_sp1 = filter(lambda x: x, xml_sp[0].split('z'))
                if xml_sp1:
                    np = xml_sp1[-1].split('_')[1]
            #print 'rrrr', np
            ref_data[rawdb_row_id] = {'x':xml_id, 'bx':bbox, 'pc':page_coords, 'd':doc_id, 'p': np, 't':table_id}
            all_docs[doc_id] = 1
        return ref_data, all_docs
        
    def read_db_phcsv(self, conn, cur):
        read_qry = """ SELECT rawdb_row_id, period_type, period, currency, scale, value_type FROM phcsv_info; """ 
        cur.execute(read_qry)
        t_data = cur.fetchall()
        phcsv_dct = {}
        for row_data in t_data:
            rawdb_row_id, period_type, period, currency, scale, value_type = row_data
            phcsv_dct[rawdb_row_id] = {'pt':period_type, 'p':period, 'c':currency, 's':scale, 'v':value_type}
        return phcsv_dct    
        
    def read_ref_rid(self, conn, cur, row_id):      
        read_qry = """ SELECT xml_id, bbox, page_coords, doc_id, page_no, table_id FROM reference_table WHERE rawdb_row_id=%s; """%(row_id)
        print read_qry
        cur.execute(read_qry)
        t_data = cur.fetchone()
        xml_id, bbox, page_coords, doc_id, page_no, table_id = t_data
        if page_coords:
            page_coords = eval(page_coords)
        
        read_qry = """ SELECT period_type, period, currency, scale, value_type FROM phcsv_info WHERE rawdb_row_id=%s; """%(row_id) 
        cur.execute(read_qry)
        t_data = cur.fetchone()
        period_type, period, currency, scale, value_type = t_data
        tmpar   = []
        for x in xml_id.split('#'):
            if 'z' in x:
                x   =  x.split('z')[1]
            tmpar.append(x)
        xml_id  = '#'.join(tmpar)
            
        ref_ph_dct = {'x':xml_id, 'bbox':eval(bbox), 'coord':page_coords, 'd':doc_id, 'p':page_no, 't':table_id, 'pt':period_type, 'period':period, 'c':currency, 's':scale, 'vt':value_type}
        return ref_ph_dct
    
    def read_col_class(self, conn, cur):
        read_qry = """  SELECT rawdb_row_id, column_class FROM column_classification;  """
        cur.execute(read_qry)
        t_data = cur.fetchall()
    
        col_class_dct = {}
        for row_data in t_data:
            rawdb_row_id, column_class = row_data
            col_class_dct.setdefault(rawdb_row_id, []).append(column_class)
        return col_class_dct 
        
    def read_mapping_info_for_grid(self, company_id, project_id, doc_id):
        db_path = config.Config.mapping_path.format(company_id, project_id, doc_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT db_row_id FROM mapping_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        mapping_rid_dct = {int(e[0]):1 for e in t_data} 
        return mapping_rid_dct
        
        
    def read_formula_cells(self, conn, cur):
        read_qry = """ SELECT distinct(rawdb_row_id) FROM formula_table; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        
        formula_rids = {}
        for row_data in t_data:
            formula_rids[row_data[0]] = 1
        return formula_rids
        
    def read_data_builder_info(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        table_type = ijson['table_type']

        if ijson.get('tab', '').lower() == 'builder':    
            db_path = config.Config.databuilder_path_test.format(company_id, project_id)
        else:
            db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        lc_d    = {} 
        super_key_flg   = {}
        if 1: #THIS NEED TO CHANGED BY TABLE TYPE
            sql = "select type, rawdb_row_id, label from label_changes where table_type=%s"%(table_type)
            cur.execute(sql)
            t_data = cur.fetchall()
            for r in t_data:
                ttype, rawdb_row_id, label  = r
                lc_d.setdefault(ttype, {})[int(rawdb_row_id)]   = label
            #sql = "select table_type, db_row_id, super_key from super_key_info where table_type=%s"%(ijson['table_type'])
            #cur.execute(sql)
            #t_data = cur.fetchall()
            #for r in  t_data:
            #    table_type, db_row_id, super_key   = r
            #    super_key_flg[int(db_row_id)] = ('G', super_key)
        try:
            alter_stmt = """ ALTER TABLE data_builder ADD COLUMN copy_cell TEXT; """
            cur.execute(alter_stmt)
            alter_stmt = """ ALTER TABLE data_builder ADD COLUMN super_key_poss TEXT; """
            cur.execute(alter_stmt)
        except:pass
        
        read_qry = """ SELECT row_id, taxo_group_id, src_row, src_col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id, copy_cell FROM data_builder WHERE table_type='%s'; """%(table_type)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        #print '---', len(t_data)

        ref_data, all_docs = {}, {} #self.read_db_ref_data(conn, cur)
        phcsv_dct = {} #self.read_db_phcsv(conn, cur)         
        col_class_dct = {} #self.read_col_class(conn, cur)
        try:
            formula_rids = {} #self.read_formula_cells(conn, cur)
        except:formula_rids = {}

        mapping_rid_dct = {}
        for dc in all_docs:
            try:
                map_dct = {} #self.read_mapping_info_for_grid(company_id, project_id, dc)
                mapping_rid_dct.update(map_dct)
            except:continue
        
        sn = 1
        res_dct = {}
        collect_col_wise_grp = {}
        group_wise_rids_dct = {}
        for row_data in t_data: 
            row_id, taxo_group_id, row, col, value, cell_type, cell_ph, formula_flag, super_key, super_key_poss, table_id, copy_cell = row_data
            res_dct.setdefault(int(row), {})[int(col)] = {'v':value, 'row_id':row_id, 'tx_id':taxo_group_id, 'ct':cell_type, 'cell_ph':cell_ph, 'ff':formula_flag, 't':table_id, 'c_cel':copy_cell}
            if 1:#cell_type != 'HGH':
                collect_col_wise_grp.setdefault(int(col), {})[taxo_group_id] = 1
                group_wise_rids_dct.setdefault(taxo_group_id, {})[row_id] = 1
            if super_key:# and (int(row) not in super_key_flg):
                super_key_flg[int(row)] = ('G', super_key)
            elif super_key_poss:# and (int(row) not in super_key_flg):
                super_key_flg[int(row)] = ('O', super_key_poss)
        
        group_range_dct = {}
        for gp, r_id_dct in group_wise_rids_dct.iteritems():
            for r_id in r_id_dct:
                cc_lst = col_class_dct.get(r_id, [])
                #print 'CCCCCCCCC', cc_lst
                for prg in cc_lst:
                    if prg in ('range', 'group'):
                        group_range_dct.setdefault(gp, {})[prg] = r_id
            
        
        sn = 1
        data_lst = [] 
        rows = sorted(res_dct.keys()) 
        collect_ph = {}
        inf_map = {}
        hgh_col = {}
        dis_rids = {}
        for row in rows: 
            col_dct = res_dct[row]
            row_dct = {'sn':{'v':sn}, 'rid':sn, 'cid':sn,'db_rid':row}
            # {'v':value, 'row_id':row_id, 'tx_id':taxo_group_id}
            cols = sorted(col_dct.keys())
            #for col, vl_dct in col_dct.iteritems():
            #    val = vl_dct[''] 
            rlc_d    = {}
            row_d   = lc_d.get('ROW', {})
            col_d   = {}
            for col in cols:
                vl_dct = col_dct[col]
                val = vl_dct['v'] 
                tab_row_id = vl_dct['row_id']
                lchane  = row_d.get(tab_row_id, '')
                tx_id   = vl_dct['tx_id']
                c_type  = vl_dct['ct']
                c_ph    = vl_dct['cell_ph']

                if lchane:
                    col_d[tx_id]    = int(col)
                    rlc_d.setdefault(tx_id, {})[lchane]   = (tab_row_id, c_ph)
                ff      = vl_dct['ff']
                t_b_id  = vl_dct['t']
                col_class_lst = col_class_dct.get(tab_row_id, [])
                col_desc = col
                p_dct = phcsv_dct.get(tab_row_id, {})
                p_dct['t'] = t_b_id
                #ph = '%s%s'%(p_dct.get('pt', ''), p_dct.get('p', ''))
                if 1:#c_type == 'HGH':
                    #col_desc = 'desc'
                    collect_ph.setdefault(col, set()).add(c_ph)
                if c_type == 'HGH': 
                    hgh_col[col] = 1
                val_dct_i = {'v':val, 'title':p_dct}
                cls_info_lst = []
                if int(tab_row_id) in mapping_rid_dct:
                    #val_dct_i['cls'] = 'tagged-cells'
                    cls_info_lst.append('tagged-cells')
                #if int(tab_row_id) in formula_rids:
                    #val_dct_i['fls'] = 'formula-info'
                if ff == 'Y':
                    cls_info_lst.append('resultant-formula')
                if vl_dct['c_cel']:
                    cls_info_lst.append('copy_'+vl_dct['c_cel'])
                cls_info = ' '.join(cls_info_lst)
                if cls_info:
                    val_dct_i['cls'] = cls_info        
    
                dis_rids[int(tab_row_id)] = 1
                row_dct[col_desc] = val_dct_i
                inf_map['%s_%s'%(sn, col_desc)] = {'ref_k':tab_row_id, 1:col_class_lst}
            if rlc_d:
                lc_ar   = []
                grpnames = rlc_d.keys()
                grpnames.sort(key=lambda x:col_d[x])
                for grp in grpnames:
                    l_ar    = []
                    for l, rtup in rlc_d[grp].items():
                        l_ar.append({'l':l, 'rid':rtup[0], 'ph':rtup[1]})
                    lc_ar.append({'g':grp, 'data':l_ar})
                row_dct['lchane']   = lc_ar
                row_dct['lc_f']   = {'v':'O'}
            if row in super_key_flg:
                row_dct['skey_f']   = {'v':super_key_flg[row][0]}
                if super_key_flg[row][0] == 'G':
                    row_dct['skey']   = super_key_flg[row][1]
                elif super_key_flg[row][0] == 'O':
                    row_dct['skey_poss']   = super_key_flg[row][1]
            else:
                row_dct['skey_f']   = {'v':'GR'}
                
            data_lst.append(row_dct)
            sn += 1
         
        #col_def_lst = [{'k':'desc', 'n':'Description', 'g':''}]
        col_def_lst = [{'v_opt': 3, 'k': "checkbox", 'n': "",'pin':'pinnedLeft'}, {'k':'sn', 'n':'S NO','type':"SL", 'pin':'pinnedLeft'}, {'k':'lc_f', 'n':'LC Flg', 'v_opt':2,'pin':'pinnedLeft'}, {'k':'skey_f', 'n':'S.Key Flg', 'v_opt':2,'pin':'pinnedLeft'}]
        all_cols = sorted(collect_col_wise_grp.keys())
        for c_l in all_cols:
            g_i_str = ' '.join(collect_col_wise_grp[c_l])
            p_h_lst = collect_ph[c_l] 
            p_h = ''
            if col not in hgh_col:
                p_h = '~'.join(p_h_lst)
            k_dct = {'k':c_l, 'n':p_h, 'g':g_i_str}
            col_def_lst.append(k_dct)
        res = [{'message':'done', 'data':data_lst, 'col_def':col_def_lst, 'map':inf_map, 'ttype_d':group_range_dct}]
        return res 
    def read_db_ref_info_skey(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        row_id = ijson['rid']
        if ijson.get('tab', '').lower() == 'builder':
            db_path = config.Config.databuilder_path_test.format(company_id, project_id)
        else:
            db_path = config.Config.super_key_db_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        ref_ph_dct = self.read_ref_rid(conn, cur, row_id)    
        ref_path = self.ref_path_info_workspace(company_id)
        ref_ph_dct['ref_path'] = ref_path
        res = [{'message':'done', 'data':ref_ph_dct}]
        return res
    
    def read_db_ref_info(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        sk_flg = ijson.get('sk_flg', 0)
        row_id = ijson['rid']
        if not sk_flg:
            if ijson.get('tab', '').lower() == 'builder':
                db_path = config.Config.databuilder_path_test.format(company_id, project_id)
            else:
                db_path = config.Config.databuilder_path.format(company_id, project_id)
        elif sk_flg:
            db_path = config.Config.super_key_db_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        ref_ph_dct = self.read_ref_rid(conn, cur, row_id)    
        ref_path = self.ref_path_info_workspace(company_id)
        ref_ph_dct['ref_path'] = ref_path
        res = [{'message':'done', 'data':ref_ph_dct}]
        return res
        
    def data_builder_stats(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']

        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
            
        read_qry = """ SELECT db.row_id, db.table_type, rt.doc_id  FROM data_builder AS db INNER JOIN reference_table AS rt ON db.row_id=rt.rawdb_row_id WHERE cell_type='GV'; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        
        table_type_rid_dct = {}
        all_docs = {}
        for row_data in t_data:
            rid, table_type, doc_id  = row_data
            table_type_rid_dct.setdefault(table_type, set()).add(rid) 
            all_docs[doc_id] = 1

        mapping_rid_dct = {}
        for dc in all_docs:
            try:
                map_dct = self.read_mapping_info_for_grid(company_id, project_id, dc)
                mapping_rid_dct.update(map_dct)
            except:continue
            
        color_tt_dct = {}
        for tt, rid_set in table_type_rid_dct.iteritems():
            if all(x in mapping_rid_dct for x in rid_set):
                color_tt_dct[tt] = 'green'
            elif any(x in mapping_rid_dct for x in rid_set):
                color_tt_dct[tt] = 'orange'
            else:
                color_tt_dct[tt] = 'red'
        #res = [{'message':'done', 'data':color_tt_dct}]
        return color_tt_dct
        
    def read_all_tables(self, company_id):
        db_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(db_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, page_no, grid_id FROM table_mgmt; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        
        all_docs = {} 
        all_grids = {}
        doc_table_dct = {}
        table_xml_rc = {}
        dct_rc_cnt = {}
        for row_data in t_data: 
            doc_id, page_no, grid_id = row_data
            all_docs[doc_id] = 1
            grid_id = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
            all_grids[grid_id] = 1
            doc_table_dct.setdefault(doc_id, {})[grid_id] = 1
            grid_json = self.read_json_file(grid_id, company_id)
            tab_row, tab_col, tab_rc, tab_xml_rc_dct = self.info_gv_count_stats(grid_json, grid_id)#self.gv_count_stats(grid_json)
            table_xml_rc.update(tab_xml_rc_dct)
            dct_rc_cnt[grid_id] = (tab_row, tab_col, tab_rc)
        return all_docs, all_grids, doc_table_dct, table_xml_rc, dct_rc_cnt

    def gv_count_stats(self, json_dict):
        if not json_dict:
            return 0
        rc_dct = json_dict['data']
        all_cols = {}
        all_rows = {}
        all_rc = {}
        for rc, cell_dct in rc_dct.iteritems():
            if cell_dct['ldr'] != 'value':continue
            row, col = rc.split('_')
            all_cols[col] = 1
            all_rows[row] = 1
            rc = '{0}_{1}'.format(row, col)
            all_rc[rc] = 1
        return all_rows, all_cols, all_rc

    def info_gv_count_stats(self, json_dict, table_id):
        if not json_dict:
            return 0
        rc_dct = json_dict['data']
        all_cols = {}
        all_rows = {}
        all_rc = {}
        tab_xml_rc_dct = {}
        for rc, cell_dct in rc_dct.iteritems():
            if cell_dct['ldr'] not in ('value', ):continue
            xml_id= '#'.join(cell_dct['xml_ids'].split('$$'))
            row, col = rc.split('_')
            all_cols[col] = 1
            all_rows[row] = 1
            rc = '{0}_{1}'.format(row, col)
            all_rc[rc] = 1
            tab_xml_rc_dct[(table_id, xml_id)] = rc
        return all_rows, all_cols, all_rc, tab_xml_rc_dct
        
    def read_data_builde_info(self, company_id, project_id, table_xml_rc):
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT db.table_type, db.src_row, db.src_col, rt.doc_id, rt.table_id, rt.xml_id FROM data_builder AS db INNER JOIN reference_table AS rt ON db.row_id =rt.rawdb_row_id WHERE db.cell_type='GV'; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()        
    
        doc_wise_cols = {}
        table_wise_cols = {}
        all_builder_grids = {}
        tt_rc_cnt = {} 
        table_rc_db_info = {}
        tt_wise_table_id = {}
        for row_data in t_data:
            table_type, row, col, doc_id, table_id, xml_id = row_data
            rc = table_xml_rc.get((table_id, xml_id), '')
            #rc = '{0}_{1}'.format(row, col)
            doc_wise_cols.setdefault(doc_id, {}).setdefault(table_id, {}).setdefault('C', {})[col] = 1
            doc_wise_cols.setdefault(doc_id, {}).setdefault(table_id, {}).setdefault('R', {})[row] = 1
            doc_wise_cols.setdefault(doc_id, {}).setdefault(table_id, {}).setdefault('RC', {})[rc] = 1
            all_builder_grids[table_id] = 1
            tt_rc_cnt.setdefault(table_type, {}).setdefault('R', {})[row] = 1
            tt_rc_cnt.setdefault(table_type, {}).setdefault('C', {})[col] = 1
            tt_rc_cnt.setdefault(table_type, {}).setdefault('RC', {})[rc] = 1
            table_rc_db_info.setdefault(table_id, {})[rc] = 1
            tt_wise_table_id.setdefault(table_type, {}).setdefault(table_id, {})[rc] = 1

        doc_col_cnt = {}
        table_col_cnt = {}
        for doc, table_dct in doc_wise_cols.iteritems():
            for table, rc_dct in table_dct.iteritems():
                r_info, c_info, rc_info = rc_dct['R'], rc_dct['C'], rc_dct['RC']
                ln_row = len(r_info)
                ln_col = len(c_info)
                ln_rc  = len(rc_info)
                #doc_col_cnt.setdefault(doc, {'R':0, 'C':0, 'RC':0})
                #table_col_cnt.setdefault(table, {'R':0, 'C':0, 'RC':0})
                doc_col_cnt[doc]['R'] = doc_col_cnt.setdefault(doc, {}).setdefault('R', 0) + ln_row                
                doc_col_cnt[doc]['C'] = doc_col_cnt.setdefault(doc, {}).setdefault('C', 0) + ln_col                
                doc_col_cnt[doc]['RC'] = doc_col_cnt.setdefault(doc, {}).setdefault('RC', 0) + ln_rc                

                table_col_cnt[doc]['R'] = table_col_cnt.setdefault(doc, {}).setdefault('R', 0) + ln_row                
                table_col_cnt[doc]['C'] = table_col_cnt.setdefault(doc, {}).setdefault('C', 0) + ln_col                
                table_col_cnt[doc]['RC'] = table_col_cnt.setdefault(doc, {}).setdefault('RC', 0) + ln_rc                
        return doc_col_cnt, table_col_cnt, all_builder_grids, tt_rc_cnt, table_rc_db_info, tt_wise_table_id
        

    def grids_get_doc_table_stats(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        
        all_docs, all_grids, doc_table_dct, table_xml_rc, dct_table_rc_cnt = self.read_all_tables(company_id)

        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        #print db_path 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        #db_path = config.Config.databuilder_path.format(company_id, project_id)
        read_qry = """ SELECT doc_id, table_id FROM scoped_gv; """ 
        cur.execute(read_qry)
        t_data = cur.fetchall()
        
        doc_tab_dct_scoped = {}
        scoped_table_dct = {}
        for row_data in t_data:
            doc_id, table_id = row_data
            doc_tab_dct_scoped.setdefault(doc_id, {})[table_id] = 1
            scoped_table_dct[table_id] = 1
    
        #print len(scoped_table_dct)
        
        doc_rc_cnt, table_rc_cnt, all_builder_grids, tt_rc_cnt, table_rc_db_info, tt_wise_table_id = self.read_data_builde_info(company_id, project_id, table_xml_rc)
        
        type_flg = ijson.get('type_flg', 'company')
        if type_flg == 'table_type':
            tab_type = ijson['stats_table_type']
            tab_tt_get = tt_wise_table_id.get(str(tab_type), {})
            #print tab_tt_get
             
        doc_wise_col_count = {}        
        table_wise_col_count = {} 
        
        doc_stats = {}
        doc_stats_format = {}
        company_gvs = 0 
        scoped_gv_cnt = 0
        db_gv_cnt = 0
        rem_db_cnt_from_scope = 0
        rem_db_grids_from_scope = {}
        db_table_all_gvs = 0
        db_left_grids = {}
        db_table_insert_grids = 0
        scoped_table_info_dct = {}
        all_table_ids = {}
        db_table_ids = {}
        for dc, table_dct in doc_table_dct.iteritems():
            if type_flg == 'document':
                did = ijson['stats_doc_id']
                if did != dc:continue
            for tab in table_dct:
                if type_flg == 'table_type':
                    if tab not in tab_tt_get:continue
                grid_json = self.read_json_file(tab, company_id)
                tab_row, tab_col, tab_rc = dct_table_rc_cnt[tab]#self.gv_count_stats(grid_json)
                ln_tb_row = len(tab_row)
                ln_tb_col = len(tab_col)
                ln_tb_rc  = len(tab_rc)
                all_table_ids[tab] = 1         

                if tab in scoped_table_dct:
                    scoped_table_info_dct[tab] = 1 
                    scoped_gv_cnt += ln_tb_rc 

                if tab in table_rc_db_info:
                    db_table_ids[tab] = 1
                    db_table_all_gvs += ln_tb_rc
                    db_table_grids = table_rc_db_info.get(tab, {})
                    db_table_insert_grids += len(db_table_grids)
                    diff_db_gv = ln_tb_rc - len(db_table_grids)
                    if diff_db_gv:
                        db_left_grids[tab] = 1   
                        #if tab == '1_54_1':
                        #    print 'PPPPPPPPPPPPPP',[ln_tb_rc, db_table_grids, diff_db_gv]
        #print db_left_grids
        
        over_all_tables = len(all_table_ids)
        scoped_tables = len(scoped_table_info_dct)
        scoped_percent = round((float(scoped_tables)/float(over_all_tables))*100, 2)
        
        rem_scop_tabs = list(set(all_table_ids) - set(scoped_table_info_dct))
        len_remscope_tables = len(rem_scop_tabs)
        rem_scope_pcent  = round((float(len_remscope_tables)/float(over_all_tables))*100, 2)
        
        builder_cnt_grids  = len(db_table_ids) 
        builder_grid_pcent = round((float(builder_cnt_grids)/float(over_all_tables))*100, 2)
        
        rem_build_list = list(set(scoped_table_info_dct) - set(db_table_ids))
        remaining_builder_grids = len(rem_build_list)
        try:
            remain_builder_cnt_percent = round((float(remaining_builder_grids)/float(scoped_tables))*100, 2) 
        except:remain_builder_cnt_percent = 0

        list_all_grids = sorted(all_table_ids.keys(), key=lambda x:tuple(map(int, x.split('_'))))
        list_scoped_grids = sorted(scoped_table_info_dct.keys(), key=lambda x:tuple(map(int, x.split('_'))))
        list_remaining_scop_grid = sorted(rem_scop_tabs, key=lambda x:tuple(map(int, x.split('_'))))
        list_builder_grids   = sorted(db_table_ids.keys(), key=lambda x:tuple(map(int, x.split('_'))))
        list_rem_build_grids = sorted(rem_build_list, key=lambda x:tuple(map(int, x.split('_'))))
        list_db_left_grids = sorted(db_left_grids.keys(), key=lambda x:tuple(map(int, x.split('_'))))
        
        db_gv_left_cnt = db_table_all_gvs - db_table_insert_grids
        db_gv_pcent_out_all  = round((float(db_gv_left_cnt)/float(db_table_all_gvs))*100, 2)

        stats_data = [{'k':'total_grids', 'n':'Total Grids', 'c':over_all_tables, 'grids':list_all_grids, 'p':100}, {'k':'total_gv', 'n':'Total GVs', 'c':db_table_all_gvs, 'grids':list_builder_grids, 'p':100}, {'k':'rem_db_gv', 'n':'Leftover DB GV', 'c':db_gv_left_cnt, 'grids':list_db_left_grids, 'p':db_gv_pcent_out_all},{'k':'scoped', 'n':'Scoped', 'c':scoped_tables, 'grids':list_scoped_grids, 'p':scoped_percent}, {'k':'remaining', 'n':'Not Scoped', 'c':len_remscope_tables, 'grids':list_remaining_scop_grid, 'p':rem_scope_pcent}, {'k':'db_grids', 'n':'DB Grids', 'c':builder_cnt_grids, 'grids':list_builder_grids, 'p':builder_grid_pcent}, {'k':'db_remain', 'n':'DB Remain', 'c':remaining_builder_grids, 'grids': list_rem_build_grids, 'p':remain_builder_cnt_percent}] 
        res = [{'message':'done', 'stats_data':stats_data}]
        return res 

    def delete_scoped(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        doc_id     = ijson['doc_id']
        page_no    = ijson['page_no']
        grid_id    = ijson['grid_id']
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        del_stmt = """ DELETE FROM scoped_gv WHERE info_type='table' AND doc_id=%s AND page_no=%s AND grid_id=%s; """%(doc_id, page_no, grid_id) 
        cur.execute(del_stmt)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res

    def read_data_builde_info_table_id(self, company_id, project_id, doc_id, page_no, grid_id, xml_info_dct):
        table_id = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT db.src_row, db.src_col, rt.table_id, rt.xml_id  FROM data_builder AS db INNER JOIN reference_table AS rt ON db.row_id =rt.rawdb_row_id WHERE rt.table_id='%s' AND db.cell_type='GV'; """%(table_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()        
        
        db_rc = {}
        for row_data in t_data:
            row, col, table_id, xml_id = row_data
            rc  = xml_info_dct.get((table_id, xml_id))
            if not rc:continue
            db_rc[rc] = 1 
        return db_rc
        
    def read_rem_rc_table(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        doc_id     = ijson['doc_id']
        page_no    = ijson['page_no']
        grid_id    = ijson['grid_id']
        tab = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
        grid_json = self.read_json_file(tab, company_id)
        tab_row, tab_col, tab_rc, xml_info_dct = self.info_gv_count_stats(grid_json, tab)
        db_rc = self.read_data_builde_info_table_id(company_id, project_id, doc_id, page_no, grid_id, xml_info_dct)
        print db_rc
        data_rc =  list(set(tab_rc)-set(db_rc))
        res = [{'message':'done', 'data':data_rc}]
        return res

    def read_data_builder_classified_info(self, company_id, project_id, clasified_id_dct):
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT db.table_type, rt.table_id  FROM data_builder AS db INNER JOIN reference_table AS rt ON db.row_id =rt.rawdb_row_id; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()        
        res_dct = {}
        for row_data in t_data:
            table_type_id, table_id = row_data
            table_type_id = str(table_type_id) 
            desc = clasified_id_dct.get(table_type_id, '')
            if not desc:continue
            res_dct[table_id] = [table_type_id, desc]
        return res_dct
        
    def read_global_table_type_info(self, ijson):
        company_id = ijson['company_id']
        project_id  = ijson['project_id']

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
        
        db_tabl_class = self.read_data_builder_classified_info(company_id, project_id, clasified_id_dct)
        
        #cid_path = config.Config.company_id_path.format(company_id, project_id)
        #db_path = os.path.join(cid_path, 'table_info.db')
        #conn, cur   = conn_obj.sqlite_connection(db_path)
        #read_qry = """ SELECT classified_id, doc_id, page_no, grid_id FROM table_mgmt WHERE highly_connected='Y'; """
        #cur.execute(read_qry)
        #t_data = cur.fetchall()
        #conn.close()
        #table_dct = {}
        #for rd in t_data:   
        #    classified_id, doc_id, page_no, grid_id = rd
        #    
        #    tt = clasified_id_dct.get(classified_id, '')
        #    if not tt:continue
        #    tab_id = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
        #    table_dct[tab_id] = [classified_id, tt]
        res = [{'message':'done', 'data':db_tabl_class}]
        return res
        
    def read_doc_meta_data(self, ijson):
        company_id = ijson['company_id']
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'document_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, doc_name, doc_type, period_type, period, filing_type, meta_data FROM document_meta_info;  """        
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        doc_lst = []
        for row_data in t_data:
            doc_id, doc_name, doc_type, period_type, period, filing_type, meta_data  = row_data
            ph = '{0}{1}'.format(period_type, period)
            n_key = '{0}_{1}'.format(ph, doc_type)
            doc_inf_dct =  {'k':doc_id, 'n':n_key, 'ph':ph, 'ft':filing_type, 'd_t':doc_type, 'd_n':doc_name}
            doc_lst.append(doc_inf_dct)
        doc_lst.sort(key=lambda x:x['k'])
        res = [{'message':'done', 'data':doc_lst}]
        return res

    def read_hgh_text_index_wise(self, company_id, doc_id):
        hght_txt_path = '/mnt/eMB_db/company_management/{0}/equality/{1}_lexacross.slv'.format(company_id, doc_id)
        sh = shelve.open(hght_txt_path)
        data = sh['data']
        sh.close()
        hgh_txt_lst = data['all_tree_node']
        idx_wise_text = {}
        for tpl in hgh_txt_lst:
            hgh_txt_idx = tpl[0].strip()
            #print [hgh_txt_idx]
            idx, hgh_txt = hgh_txt_idx.split('@')
            idx_wise_text[idx] = hgh_txt
        return idx_wise_text 
        
    def read_taxo_shelve_file(self, company_id, doc_id):
        shl_path = '/mnt/eMB_db/company_management/{0}/equality/lextree_{1}.slv'.format(company_id, doc_id)
        print shl_path
        sh = shelve.open(shl_path)
        data = sh['data']
        sh.close() 
        return data
        
    def read_index_info(self, ijson):
        company_id = ijson['company_id']
        table_id   = ijson['table_id']
        doc_id, page_no, grid_id   = table_id.split('#')
        idx_wise_text_dct =  self.read_hgh_text_index_wise(company_id, doc_id) 
        print idx_wise_text_dct
        
        slv_data = self.read_taxo_shelve_file(company_id, doc_id)
        rc_info = {}
        
        map_inf = {}
        data_lst = []
        sn = 1
        for tid_str, rc_dct in slv_data.iteritems(): 
            if table_id not in tid_str:continue
            for rc_tup, rc_idx_lst in rc_dct.iteritems():
                rc_str = '%s_%s'%rc_tup
                rc_info[rc_str] = 1
                txt_lst = []   
                idx_arr = []
                for id_tup in rc_idx_lst:
                    txt_str = ' '.join(id_tup[0])
                    txt_lst.append(txt_str)
                    idx_lst = id_tup[5]
                    for idx in idx_lst:
                        idx_txt = idx_wise_text_dct.get(idx, '')
                        idx_arr.append((idx, idx_txt))
                rc_head_txt = ' '.join(txt_lst)
                rc_row_dct = {'rid':sn, 'cid':sn, 0:{'v':rc_str, 'cls': 'grid-header'}, 1:{'cls':'grid-header', 'v':rc_head_txt}, '$$treeLevel': 0}
                data_lst.append(rc_row_dct)
                sn += 1
                for ix_tp in idx_arr:
                    ix, ix_txt = ix_tp
                    idx_row_dct = {'rid':sn, 'cid':sn, 0:{'v':ix}, 1:{'v':ix_txt}, '$$treeLevel': 1}
                    data_lst.append(idx_row_dct)
                    sn += 1
                emp_row_dct = {'rid':sn, 'cid':sn, 0:{'v':''}, 1:{'v':''}, '$$treeLevel': 0}
                data_lst.append(emp_row_dct)
                sn +=1
        col_def_lst = [{'k':0, 'n':'Description', 'v_opt':1}, {'k': 1, 'n': "A"}]
        res = [{'message':'done', 'data':data_lst, 'coldef':col_def_lst, 'map':map_inf, 'rcinfo':rc_info}]
        return res

    def update_table_type(self, ijson):
        company_id = ijson['company_id']
        project_id  = ijson['project_id']
        table_type  = ijson['table_type']
        short_form   = ijson['short_form']
        row_id      = ijson['row_id']
        db_path = config.Config.gl_db 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        update_stmt = """ UPDATE all_table_types SET table_type='%s', short_form='%s' WHERE row_id=%s; """%(table_type, short_form, row_id)
        cur.execute(update_stmt)
        conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res
        
    def read_data_type_info(self, ijson):
        project_id = ijson['project_id']
        dir_path = '/mnt/eMB_db/company_management/{0}/'.format(project_id)
        if not os.path.exists(dir_path):
            os.system('mkdir -p {0}'.format(dir_path))
        db_path = os.path.join(dir_path, 'global.db')            
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT field_name, key, value, target_key FROM data_type_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall() 
        conn.close()        
        
        res_dct = {}
        for row_data in t_data:
            field_name, key, value, target_key = row_data   
            #print field_name
            if not target_key:
                res_dct.setdefault(field_name, []).append({'k':key, 'n':value})
            if target_key:
                res_dct.setdefault(field_name, {}).setdefault(target_key, []).append({'k':key, 'n':value})
        res = [{'message':'done', 'data':res_dct}]    
        return res
        
    def add_data_type_from_interface(self, ijson):
        project_id = ijson['project_id']
        field_name  = ijson['field_name']
        key         = ijson['k']
        value       = ijson['n'] 
        target_key  = ijson.get('target_key', '')
             
        dir_path = '/mnt/eMB_db/company_management/{0}/'.format(project_id)
        if not os.path.exists(dir_path):
            os.system('mkdir -p {0}'.format(dir_path))
        db_path = os.path.join(dir_path, 'global.db')            
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT field_name, key, value, target_key FROM data_type_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall() 
            
        d_tup = (field_name, key, value, target_key)
        if d_tup not in t_data:
            insert_stmt = """ INSERT INTO data_type_info(field_name, key, value, target_key) VALUES('%s', '%s', '%s', '%s'); """%d_tup
            cur.execute(insert_stmt)
            conn.commit()
            res = [{'message':'done'}]
        else:
            res = [{'message':'Already Exists'}]
        conn.close()    
        return res
        
    def read_db_name(self, company_id):
        m_cur, m_conn = conn_obj.MySQLdb_conn(config.Config.inc_project_info) 
        read_qry = """ SELECT ProjectCode FROM ProjectMaster WHERE ProjectID='%s'; """%(company_id)
        m_cur.execute(read_qry) 
        t_data = m_cur.fetchone()
        m_conn.close()
        inc_db_name = t_data[0]
        return inc_db_name
        
    def read_doc_meta_from_inc(self, doc_id_lst, db_name):
        doc_str = ', '.join(map(str, doc_id_lst))
        inc_host_dct = config.Config.inc_host_info
        inc_host_dct['db'] = db_name
        m_cur, m_conn = conn_obj.MySQLdb_conn(inc_host_dct) 
        read_qry = """ SELECT doc_id, doc_name, doc_type, meta_data FROM batch_mgmt_upload WHERE doc_id in (%s); """%(doc_str)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        meta_data_lst = []
        for row_data in t_data:
            doc_id, doc_name, doc_type, meta_data = row_data
            if meta_data:
                meta_data = eval(meta_data)
            pt = meta_data.get('periodtype', '')
            year = meta_data.get('Year', '')
            filling_type = meta_data.get('FilingType', '')
            fye = meta_data.get('FY', '')
            meta_data = json.dumps(meta_data)
            dt_tup = (doc_id, doc_name, doc_type, pt, year, filling_type, fye, meta_data)
            meta_data_lst.append(dt_tup)
        return meta_data_lst

    def update_document_meta_info(self, ijson):
        company_id = ijson['company_id']
        doc_id_lst     = ijson['doc_ids']
        inc_db_name = self.read_db_name(company_id)
        meta_data_lst = self.read_doc_meta_from_inc(doc_id_lst, inc_db_name)
        doc_str = ', '.join(map(str, doc_id_lst))
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'document_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        crt_stmt = """ CREATE TABLE IF NOT EXISTS document_meta_info(row_id INTEGER PRIMARY KEY AUTOINCREMENT, doc_id INTEGER NOT NULL, doc_name TEXT, doc_type TEXT, period_type VARCHAR(32), period VARCHAR(32), filing_type VARCHAR(32), fye VARCHAR(256), meta_data TEXT); """
        cur.execute(crt_stmt)
        try:
            del_stmt = """ DELETE FROM document_meta_info WHERE doc_id in (%s); """%(doc_str)
            cur.execute(del_stmt)
        except:pass 
        
        if meta_data_lst:
            insert_stmt = """ INSERT INTO document_meta_info(doc_id, doc_name, doc_type, period_type, period, filing_type, fye, meta_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?); """
            cur.executemany(insert_stmt, meta_data_lst)
            conn.commit()
        conn.close()
        res = [{'message':'done'}]
        return res 

    def read_table_shelve_file(self, shl_path):
        sh = shelve.open(shl_path)
        data = sh['data']
        sh.close() 
        return data
        
    def merge_common(self, lists): 
        neigh = defaultdict(set) 
        visited = set() 
        for each in lists: 
            for item in each: 
                neigh[item].update(each) 
        def comp(node, neigh = neigh, visited = visited, vis = visited.add): 
            nodes = set([node]) 
            next_node = nodes.pop 
            while nodes: 
                node = next_node() 
                vis(node) 
                nodes |= neigh[node] - visited 
                yield node 
        for node in neigh: 
            if node not in visited: 
                yield sorted(comp(node))
            
    def group_table_for_doc(self, company_id, doc_id, grp_wise_tables_dct):
        db_path = os.path.join(config.Config.comp_path.format(company_id), 'table_info.db')
        print db_path
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT group_id, table_id FROM group_tables WHERE doc_id={0}; """.format(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        #grp_wise_tables_dct = {}
        for rd in t_data:
            group_id, table_id = rd
            grp_wise_tables_dct.setdefault(group_id, {})[table_id] = 1
        
            #grp_table_lst = []
            #for grp, t_dct in grp_wise_tables_dct.iteritems():
            #    grp_table_lst.append(t_dct.keys())
            #return grp_table_lst
        
    def create_groups_tables(self, ijson):
        company_id = ijson['company_id'] 
        doc_id     = ijson['doc_id']
        table_ids  = map(lambda x:'_'.join(x.split('#')), ijson['table_ids'])
        dist_docs  = {t.split('_')[0] for t in table_ids} 
           
        #print dist_docs 
        arr_tables = []
        grp_wise_tables_dct = {}
        for dc in dist_docs:
            #sh_path    = config.Config.table_group_sh.format(company_id, dc)
            #at  = self.read_table_shelve_file(sh_path)
            try:
                self.group_table_for_doc(company_id, dc, grp_wise_tables_dct)
            except:pass
        print grp_wise_tables_dct
        #info = list(self.merge_common(arr_tables))
        #print arr_tables
        #import form_perm_comb
        #info = form_perm_comb.form_perm_comb().form_perm_comb(arr_tables)
        #info = copy.deepcopy(arr_tables)        
        res_lst = []
        for grp, grp_tabl_dct in grp_wise_tables_dct.iteritems():
            row_set = set(table_ids).intersection(set(grp_tabl_dct)) 
            if not row_set:continue
            row_list = list(row_set)
            res_lst.append(row_list) 

        '''
        res_lst = [] 
        done_d  = {}
        for group_list in  info:
            dt = list(set(table_ids).intersection(set(group_list)))
            if not dt:continue
            print 'DDDDDDDDDDDDD', dt
            for elm in dt:
                done_d[elm]  = 1
            res_lst.append(dt) 
        for elm in table_ids:
            if elm in done_d:continue
            res_lst.append([elm])
        '''
       
        res = [{'message':'done', 'data':res_lst}] 
        return res
    
    def generate_rules(self, ijson):
        ijson['return_rule'] = 'Y'
        sys.path.insert(0, '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')        
        import populate_table_lets_DB as PD
        p_Obj = PD.Populate()  
        disableprint()
        f_ar, f_individual_ar, error_d = p_Obj.read_traverse_path(ijson)
        enableprint()
        res = [{'message':'done', 'data':f_ar, 'f_i':f_individual_ar, 'error_d':error_d}]
        return res
        

    def read_table_type_name(self):
        db_path = '/mnt/eMB_db/company_management/global_info.db' 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        r_qry = """ SELECT row_id, table_type  FROM all_table_types; """
        cur.execute(r_qry)
        mst_data = cur.fetchall()
        conn.close()
        
        tt_name_dct = {}
        for rw_dt in mst_data:
            row_id, table_type = rw_dt
            tt_name_dct[str(row_id)] = table_type
        return tt_name_dct
        
    def read_data_builder_tt(self, company_id, project_id):
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT distinct(table_type) FROM data_builder; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        db_tt_dct = {}
        for rd in t_data:
            db_tt_dct[str(rd[0])] = 1
        return db_tt_dct

    def table_type_flag_grid_module(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        
        db_tt_dct = self.read_data_builder_tt(company_id, project_id)
        tt_name_dct = self.read_table_type_name()

        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT distinct(classified_id) FROM scoped_gv; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        inf_map = {}
        data_lst = []
        sn = 1
        for r_d in t_data:
            c_id = str(r_d[0])
            tt_name = tt_name_dct[c_id]
            row_dct = {'rid':sn, 'cid':sn, 'sn':{'v':sn}, 'tt':{'v':tt_name}, 'flg':{'v':'R'}, 't_id':c_id}
            inf_map['{0}_{1}'.format(sn, 'tt')] = {'ref_k':c_id}
            if c_id in db_tt_dct:
                row_dct['flg']['v'] = 'G'
            data_lst.append(row_dct)
            sn += 1
        col_def_lst = [{'k':'checkbox', 'n':'', 'v_opt':3}, {'k':'sn', 'n':'S.No', 'type':'SL'}, {'k':'tt', 'n':'Table Type'}, {'k':'flg', 'n':'Flag', 'v_opt':2}]
        res = [{'message':'done', 'data':data_lst, 'col_def':col_def_lst, 'map':inf_map}]
        return res

    def get_classified_tables_group(self, company_id, project_id, ijson):
        return self.get_classified_tables(company_id, project_id, ijson)
        dbpath      = config.Config.company_id_path.format(company_id, project_id)
        dbfile      = '%stable_info.db'%(dbpath)
        print 'DDDDDDDD', dbfile
        if not os.path.exists(dbfile):
            return {}
        conn, cur   = conn_obj.sqlite_connection(dbfile)
        sql         = "select table_id, group_id, classified_id from group_tables"
        try:
            cur.execute(sql)
            res         = cur.fetchall()
        except:
            res = []
        conn.close()
        dd  = {}
        for r in res:
            table_id, group_id, classified_id   = r
            dd[table_id]   = {'c_id':classified_id, 'taxo_info':[], 'ttype':''}
        return dd

    def get_classified_tables(self, company_id, project_id, ijson):
        dbpath      = config.Config.company_id_path.format(company_id, project_id)
        dbfile      = '%stable_info.db'%(dbpath)
        if 0:
            conn, cur   = conn_obj.sqlite_connection(dbfile)
            sql = "select table_id, classified_id from group_tables"
            try:
                cur.execute(sql)
                res = cur.fetchall()
            except:
                res = []
            conn.close()
            dd  = {}
            for r in res:
                table_id, classified_id = r
                dd[table_id]   = {'c_id':classified_id, 'taxo_info':'', 'ttype':''}
            return dd    
        if not os.path.exists(dbfile):
            return {}
        print 'DDDDDDDD', dbfile
        conn, cur   = conn_obj.sqlite_connection(dbfile)
        sql = "alter table scoped_gv add column taxo_info text"
        try:
            cur.execute(sql)
        except:pass
        sql         = "select row_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc,  f_type, formula_id, equality_id, traverse_path, level_id, info_type, r_type, table_type, classified_id, taxo_info, table_type from scoped_gv where doc_id in (%s)"%(ijson['doc_id'])
        sql         = "select row_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc, info_type, r_type, table_type, classified_id, taxo_info, table_type from scoped_gv where doc_id=%s"%(ijson['doc_id'])
        try:
            cur.execute(sql)
            res         = cur.fetchall()
        except:
            res = []
        conn.close()
        dd  = {}
        for r in res:
            row_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc,  info_type, r_type, tt, classified_id, taxo_info, table_type  = r
            if str(classified_id) == '-1':continue
            
            if info_type == 'table':
                tmptaxo_info    = []
                if taxo_info:
                    tmptaxo_info    = eval(taxo_info)
                dd['%s_%s_%s'%(doc_id, page_no, grid_id)]   = {'c_id':classified_id, 'taxo_info':tmptaxo_info, 'ttype':table_type}
        return dd


    def read_tables(self, company_id, doc_id, tableid_list=[]):
        import sqlite3
        db_path = '/mnt/eMB_db/company_management/{0}/table_info.db'.format(company_id)
        con         = sqlite3.connect(db_path, timeout=30000)
        cur         = con.cursor()
        sql         = "select doc_id, page_no, grid_id from table_mgmt where doc_id=%s"%(doc_id)
        cur.execute(sql)
        res         = cur.fetchall()
        done_d  = {}
        for r in res:
            doc_id, page_no, grid_id    = r
            table_id    = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)    
            if tableid_list:
                if table_id not in tableid_list:continue
            if table_id in done_d:continue
            json_file   = '/mnt/eMB_db/company_management/{0}/json_files/{1}.json'.format(company_id, table_id)
            grid_data   = json.loads(open(json_file, 'r').read())
            done_d[table_id]    = (int(page_no), (int(grid_data['table_boundry'][1]), int(grid_data['table_boundry'][1])+int(grid_data['table_boundry'][3])))
        table_ids   = done_d.keys()
        table_ids.sort(key=lambda x:done_d[x])
        #for r in table_ids:
        #    print r, done_d[r]
        return table_ids

        
    def read_group_tables(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        tscope_d    = self.get_classified_tables_group(company_id, ijson['project_id'], ijson)
        db_path = os.path.join(config.Config.comp_path.format(company_id), 'table_info.db')
        print db_path
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT group_id, table_id FROM group_tables""" #  WHERE doc_id='%s'; """%(doc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        #all_tabls  = self.read_tables(company_id, doc_id)  
        group_table_dct = {}
        for row_data in t_data:
            group_id, table_id = row_data
            group_table_dct.setdefault(group_id, []).append(table_id)
        
        data_lst = []   
        db_path = config.Config.gl_db
        conn, cur   = conn_obj.sqlite_connection(db_path)
        def get_class_name(c_id):
            sql = "select table_type from all_table_types where row_id=%s"%(c_id)
            cur.execute(sql)
            res = cur.fetchone()
            if res and res[0]:
                return res[0]
            return ''
        

        for grp, table_lst in  group_table_dct.iteritems():
            tmptlist    = filter(lambda x:x.split('_')[0] == str(ijson['doc_id']), table_lst)
            if not tmptlist:continue
            res_table = tmptlist[0]
            pg = '_'.join(res_table.split('_')[1:])
            #table_lst.sort(key=lambda x:all_tabls.index(x))
            row_dct = {'k':res_table, 'n':pg, 'g':grp, 'g_t':tmptlist}
            if not row_dct['g_t']:continue
            f   = 1
            td  = {}
            for tid in tmptlist:
                if tid not in tscope_d:
                    f = 0
                else:
                    td[tscope_d[tid]['c_id']]  = 1
            #print res_table, td, table_lst
            if td:
                if f == 1:
                    row_dct['flg'] = 'G'
                else:
                    row_dct['flg'] = 'O'
                if len(td.keys()) > 1:
                    row_dct['flg'] = 'P'
                row_dct['title']    = json.dumps(map(lambda x:get_class_name(x), td.keys()))
            #else:
            #    row_dct['flg'] = 'R'
            
            data_lst.append(row_dct)
        conn.close()
        data_lst.sort(key=lambda x:tuple(map(lambda x1:int(x1), x['k'].split('_'))))
        res = [{'message':'done', 'data':data_lst}]
        return res
        
    def table_gh_hvgh_cnt(self, company_id, table_id, gh_dct, hv_gh_dct):
        grid_json = self.read_json_file(table_id, company_id)
        json_dct = grid_json['data']
        for rc, cell_dct in json_dct.iteritems():
            trc = (table_id, rc)
            ldr = cell_dct.get('ldr', '')
            if ldr not in ('gh', 'hch', 'vch'):continue
            val_txt = cell_dct['data']
            val_txt_lst = val_txt.split()
            val_txt_l = ' '.join([e.strip() for e in val_txt_lst])
            val_txt_lower = val_txt_l.lower()
            if ldr == 'gh':
                #gh_dct[val_txt_lower] = [gh_dct.get(val_txt_lower, [0, '', {}])[0] + 1, val_txt, {}]
                gh_cnt, e_txt, tab_dct, t_rc_tup = gh_dct.get(val_txt_lower, [0, '', {}, trc])
                gh_cnt += 1
                tab_dct[table_id] = 1                
                gh_dct[val_txt_lower] = [gh_cnt, val_txt, tab_dct, t_rc_tup]                
            elif ldr in ('hch', 'vch'):
                #hv_gh_dct[val_txt_lower] = [hv_gh_dct.get(val_txt_lower, [0, ''])[0] + 1, val_txt, {}]
                vh_cnt, vhe_txt, vh_tab_dct, vh_trc_tup = hv_gh_dct.get(val_txt_lower, [0, '', {}, trc]) 
                vh_cnt += 1
                vh_tab_dct[table_id] = 1
                hv_gh_dct[val_txt_lower] = [vh_cnt, val_txt, vh_tab_dct, vh_trc_tup]               
        
    def redis_gh_vh(self, company_id, table_id, gh_dct, hv_gh_dct, obj):
        #print 'TTTTTTTTTTTTTT', table_id
        doc, page, grid = map(int, table_id.split('_'))
        rObj = pre.exe("172.16.20.7##6384##0", int(company_id), int(doc)) 
        for stype in ['gh', 'vch', 'hch']:
            info_lst = rObj.read_sction_data(page, grid, stype)
            if not info_lst:
                print 'IIIIIIIIIIIIIII', [table_id, info_lst]
            for cell_dct in info_lst:
                val_txt = cell_dct['txt']
                val_txt = obj.dt_sig(val_txt)
                print [val_txt, table_id], '\n'
                rc = cell_dct['rc']
                trc = (table_id, rc)
                val_txt_lst = val_txt.split()
                val_txt_l = ' '.join([e.strip() for e in val_txt_lst])
                val_txt_lower = val_txt_l.lower()
                if stype == 'gh':
                    #gh_dct[val_txt_lower] = [gh_dct.get(val_txt_lower, [0, '', {}])[0] + 1, val_txt, {}]
                    gh_cnt, e_txt, tab_dct, t_rc_tup = gh_dct.get(val_txt_lower, [0, '', {}, trc])
                    gh_cnt = gh_cnt + 1
                    tab_dct[table_id] = 1                
                    gh_dct[val_txt_lower] = [gh_cnt, val_txt, tab_dct, t_rc_tup]                
                elif stype in ('hch', 'vch'):
                    #hv_gh_dct[val_txt_lower] = [hv_gh_dct.get(val_txt_lower, [0, ''])[0] + 1, val_txt, {}]
                    vh_cnt, vhe_txt, vh_tab_dct, vh_trc_tup = hv_gh_dct.get(val_txt_lower, [0, '', {}, trc]) 
                    vh_cnt = vh_cnt+ 1
                    vh_tab_dct[table_id] = 1
                    hv_gh_dct[val_txt_lower] = [vh_cnt, val_txt, vh_tab_dct, vh_trc_tup]               
            

    def stats_gh_hgh_vgh(self, ijson):
        company_id = ijson['company_id']
        table_ids  = ijson['table_ids'] 
        gh_dct, hv_gh_dct = {}, {}
        print table_ids
        
        import dt_sig 
        obj = dt_sig.DTSIG()

        for table_id in table_ids:
            self.redis_gh_vh(company_id, table_id, gh_dct, hv_gh_dct, obj)
            #self.table_gh_hvgh_cnt(company_id, table_id, gh_dct, hv_gh_dct)
        #obj.dt_sig(txt)

        gh_lst = []
        for gh_txt, gh_cnt_lst in gh_dct.iteritems():
            gh_cnt, or_txt, gh_tab_dct, gh_trc = gh_cnt_lst 
            if not or_txt:continue
            #or_txt = obj.dt_sig(or_txt)
            g_tab, g_rc = gh_trc
            grc_tup = tuple(map(int, g_rc.split('_')))
            gindex_tab = table_ids.index(g_tab)
            gtrc = (gindex_tab, ) + grc_tup
            gh_row_dct = {'gh':or_txt, 'gh_cnt':len(gh_tab_dct), 'gh_tids':gh_tab_dct.keys(), 'trc':gtrc}
            gh_lst.append(gh_row_dct)
        
        hv_gh_lst = []
        for vh_txt, vh_cnt_lst in hv_gh_dct.iteritems():
            vh_cnt, vor_txt, vch_tids, vh_trc = vh_cnt_lst
            if not vor_txt:continue 
            #vor_txt = obj.dt_sig(vor_txt)
            vh_t, vh_rc = vh_trc
            vh_tup = tuple(map(int, vh_rc.split('_')))
            vh_index_tab = table_ids.index(vh_t)
            vhtrc = (vh_index_tab, ) + vh_tup
            vh_row_dct = {'vh':vor_txt, 'vh_cnt':len(vch_tids), 'vch_tids':vch_tids.keys(), 'trc':vhtrc}
            hv_gh_lst.append(vh_row_dct)
        
        gh_lst.sort(key=lambda x:x['trc'])
        hv_gh_lst.sort(key=lambda x:x['trc'])
        
        from itertools import izip_longest as zpl
        zip_lst = zpl(gh_lst, hv_gh_lst)
        
        res_lst = []
        for r_tup in zip_lst:
            gh_i, vh_i = r_tup
            row_dct = {}
            if gh_i:
                row_dct.update(gh_i)
            if vh_i:
                row_dct.update(vh_i)
            if row_dct:
                res_lst.append(row_dct)
        res = [{'message':'done', 'data':res_lst}] 
        return res
        
    def non_scoped_groups(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        doc_filter = ijson.get('doc_filter', 'N') 
        tscope_d    = self.get_classified_tables(company_id, ijson['project_id'], ijson)
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT distinct(table_id) FROM group_tables;  """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        scoped_tabs = {e[0] for e in t_data}

        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        r_qry = """ SELECT group_id, table_id FROM group_tables; """
        if doc_filter == 'Y':
            r_qry = """ SELECT group_id, table_id FROM group_tables WHERE doc_id='%s'; """%(ijson['filter_doc_id'])
        cur.execute(r_qry)
        t_data = cur.fetchall()
        conn.close()

        group_wise_tables = {} 
        for rd in t_data:
            group_id, table_id = rd
            group_wise_tables.setdefault(group_id, set()).add(table_id)
        
        data_lst = []
        db_path = config.Config.gl_db 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        def get_class_name(c_id):
            sql = "select table_type from all_table_types where row_id=%s"%(c_id)
            cur.execute(sql)
            res = cur.fetchone()
            if res and res[0]:
                return res[0]
            return ''
        for grp, tab_set in group_wise_tables.iteritems():
            e_set = tab_set - scoped_tabs
            if e_set:
                tab_set = e_set
                res_table = next(iter(tab_set))
                pg = '_'.join(res_table.split('_')[:])
                row_dct = {'k':res_table, 'n':pg, 'g':grp, 'g_t':list(tab_set)}
                print row_dct, '\n'
                f   = 1
                td  = {}
                table_lst   = list(group_wise_tables[grp])
                for tid in table_lst:
                    if tid not in tscope_d:
                        f = 0
                    else:
                        td[tscope_d[tid]['c_id']]  = 1
                if td:
                    if f == 1:
                        row_dct['flg'] = 'G'
                    else:
                        row_dct['flg'] = 'O'
                    if len(td.keys()) > 1:
                        row_dct['flg'] = 'P'
                    row_dct['title']    = json.dumps(map(lambda x:get_class_name(x), td.keys()))
                data_lst.append(row_dct)
        conn.close()
        data_lst.sort(key=lambda x:tuple(map(lambda x1:int(x1), x['k'].split('_'))))
        res = [{'message':'done', 'data':data_lst}]
        #sys.exit()
        return res
    
    def read_class_doc_type_wise(self, ijson):
        company_id = ijson['company_id']
        doc_types   = ', '.join(['"'+e+'"' for e in ijson['doc_type']])
        project_id = ijson['project_id']
        db_path = config.Config.docinfo_path.format(company_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, doc_type FROM document_meta_info WHERE doc_type in (%s)"""%(doc_types)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        #doc_str = ', '.join([str(e[0]) for e in t_data])
        doc_dct = {}
        for dc, dtype in t_data:
            doc_dct[str(dc)] = dtype 
        
        doc_str = ', '.join(doc_dct.keys())
        
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT classified_id, doc_id, table_type FROM scoped_gv WHERE doc_id in (%s); """%(doc_str)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()        
    
        cls_id_dct = {}
        cls_tt_info = {}
        for cls_id, docid, tt in t_data:
            if tt == '':
                tt = 'Type I'
            cls_id_dct.setdefault(str(cls_id), {})[str(docid)] = 1
            cls_tt_info.setdefault(str(cls_id), {})[str(tt)] = 1

        cls_str = ', '.join(cls_id_dct)

        db_path = '/mnt/eMB_db/company_management/global_info.db' 
        conn, cur   = conn_obj.sqlite_connection(db_path)
        r_qry = """ SELECT row_id, table_type, short_form FROM all_table_types WHERE row_id in (%s); """%(cls_str)
        cur.execute(r_qry)
        mst_data = cur.fetchall()
        conn.close()
        clasified_id_lst = []
        for row_data in mst_data:
            row_id, table_type, short_form = row_data
            cls_docs = cls_id_dct.get(str(row_id), {})
            tt_dct = cls_tt_info.get(str(row_id), {})
            dt_dct = {}
            for dc in cls_docs:
                dtyp = doc_dct[dc]
                dt_dct[dtyp] = 1
            clasified_id_lst.append({'k':row_id, 'n':table_type, 'dt':dt_dct, 'tt':tt_dct})
        for r in self.add_hard_coded_table_types():
            clasified_id_lst.append(r)
        res = [{'message':'done', 'data':clasified_id_lst}]
        return res
        
    def add_hard_coded_table_types(self):
        r1 ={'k':'Company_Basic_Information', 'n':'Company_Basic_Information', 'tt':{}, 'dt':{}}
        r2 ={'k':'Company_Client_Information', 'n':'Company_Client_Information', 'tt':{}, 'dt':{}}
        return r1, r2
        
    def check_func(self):
        r1 ={'k':'Company Name', 'n':'Company Name', 'ttypes':{'Company_Basic_Information':1}, 'tt':{}, 'dt':{}}
        r2 ={'k':'Industry', 'n':'Industry', 'ttypes':{'Company_Basic_Information':1}, 'tt':{}, 'dt':{}}
        r3 ={'k':'Client Id', 'n':'Client Id', 'ttypes':{'Company_Client_Information':1}, 'tt':{}, 'dt':{}}
        r4 ={'k':'Client Name', 'n':'Client Name', 'ttypes':{'Company_Basic_Information':1}, 'tt':{}, 'dt':{}} 
        return 
        
    def read_doc_type_info(self, company_id):
        db_path = config.Config.docinfo_path.format(company_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, doc_type FROM document_meta_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        doc_dct = {}
        for dc, dtype in t_data:
            doc_dct[str(dc)] = dtype 
        return doc_dct
        
    def read_ref_table(self, cur):
        read_qry = """ SELECT rawdb_row_id, doc_id FROM reference_table; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        row_id_doc_map = {}
        for rd in t_data:
            rawdb_row_id, doc_id = rd
            row_id_doc_map[str(rawdb_row_id)] = str(doc_id)
        return row_id_doc_map
            
    def read_ttypes(self, company_id, project_id, doc_str):
        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT classified_id, table_type FROM scoped_gv WHERE doc_id in (%s); """%(doc_str)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()        
    
        cls_tt_info = {}
        for cls_id, tt in t_data:
            if tt == '':
                tt = 'Type I'
            cls_tt_info.setdefault(str(cls_id), {})[str(tt)] = 1
        return cls_tt_info

    def read_tt_wise_taxo_group(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        table_types = ijson['table_types']
        doc_map_dt =  self.read_doc_type_info(company_id)
        tt_str  =  ', '.join(['"'+str(r)+'"' for r in table_types])
        db_path = config.Config.databuilder_path.format(company_id, project_id)
        print db_path
        conn, cur   = conn_obj.sqlite_connection(db_path)

        read_qry = """  SELECT DISTINCT table_type, taxo_group_id, table_id FROM data_builder; """
        if tt_str:
            read_qry = """  SELECT DISTINCT table_type, taxo_group_id, table_id FROM data_builder WHERE table_type IN (%s);  """%(tt_str)
        cur.execute(read_qry)
        t_data = cur.fetchall() 
        #row_id_doc_map = self.read_ref_table(cur)        
        conn.close()
        tab_grp_dct = {}
        taxo_wise_doc_dct = {}
        dist_docs = {}
        for row_data in t_data:
            table_type, taxo_group_id, table_id = row_data
            doc, page, grid = table_id.split('_')
            tab_grp_dct.setdefault(taxo_group_id, {})[table_type] = 1
            tab_grp_dct.setdefault('TAS DB ID', {})[table_type] = 1
            #doc = row_id_doc_map.get(str(row_id), '')
            #if not doc:continue
            taxo_wise_doc_dct.setdefault(taxo_group_id, {})[doc] = 1
            dist_docs[doc] = 1
        
        doc_str = ', '.join(dist_docs)

        tt_info_dct = self.read_ttypes(company_id, project_id, doc_str)
        
        currency_country_info = self.add_currency_contry_info_add_168()    

        res_lst = []
        for tx, tt_dct in tab_grp_dct.iteritems():
            row_dct = {'k':tx, 'n':tx, 'ttypes':tt_dct}
            info_dct = currency_country_info.get(tx, {})
            row_dct['ttypes'].update(info_dct)
            doc_type_dct = taxo_wise_doc_dct.get(tx, {})
            dt = {}
            for doc in doc_type_dct:
                dtype = doc_map_dt[doc]
                dt[dtype] = 1
            row_dct['dt'] = dt
            tt_i = {}
            for cls in tt_dct:
                tt_res = tt_info_dct.get(cls, {})
                tt_i.update(tt_res) 
            row_dct['tt'] = tt_i
            res_lst.append(row_dct)
        for r in self.hard_coded_taxo_tt_info():
            res_lst.append(r)
        res = [{'message':'done', 'data':res_lst}]
        return res            

    def hard_coded_taxo_tt_info(self):
        r1 ={'k':'Company Name', 'n':'Company Name', 'ttypes':{'Company_Basic_Information':1}, 'tt':{}, 'dt':{}}
        r2 ={'k':'Industry', 'n':'Industry', 'ttypes':{'Company_Basic_Information':1}, 'tt':{}, 'dt':{}}
        r3 ={'k':'Client Id', 'n':'Client Id', 'ttypes':{'Company_Client_Information':1}, 'tt':{}, 'dt':{}}
        r4 ={'k':'Client Name', 'n':'Client Name', 'ttypes':{'Company_Basic_Information':1}, 'tt':{}, 'dt':{}}
        #r5 ={'k':'Country', 'n':'Country', 'ttypes':{'Company_Basic_Information':1}, 'tt':{}, 'dt':{}}
        #r6 ={'k':'Currency', 'n':'Currency', 'ttypes':{'Company_Basic_Information':1}, 'tt':{}, 'dt':{}}
        return r1, r2, r3, r4
        
    def add_currency_contry_info_add_168(self):
        info_dct = {
                    'Country' : {'Company_Basic_Information':1},
                    'Currency': {'Company_Basic_Information':1}
                    }
        return info_dct
        
    def get_bounding_box(self, v):
        x1  = v['x'].keys()
        x1.sort()
        x   = x1[0]
        y1  = v['y'].keys()
        y1.sort()
        y   = y1[0]
        w1  = v['x1'].keys()
        w1.sort()
        w   = w1[-1] - x
        h1  = v['y1'].keys()
        h1.sort()
        h   = h1[-1] - y
        return x, y, w, h 
        
    def get_bbox(self, bbox, tmpx=None, tmpy=None, tmpw=None, tmph=None):
        bbox_d  =  {'x':{}, 'y':{}, 'x1':{}, 'y1':{}}
        for boxs in bbox:
            if (not isinstance(boxs, list)) and (not isinstance(boxs, tuple)):
                boxs    = [boxs]
            if boxs and ((not isinstance(boxs[0], list)) and (not isinstance(boxs[0], tuple))):
                boxs    = [boxs]

            for box in boxs:
                x, y, x1, y1    = box
                bbox_d['x'][x]   = 1
                bbox_d['y'][y]   = 1
                bbox_d['x1'][x1]   = 1
                bbox_d['y1'][y1]   = 1
        v   = bbox_d
        x, y, w,h   = self.get_bounding_box(v)
        if tmpx != None and x > tmpx:
            x   = tmpx
        if tmpw != None:
            w   = max(tmpw, w)
        return [x, y, w,h] 

    def prep_column_info(self, company_id, table_id, hgh_vgh_dct):
        #doc, page, grid = table_id.split('_')
        #json_data = self.read_json_file(table_id, company_id)
        #grid_data = json_data.get('data', {})
        doc, page, grid = map(int, table_id.split('_'))
        rObj = pre.exe("172.16.20.7##6384##0", int(company_id), int(doc)) 
        for stype in ['vch', 'hch']:
            info_lst = rObj.read_sction_data(page, grid, stype)
            for cell_dict in info_lst:
                val_txt = cell_dict['txt']
                print ['VVVVVVVV', val_txt, table_id], '\n'
                rc  = cell_dict['rc']
                row, col = rc.split('_') 
                val_txt_lst = val_txt.split()
                val_txt_l = ' '.join([e.strip() for e in val_txt_lst])
                val_txt_lower = val_txt_l.lower()
                #hgh_vgh_dct.setdefault(val_txt_lower, {})[table_id] = (rc, col, val_txt)
                trc = (table_id, rc)
                e_txt, tab_dct, t_rc_tup = hgh_vgh_dct.get(val_txt_lower, ['', {}, trc])
                if not e_txt:
                    e_txt = val_txt[:]
                tab_dct[table_id] = 1                
                hgh_vgh_dct[val_txt_lower] = [e_txt, tab_dct, t_rc_tup]                
        return
        
    def clean_xml(self, xml):
        xml_sp = filter(lambda x: x, xml.split('#'))
        nxml    = []
        for x in xml_sp:
            if 'z' in x:
                x   = x.split('z')[1]
            nxml.append(x)
        return '#'.join(nxml)        

    def read_des_table_col_xml_bbox(self, company_id, table_id, table_col, doc, page, grid):
        json_data = self.read_json_file(table_id, company_id)
        grid_data = json_data.get('data', {})
        collect_col_xml = []
        collect_bbox = []
        rc_lst = []
        for rc, cell_dict in grid_data.iteritems():
            if cell_dict['ldr'] == 'gh':continue
            row, col = rc.split('_')
            if col != table_col:continue
            cell_xml_lst = cell_dict['xml_ids'].split('$$')
            collect_col_xml += cell_xml_lst
            bbox_lst = cell_dict['bbox'].split('$$')
            print 'BBBBBBBBBBBBBB', bbox_lst
            bbx_lst = [] #[map(int, b.split('_')) for b in bbox_lst]
            for b in bbox_lst:
                if not b:continue
                bbx_lst.append(map(int, b.split('_')))
            if bbx_lst:
                collect_bbox.append(bbx_lst)
            rc_lst.append(rc)
        bounded_bbox =[]
        if collect_bbox: 
            bounded_bbox  = self.get_bbox(collect_bbox)
        table_col_xml = '#'.join(collect_col_xml)
        table_col_xml = self.clean_xml(table_col_xml)
        return table_col_xml, bounded_bbox, rc_lst 
        
    def table_column_data(self, ijson):
        company_id = ijson['company_id']
        table_ids  = ijson['table_ids']
        txt_taxo_tagged_dct = {} 
        if ijson.get('table_type', 0):
            txt_taxo_tagged_dct = self.read_taxo_tagged_info(ijson)    
        hgh_vgh_dct = {}
        table_idx = {}
        for table_id in table_ids:
            self.prep_column_info(company_id, table_id, hgh_vgh_dct) 
        #print '####################################################################\n\n'
        #print hgh_vgh_dct
        res_lst = []
        for idx, (hv_txt, data_lst) in enumerate(hgh_vgh_dct.items()):
            val_txt, tableid_dct, trc = data_lst
            random_table, rc = trc
            row, col = rc.split('_')
            tab_idx = table_ids.index(random_table)
            srt_idx = (tab_idx, int(col)) 
            #random_table = next(iter(tableid_dct))  
            #rc, col, val_txt = tableid_dct[random_table]
            doc, page, grid = random_table.split('_')
            table_col_xml, bounded_bbox, rc_lst = self.read_des_table_col_xml_bbox(company_id, random_table, col, doc, page, grid)
            row_dct = {'k':idx, 'n':val_txt, 'x':table_col_xml, 'bbox':[bounded_bbox], 'd':doc, 'p':page, 'g':grid, 't_ln':len(tableid_dct), 'idx':srt_idx, 'rc':rc_lst}
            taxo_tagged = txt_taxo_tagged_dct.get(val_txt, {})
            row_dct.update(taxo_tagged)
            res_lst.append(row_dct)
        res_lst.sort(key=lambda x:x['idx'])
        res = [{'message':'done', 'data':res_lst}] 
        return res 
        
    def read_taxo_tagged_info(self, ijson):
        company_id  = ijson["company_id"]
        project_id  = ijson["project_id"] 
        table_type  = ijson["table_type"]

        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        crt_stmt = """ CREATE TABLE IF NOT EXISTS taxo_tagged(row_id INTEGER PRIMARY KEY AUTOINCREMENT, table_type INTEGER, column_header TEXT, tagged_key TEXT, key_type TEXT); """
        cur.execute(crt_stmt)

        read_qry = """ SELECT column_header, tagged_key, key_type FROM taxo_tagged WHERE table_type='{0}'; """.format(table_type)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
            
        txt_taxo_tagged_dct = {}
        for row_tup in  t_data:
            column_header, tagged_key, key_type = row_tup
            txt_taxo_tagged_dct.setdefault(column_header, {})[key_type] = tagged_key
        return txt_taxo_tagged_dct

    def delete_taxo_tagged_tt(self, ijson):
        company_id  = ijson["company_id"]
        project_id  = ijson["project_id"] 
        table_type  = ijson["table_type"]

        cid_path = config.Config.company_id_path.format(company_id, project_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)

        del_stmt = """ DELETE FROM taxo_tagged WHERE table_type='{0}'; """.format(table_type)
        cur.execute(del_stmt)
        conn.commit()
        conn.close()
        
        res = [{'message':'done'}]
        return res
        
    def read_validation_error_tables(self, ijson):
        company_id = ijson['company_id']
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT doc_id, table_id FROM error_tables ORDER BY doc_id;  """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        res_lst = []
        for doc, tab in t_data:
            row_dct = {'d':doc, 'k':tab, 'n':tab}
            res_lst.append(row_dct)
        res = [{'message':'done', 'data':res_lst}]
        return res

    def read_validation_error_rcs(self, ijson):
        company_id = ijson['company_id']
        table_id   = ijson['table_id']
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'table_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT error_info FROM error_tables WHERE table_id='%s';  """%(table_id)
        cur.execute(read_qry)
        t_data = eval(cur.fetchone()[0])
        conn.close()
        res_lst = []
        for error_msg, rc_dct in   t_data.iteritems():
            if row_dct.keys()[0] == 'ALL RCs':continue
            row_dct = {'k':error_msg, 'n':rc_dct, 'g':error_msg}
            res_lst.append(row_dct)

        res = [{'message':'done', 'data':res_lst}]
        return res
        
    def tab_module_format(self, ijson):
        company_id  = ijson["company_id"]
        project_id  = ijson["project_id"] 
        db_path = config.Config.super_key_db_path.format(company_id, project_id)
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT table_type, filename FROM super_key_builder_map; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        res_lst = []
        for row_data in t_data:
            table_type, file_name = row_data
            row_dct = {'k':table_type, 'n':file_name}
            res_lst.append(row_dct)
        res = [{'message':'done', 'data':res_lst}]
        return res
        
        



if __name__ == '__main__':
    ic_Obj = INC_Company_Mgmt()
    ##ijson = {"company_id":"1015", "db_name":"AECN_INC", "doc_ids":[13499, 2313, 2285, 2298]}    
    #ijson = {"company_id":"1053729", "table_ids":["4_77_2", "4_75_1", "4_69_1"], "doc_ids":[1, 2, 3, 4], 'project_id':5, 'table_type':81, "doc_type":['CSV', 'HTML'], "project_id":5, "table_types":[85]} 
    #ijson = {"company_id":1053729, "table_ids":["61_96_1"]}
    ijson = {"company_id":1053729, "table_ids":["3_35_1","3_36_1","3_37_1","3_38_1"]}
    print ic_Obj.table_column_data(ijson)
    #print ic_Obj.read_page_coods_34(1053729, 3)
    #print ic_Obj.read_class_doc_type_wise(ijson)
    #print ic_Obj.read_tt_wise_taxo_group(ijson)
    #print ic_Obj.non_scoped_groups(ijson)
    #print ic_Obj.stats_gh_hgh_vgh(ijson)
    #ijson = {"company_id":"1053724", "doc_id":1, 'project_id':5, 'table_type':81}    
    #ijson = {"company_id":1053724, "doc_id":6, "table_ids":['6_8_3', '6_9_1', '6_101_1', '6_102_1', '6_103_1', '6_73_1', '6_55_1']}
    #print ic_Obj.read_group_tables(ijson) 
    #print ic_Obj.create_groups_tables(ijson)
    #print ic_Obj.read_table_shelve_file('/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/1053724/6/table_groups.sh')
    #ijson = {"cmd_id":113,"company_id":1604,"doc_id":4,"project_id":5,"hct":"Y","table_type":81,"user":"demo", "PRINT":"Y"}
    #print ic_Obj.update_document_meta_info(ijson)
    #print ic_Obj.read_doc_meta_data(ijson)
    #print ic_Obj.read_global_table_type_info(ijson)
    #print ic_Obj.get_doc_table_stats(ijson)
    #print ic_Obj.table_stats(ijson)
    #print ic_Obj.read_data_builder_info(ijson)
    #print ic_Obj.data_builder_stats(ijson)
    #print ic_Obj.get_count_tables(ijson)
    #print ic_Obj.read_rawdb_tree(ijson)
    ##print ic_Obj.scoped_predictor_info(ijson)
    ##print ic_Obj.read_project_info(ijson)
    ##print ic_Obj.read_tt_wise_docs(ijson)
    ##ic_Obj.read_DB_prep_data(ijson, {5131:1, 4897:1, 4879:1, 5130:1})
    ##ic_Obj.across_doc_DB(ijson)
    #print ic_Obj.read_all_tt(ijson)
    ##print ic_Obj.read_grid_information(ijson)
    ##print ic_Obj.read_document_meta_data(ijson)
    ##print ic_Obj.read_distinct_table_types(ijson)
    ##print ic_Obj.populate_table_information(ijson)
    ##print ic_Obj.read_company_list() 
