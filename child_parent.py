import os, sys, sqlite3, lmdb, ast, json
import config
import report_year_sort as rys
from collections import OrderedDict as OD

class GridModel(object):

    def connect_to_sqlite(self, db_path):
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        return conn, cur

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
    
    def search_operation(self, ijson):  
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        nl_flg     = ijson['nl_flg']
        data_lst = ijson['data']
        db_path = config.Config.doc_wise_db.format(company_id, doc_id) 
        conn, cur = self.connect_to_sqlite(db_path)
        formula_dct = self.operation_read_formula_info_flag(conn, cur)
        
        res_r, res_opr, res_po, res_nc = {}, {}, {}, {}
        for row_dct in data_lst:
            s_flg   =  row_dct['s_flg']
            val     =  row_dct['val']
            if s_flg == 'Resultant':
                res_r = self.resultant_search(conn, cur, val, nl_flg, formula_dct)
                print res_r
            if s_flg == 'operand':
                res_opr = self.operand_operation(conn, cur, val, nl_flg)
            if s_flg == 'Positive Contribute':
                res_po  = self.positive_contribute_operation_negative(conn, cur, val, nl_flg, 'P')
            if s_flg == 'Negative Contribute':
                res_nc  = self.positive_contribute_operation_negative(conn, cur, val, nl_flg, 'N')
        #return [{'message':'done', 'resultant':res_r, 'operand':res_opr, 'pc':res_po, 'nc':res_nc}] 

    def get_groups(self, ijson):
        company_id      = ijson['company_id']
        doc_id          = ijson['doc_id']
        db_path = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/newvalidation_test_db/{0}/{1}.db'.format(company_id, doc_id)
        if not os.path.exists(db_path): 
            return [{'message':'done', 'drop_down_data':[]}]
        conn, cur = self.connect_to_sqlite(db_path)
        read_qry = """  SELECT distinct(groupid) FROM rawdb """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()    
        drop_down_data_lst = []
        for row in t_data:
            group_id = row[0]
            dt_dct = {'k':group_id, 'n':group_id}
            drop_down_data_lst.append(dt_dct)
        return [{'message':'done', 'drop_down_data':drop_down_data_lst}]
    
    def cal_bobox_data_new(self, project_id, deal_id, company_id, did):
        lmdb_folder      = os.path.join('/var/www/html/fundamentals_intf/output/', company_id, 'doc_page_adj_cords')
        page_dict = {}
        if os.path.exists(lmdb_folder):
            env = lmdb.open(lmdb_folder, readonly=True)
            txn = env.begin()
            if 1:
                cursor = txn.cursor()
                for doc_id, res_str in cursor:
                    if str(doc_id) != did:continue
                    if res_str:
                        page_dict = ast.literal_eval(res_str)
        return page_dict

    def read_rawdb_tree(self, ijson):
        company_id      = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        doc_id          = ijson['doc_id']
        db_path = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/newvalidation_test_db/{0}/{1}.db'.format(company_id, doc_id)
        if not os.path.exists(db_path): 
            return [{'message':'done', 'grid_data':{'data':[], 'col_def':[], 'map':{}}}]
        conn, cur = self.connect_to_sqlite(db_path)
        try:
            formula_str = self.read_formula_info_flag(conn, cur)
        except:formula_str = set()
        read_qry = """  SELECT row_col_groupid, row, col, groupid, cellph, cellcsvc, cellcsvs, cellcsvv, gvtext, hghtext FROM rawdb """
        try:
            cur.execute(read_qry)
            t_data = cur.fetchall()
        except:t_data = ()
            
        conn.close()    

        group_rc_dct      = OD()
        group_col_dct     = {} 
        group_r_hgh_dct  =  {}
        formula_flg_dct  = {}
        for row_data in t_data:
            row_col_group_id, row, col, groupid, cellph, cellcsvc, cellcsvs, cellcsvv, gvtext, hghtext = row_data
            cellph    = eval(cellph)[0]
            cellcsvc  = eval(cellcsvc)[0]
            cellcsvs  = eval(cellcsvs)[0]
            gvtext    = ' '.join(eval(gvtext))
            hghtext_str   = ' '.join(eval(hghtext))  
            period_type, period = cellph[:-4], cellph[-4:]
            f_flg = 'N'
            if row_col_group_id in formula_str:   
                f_flg = 'Y'
            group_rc_dct.setdefault(groupid, OD()).setdefault(row, OD())[col] = {'v':gvtext, 'title':{'pt':period_type, 'p':period, 'c':cellcsvc, 's':cellcsvs, 'vt':cellcsvv}, 'k':row_col_group_id, 'w':len(gvtext)}
            group_r_hgh_dct.setdefault(groupid, {}).setdefault(row, {}).setdefault('txt_lst', []).append((hghtext_str, row_col_group_id))
            group_col_dct.setdefault(groupid, OD()).setdefault(col, []).append(cellph)
            formula_flg_dct.setdefault(groupid, {}).setdefault(row, set()).add(f_flg)
        
        data_lst = []
        inf_map  = {}
        sn = 1
        cl_nw_key_dt =  {}
        for grp_sn, (grp_id, rc_dct) in enumerate(group_rc_dct.iteritems(), 1):
            ck_dct = group_col_dct[grp_id]
            rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, 'desc':{'v':'Group-%s (%s)'%(grp_sn, grp_id), 'cls':'grid-header'}, '$$treeLevel':0}
            
            col_map = {}
            col_key_idx = 1
            for clo, ph_lst in ck_dct.iteritems():
                c_key = 'COL-%s'%(col_key_idx)
                col_map[clo] = c_key
                cl_nw_key_dt[c_key] = 0
                col_key_idx += 1
                sorted_ph = rys.year_sort(set(ph_lst)) 
                sorted_ph.reverse()
                cph_str = '~'.join(sorted_ph)
                rw_dct[c_key] = {'v':cph_str, 'cls':'grid-header'}
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
                    rcw_dct[cl_ky] = dt_inf
                    c_width = dt_inf['w']
                    cl_nw_key_dt[cl_ky] = max(cl_nw_key_dt[cl_ky], c_width)

                hgh_t_lst = group_r_hgh_dct[grp_id][rw]['txt_lst']
                desc_tup    = hgh_t_lst[0]
                desc, rcg = desc_tup
                rcw_dct['desc'] = {'v':desc, 'k':rcg}
                data_lst.append(rcw_dct)
                sn += 1
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
        return [{'message':'done', 'data':data_lst, 'col_def':col_def, 'map':inf_map}]
        
    def cell_id_reference(self, ijson):
        company_id      = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        doc_id          = ijson['doc_id']
        row_col_groupid = ijson['k']
        col_type   = ijson['col_type']
        db_path = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/newvalidation_test_db/{0}/{1}.db'.format(company_id, doc_id)
        print db_path
        if not os.path.exists(db_path): 
            return [{'message':'done', 'grid_data':{'data':[], 'col_def':[], 'map':{}}}]
        conn, cur = self.connect_to_sqlite(db_path)
        read_qry = """  SELECT row, col, gvxmlid, gvbbox, pg, hghxmlid, hghbbox FROM rawdb WHERE row_col_groupid='{0}' """.format(row_col_groupid)
        print read_qry
        cur.execute(read_qry)
        t_data = cur.fetchone()
        r_qry = """ SELECT pageno, pagesize FROM pagedet;  """ 
        cur.execute(r_qry)
        page_data = cur.execute(r_qry)
        page_c_dct = {}
        for rsr in page_data:
            pn, p_co = rsr
            p_co_lst = p_co.split('_')
            page_c_dct[str(pn)] = p_co_lst
        conn.close()    
        data_dct = {}
        if t_data is not None:
            row, col, gvxmlid, gvbbox, pg, hghxmlid, hghbbox = t_data
            gvxmlid   = '#'.join(eval(gvxmlid))
            hghxmlid  = '#'.join(eval(hghxmlid))
            hghbbox   = eval(hghbbox)
            gvbbox    = eval(gvbbox)
            #page_coord_dict = self.cal_bobox_data_new(str(project_id), str(deal_id), str(company_id), str(doc_id))
            #page_coord = page_coord_dict.get(str(pg), [])
            page_coord = map(float, page_c_dct.get(str(pg), []))
            hgh_bbox = reduce(lambda x, y:x+y, hghbbox)
            if col_type == 'HGH':
                data_dct = [{'x':hghxmlid, 'pno':pg, 'd':doc_id, 'coord':page_coord, 'bbox':hgh_bbox}]
            elif col_type == 'GV':
                data_dct = [{'x':gvxmlid, 'pno':pg, 'd':doc_id, 'coord':page_coord, 'bbox':gvbbox}]
        path1   = '/pdf_canvas/viewer.html?file=/var_html_path/TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/pdfpagewise/{0}/{1}.pdf'%(project_id, deal_id)
        html_path   =  '/var_html_path//TASFundamentalsV2/tasfms/data/output/%s_common/data/%s/output/{0}/html/{1}_celldict.html'%(project_id, deal_id)
        ref_path    = {
                        'ref_html':html_path,
                        'ref_pdf':path1,
                        }
        return [{'message':'done', 'ref':data_dct, 'path':ref_path}]
        
    def read_formula_info_flag(self, conn, cur):
        read_qry = """ SELECT resultant_info, groupid FROM formulainfo """
        cur.execute(read_qry)   
        t_data = cur.fetchall()
        r_set = set()
        for row in t_data:
            res_info, groupid = row[0], row[1]
            data_tup = eval(res_info)
            op, (r, c), grpid, ed = data_tup
            d_str = '{0}_{1}_{2}'.format(r, c, groupid)
            r_set.add(d_str)
        return r_set

    def read_formula_cell_id(self, ijson):
        company_id      = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        doc_id          = ijson['doc_id']
        row_col_groupid = ijson['k']
        row , col, groupid = row_col_groupid.split('_')
        db_path = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/newvalidation_test_db/{0}/{1}.db'.format(company_id, doc_id)
        if not os.path.exists(db_path): 
            return [{'message':'done', 'data':[]}]
        conn, cur = self.connect_to_sqlite(db_path)
        rd_qry = """ SELECT resultant_info, formula FROM formulainfo WHERE resultant_info LIKE '%{0}%' AND groupid='{1}' """.format((int(row), int(col)), groupid)
        #print rd_qry
        cur.execute(rd_qry)
        t_data = cur.fetchone()
        grp_rc_dct = OD()
        if t_data:
            resultant_info_tup, formula_lst = t_data
            resultant_info_tup = eval(resultant_info_tup)
            #print resultant_info_tup
            formula_lst = eval(formula_lst)
            rop, (r_r, r_c), gp, rm = resultant_info_tup
            r_str =  '%s_%s_%s'%(r_r, r_c, groupid)
            grp_rc_dct[r_str] = {'op':rop, 'r_c':'%s_%s'%(r_r, r_c), 'group_id':groupid}
            for formula_tup in formula_lst:
                oop, (fr_r, fr_c), fgp, frm = formula_tup
                f_str =  '%s_%s_%s'%(fr_r, fr_c, groupid)
                grp_rc_dct[f_str] = {'op':oop, 'r_c':'%s_%s'%(fr_r, fr_c), 'group_id':groupid}  
        
        cond_str = ', '.join(['"'+str(e)+'"' for e in grp_rc_dct])
        #print cond_str
        r_qry = """  SELECT row, col, groupid, hghtext FROM rawdb WHERE row_col_groupid in (%s) """%(cond_str)
        #print r_qry
        cur.execute(r_qry)
        mt_data = cur.fetchall()
        conn.close()
        data_dct = {}
        for rw in mt_data:
            rsw, csl, gp_id, hghtext = rw
            ky = '%s_%s_%s'%(rsw, csl, gp_id)    
            tg_dct = grp_rc_dct[ky] 
            tg_dct['description'] = hghtext
            grp_rc_dct[ky] = tg_dct
        res_lst = []
        for rcg, d_dct in grp_rc_dct.iteritems():
            res_lst.append(d_dct)
        return [{'message':'done', 'data':res_lst}] 
        
        
    def resultant_search(self, conn, cur, numval, nl_flg, formula_dct):
        read_qry = """  SELECT row, col, groupid FROM rawdb WHERE numval=%s """%(numval)
        if nl_flg == 'label':
            read_qry = """  SELECT row, col, groupid FROM rawdb WHERE hghtext LIKE '%{0}%' """.format(numval)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        res_dct = {}
        for row in t_data[:]:
            row = map(str, row)
            rcg_str = '_'.join(row)
            if rcg_str not in formula_dct:continue
            rw, cl, grpid = row
            f_dct = formula_dct.get('_'.join(map(str, (rw, cl, grpid))), {})
            res_dct[(str(rw), str(grpid))] = f_dct
        group_rc_dct       = OD()
        group_col_dct      = {}
        hgh_text_info_dct  = {}
        for row_tup, fr_dct in res_dct.iteritems():
            r_qry = """ SELECT row, col, groupid, row_col_groupid, gvtext, hghtext  FROM rawdb WHERE row='%s' and groupid='%s' """%row_tup
            cur.execute(r_qry)
            mt_data = cur.fetchall()
            for rsw in mt_data:
                r, col, groupid, row_col_groupid, rgvtext, rhghtext = rsw
                group_rc_dct.setdefault(groupid, OD()).setdefault(r, {})[col]  = {'v':rgvtext}
                group_col_dct.setdefault(groupid, {})[col] = 1
                hgh_text_info_dct.setdefault(groupid, {}).setdefault(r, []).append(rhghtext)
                fr_qry = """ SELECT formula, groupid FROM formulainfo WHERE resultant_info LIKE '%{0}%' AND groupid='{1}' """.format((int(r), int(col)), groupid) 
                cur.execute(fr_qry)
                ft_data = cur.fetchone()
                if ft_data:
                    formula_lst, f_group = eval(ft_data[0]), ft_data[1]
                    for rt_tup in formula_lst:
                        try:
                            feq, (fr, fc), fg, fe = rt_tup 
                        except:continue
                        rcg_str_fr = '{0}_{1}_{2}'.format(fr, fc, f_group)
                        dms_qry = """ SELECT row, col, groupid, row_col_groupid, gvtext, hghtext FROM rawdb WHERE row_col_groupid='%s' """%(rcg_str_fr)
                        cur.execute(dms_qry)
                        fr_data = cur.fetchone()
                        ffr, ffc, ffg, ffrcg, fgv_text, fhghtext = fr_data
                        group_rc_dct.setdefault(ffg, OD()).setdefault(ffr, {})[ffc]  = {'v':eval(fgv_text)}
                        hgh_text_info_dct.setdefault(ffg, {}).setdefault(ffr, []).append(fhghtext)
        
        data_lst = []
        c_mk_dct = {}
        inf_map  = {}
        sn  = 1
        for grp_sn, (gp_id, rw_dct) in enumerate(group_rc_dct.iteritems()):
            gr_rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, 'desc':{'v':'Group-%s (%s)'%(grp_sn, gp_id)}, '$$treeLevel':0}
            data_lst.append(gr_rw_dct)
            sn += 1
            col_info = group_col_dct[gp_id]
            col_map = {}
            col_idx = 1
            for cml in col_info:
                c_n_key  = 'COL-%s'%(col_idx)
                col_idx += 1
                col_map[cml] = c_n_key
                c_mk_dct[c_n_key] = 1
            hgh_inf_txt_rc = hgh_text_info_dct[gp_id]
            for r_inf, c_inf_dct in rw_dct.iteritems():
                hgh_t_i = hgh_inf_txt_rc[r_inf][0]
                row_dct = {'desc':{'v':hgh_t_i}, 'rid':sn, 'sn':sn, 'cid':sn, '$$treeLevel':1}
                for csl, inf_dct in c_inf_dct.iteritems():
                    cl_k  = col_map[csl]
                    row_dct[cl_k] = inf_dct
                data_lst.append(row_dct)
                sn += 1
            empty_rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, '$$treeLevel':0}
            data_lst.append(empty_rw_dct)
            sn += 1

        col_def_lst = []
        for cll in c_mk_dct:
            ddict = {'k':cll, 'n':cll}
            col_def_lst.append(ddict)
        col_def_lst.sort(key=lambda x:int(x['k'].split('-')[1]))
        res = {'data':data_lst, 'col_def':col_def_lst, 'map':inf_map}
        return res 
            
    def read_rcg_rawdb(self, conn, cur, numval, nl_flg):
        read_qry = """  SELECT row, col, groupid, numval FROM rawdb WHERE numval='%s' """%(numval)
        if nl_flg == 'label':
            read_qry = """  SELECT row, col, groupid, numval FROM rawdb WHERE hghtext LIKE '%{0}%' """%(numval)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        return t_data
        
    def read_formula_info(self, conn, cur):
        read_qry = """ SELECT resultant_info, formula, groupid FROM formulainfo; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        opr_res_map = {}
        res_form_dct = {}
        for row in t_data[:]:
            resultant_info, formula, groupid = row
            resultant_info_tup = eval(resultant_info)
            formula_lst        = eval(formula)  
            rop, (rr, rc), rgp, rep = resultant_info_tup
            res_str = '_'.join((str(rr), str(rc), str(groupid))) 
            lst_ops = []
            for fm_tup in formula_lst:
                try:
                    fop, (fr, fc), fgp, fep = fm_tup
                except:continue
                fr_tup = (fr, groupid)
                fr_str = '_'.join((str(fr), str(fc), str(groupid)))
                lst_ops.append(fr_tup)
                opr_res_map.setdefault(fr_str, {})[res_str] = fop
            res_form_dct[res_str] =  lst_ops 
        return opr_res_map, res_form_dct
    
    def read_rawdb_all_info(self, conn, cur, unique_list, res_data):
        t_data = []
        for tup in  unique_list + [res_data]:
            read_qry = """ SELECT row, col, groupid, gvtext, hghtext FROM rawdb WHERE row='%s' AND groupid='%s' ; """%tup
            cur.execute(read_qry)
            tdd_data = cur.fetchall()
            t_data += tdd_data
        rc_dct = {}
        col_dct = {}
        for row in t_data:
            r, c, grp_id, gvtext, hghtext  = row
            rc_dct.setdefault(r, {})[c] = {'v':gvtext, 'hgh':hghtext}
            col_dct[c] = 1
        return rc_dct, col_dct
            
    def operand_operation(self, conn, cur, numval, nl_flg):
        t_data      = self.read_rcg_rawdb(conn, cur, numval, nl_flg)
        opr_res_map, res_form_dct = self.read_formula_info(conn, cur)
            
        groupid_wise_res_info = {}
        cl_inf_dct = {}
        for row in t_data[:]:
            r, c, grp_id, n_val = row
            rr_str = '_'.join((str(r), str(c), str(grp_id)))
            if rr_str not in opr_res_map:continue
            res_info_dct = opr_res_map[rr_str]
            for res_inf in res_info_dct:
                rmss, cmss, grpss = res_inf.split('_')
                form_lst = res_form_dct.get(res_inf, [])
                if not form_lst:continue
                rc_dct, col_dct = self.read_rawdb_all_info(conn, cur, form_lst, (rmss, grpss))
                groupid_wise_res_info.setdefault(grp_id, {}).setdefault(rr_str, {})[res_inf] = rc_dct
                cl_inf_dct.setdefault(grp_id, {}).setdefault(rr_str, {})[res_inf] = col_dct
        
        data_lst = []
        sn = 1
        col_def_dct = set()
        inf_map = {}
        for grp_sn, (gp_id, op_dct) in enumerate(groupid_wise_res_info.iteritems(), 1):
            rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, 'desc':{'v':'Group-%s (%s)'%(grp_sn, grp_id), 'cls':'grid-header'}, '$$treeLevel':0}
            data_lst.append(rw_dct)
            sn += 1
            for oprn, rslt_dct in op_dct.iteritems():
                for rsl, rcs_dct in rslt_dct.iteritems():
                    for rs, cwl_dct in rcs_dct.iteritems():
                        rsw_dct = {'rid':sn, 'sn':sn, 'cid':sn, 'desc':{'v':''}, 'cls':'grid-header', '$$treeLevel':1}
                        hg_lst = []
                        cl_ky_idx = 1
                        for cs, v_dct in cwl_dct.iteritems():
                            cl_n_key = 'COL-%s'%(cl_ky_idx)
                            cl_ky_idx += 1
                            col_def_dct.add(cl_n_key)
                            hg_txt = v_dct['hgh'] 
                            hg_lst.append(hg_txt)
                            rsw_dct[cl_n_key] = v_dct
                        rsw_dct['desc'] = {'v':hg_lst[0]}
                        data_lst.append(rsw_dct)
                        sn += 1
                    empty_rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, '$$treeLevel':0}
                    data_lst.append(empty_rw_dct)
                    sn += 1
        col_def_lst = []
        for cslw in col_def_dct:    
            dmt_dct = {'k':cslw, 'n':cslw}
            col_def_lst.append(dmt_dct)
        col_def_lst.sort(key=lambda x:int(x['k'].split('-')[1]))
        return  {'data':data_lst, 'col_def':col_def_lst, 'map':inf_map}
        
    def positive_contribute_operation_negative(self, conn, cur, numval, nl_flg, pn_flg= 'P'):
        t_data      = self.read_rcg_rawdb(conn, cur, numval, nl_flg)
        opr_res_map, res_form_dct = self.read_formula_info(conn, cur)
        for row in t_data[:]:
            r, c, grp_id, n_val = row
            rr_str = '_'.join((str(r), str(c), str(grp_id)))
            if rr_str not in opr_res_map:continue
            res_info_dct = opr_res_map[rr_str]
            for res_inf, op_type in res_info_dct.iteritems():
                if pn_flg =='P':
                    if not ((int(numval) < 0 and op_type == '-') or (int(numval) > 0 and op_type == '+')):continue
                elif pn_flg =='N':
                    if (int(numval) < 0 and op_type == '-') or (int(numval) > 0 and op_type == '+'):continue
                rmss, cmss, grpss = res_inf.split('_')
                form_lst = res_form_dct.get(res_inf, [])
                if not form_lst:continue
                rc_dct, col_dct = self.read_rawdb_all_info(conn, cur, form_lst, (rmss, grpss))
                groupid_wise_res_info.setdefault(grp_id, {}).setdefault(rr_str, {})[res_inf] = rc_dct
                cl_inf_dct.setdefault(grp_id, {}).setdefault(rr_str, {})[res_inf] = col_dct
        
        data_lst = []
        sn = 1
        col_def_dct = set()
        inf_map = {}
        for grp_sn, (gp_id, op_dct) in enumerate(groupid_wise_res_info.iteritems(), 1):
            rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, 'desc':{'v':'Group-%s (%s)'%(grp_sn, grp_id), 'cls':'grid-header'}, '$$treeLevel':0}
            data_lst.append(rw_dct)
            sn += 1
            for oprn, rslt_dct in op_dct.iteritems():
                for rsl, rcs_dct in rslt_dct.iteritems():
                    for rs, cwl_dct in rcs_dct.iteritems():
                        rsw_dct = {'rid':sn, 'sn':sn, 'cid':sn, 'desc':{'v':''}, 'cls':'grid-header', '$$treeLevel':1}
                        hg_lst = []
                        cl_ky_idx = 1
                        for cs, v_dct in cwl_dct.iteritems():
                            cl_n_key = 'COL-%s'%(cl_ky_idx)
                            cl_ky_idx += 1
                            col_def_dct.add(cl_n_key)
                            hg_txt = v_dct['hgh'] 
                            hg_lst.append(hg_txt)
                            rsw_dct[cl_n_key] = v_dct
                        rsw_dct['desc'] = {'v':hg_lst[0]}
                        data_lst.append(rsw_dct)
                        sn += 1
                    empty_rw_dct = {'rid':sn, 'sn':sn, 'cid':sn, '$$treeLevel':0}
                    data_lst.append(empty_rw_dct)
                    sn += 1
        col_def_lst = []
        for cslw in col_def_dct:    
            dmt_dct = {'k':cslw, 'n':cslw}
            col_def_lst.append(dmt_dct)
        col_def_lst.sort(key=lambda x:int(x['k'].split('-')[1]))
        return {'data':data_lst, 'col_def':col_def_lst, 'map':inf_map}

    def operation_read_formula_info_flag(self, conn, cur):
        read_qry = """ SELECT resultant_info, groupid, formula FROM formulainfo """
        cur.execute(read_qry)   
        t_data = cur.fetchall()
        r_dct = {}
        operand_info_dct = {}
        for row in t_data:
            res_info, groupid, formula = row[0], row[1], row[2]
            data_tup = eval(res_info)
            formula_lst = eval(formula)
            op, (r, c), grpid, ed = data_tup
            d_str = '{0}_{1}_{2}'.format(r, c, groupid)
            cd_dct = {}
            for ix, tpl in enumerate(formula_lst):
                try:
                    ucode_op, (rp, cp), gp, ep = tpl
                except:continue
                rcp_str = '_'.join((str(rp), str(cp), str(groupid)))
                cd_dct[rcp_str] = (ix, ucode_op)
                operand_info_dct.setdefault(rcp_str, {})[d_str] = ucode_op
            r_dct[d_str] = cd_dct 
        return r_dct, operand_info_dct
        
    def read_raw_db_data(self, conn, cur, numval, formula_dct, operand_info_dct):
        read_qry = """ SELECT row_col_groupid, gvtext FROM rawdb WHERE numval=%s  """%(numval)
        cur.execute(read_qry)
        t_data = cur.fetchall() 
        res_dct = {}
        sure_resultant_dct = {} 
        sure_operand_dct   = {}
        for rw in t_data:
            row_col_groupid, gvtext = rw
            res_dct[row_col_groupid] = gvtext
            if row_col_groupid in formula_dct:
                sure_resultant_dct[row_col_groupid] = gvtext    
            if row_col_groupid in operand_info_dct: 
                sure_operand_dct[row_col_groupid] = gvtext
        return res_dct, sure_resultant_dct, sure_operand_dct
        
    def form_data_builder_data(self, conn, cur, numval, check_dct, formula_dct, operand_info_dct, row_dct):
        all_data_dct, sure_resultant_dct, sure_operand_dct  = self.read_raw_db_data(conn, cur, numval, formula_dct, operand_info_dct)

        def rec_func(form_lst, formula_dct):
            rs_dct = {}
            for fm_str in form_lst:
                rs, cs, gp_s = fm_str.split('_')
                rs_dct[(rs, gp_s)] = 1
                if fm_str in formula_dct:
                    ch_form_lst = formula_dct.get(fm_str)
                    ch_rs_dct = {}
                    if ch_form_lst:
                        ch_rs_dct = rec_func(ch_form_lst, formula_dct)
                    rs_dct.update(ch_rs_dct)
            return rs_dct
         
        if row_dct.get('Resultant'):
            for rbw in sure_resultant_dct:
                r, c, gpid = rbw.split('_')
                check_dct[(r, gpid)]  = 1
                op_forml_lst = formula_dct.get(rbw)
                if op_forml_lst:
                    f_rgrp = rec_func(op_forml_lst, formula_dct)
                    check_dct.update(f_rgrp)

        op_flg    = row_dct.get('Operand')
        op_pc_flg = row_dct.get('Positively contribute')
        op_nc_flg = row_dct.get('Negatively contribute')
        print [op_flg, op_pc_flg, op_nc_flg]
        if ((op_flg) and ((not op_pc_flg) and (not op_nc_flg))) or (op_flg and (op_pc_flg and op_nc_flg)):
            print 'HERE'
            for rbw in sure_operand_dct:
                r, c, gpid = rbw.split('_')
                check_dct[(r, gpid)]  = 1 
                op_forml_lst = formula_dct.get(rbw)
                if op_forml_lst:
                    f_rgrp = rec_func(op_forml_lst, formula_dct)
                    check_dct.update(f_rgrp)
        
        elif (op_flg and (not op_pc_flg and op_nc_flg)):
            for rbw in sure_operand_dct:
                r, c, gpid = rbw.split('_')
                oper_res_pn_dct = operand_info_dct.get(rbw)
                if not oper_res_pn_dct:continue
                nm_vl = self.read_numval(conn, cur, rbw)    
                bknc_flg = 0
                for r_inf, op_val in oper_res_pn_dct.iteritems():
                    if bknc_flg:break
                    if ((op_val == '-') and (int(nm_vl) > 0)) or ((op_val == '+') and (int(nm_vl) < 0)):
                        check_dct[(r, gpid)] = 1
                        op_forml_lst = formula_dct.get(rbw)
                        if op_forml_lst:
                            f_rgrp = rec_func(op_forml_lst, formula_dct)
                            check_dct.update(f_rgrp)
                        bknc_flg = 1

        elif (op_flg and (op_pc_flg and not op_nc_flg)):
            for rbw in sure_operand_dct:
                r, c, gpid = rbw.split('_')
                oper_res_pn_dct = operand_info_dct.get(rbw)
                if not oper_res_pn_dct:continue
                nm_vl = self.read_numval(conn, cur, rbw) 
                bkpc_flg = 0
                for r_inf, op_val in oper_res_pn_dct.iteritems():
                    if bkpc_flg:break
                    if (((op_val == '-') and (int(nm_vl) < 0))) or (((op_val == '+') and (int(nm_vl) > 0))):
                        check_dct[(r, gpid)] = 1
                        op_forml_lst = formula_dct.get(rbw)
                        if op_forml_lst:
                            f_rgrp = rec_func(op_forml_lst, formula_dct)
                            check_dct.update(f_rgrp)
                        bkpc_flg = 1
        return check_dct
        
    def read_numval(self, conn, cur, rcg):
        read_qry = """ SELECT numval FROM rawdb WHERE row_col_groupid='%s'  """%(rcg)   
        cur.execute(read_qry)
        numval= cur.fetchone()[0]
        return numval
        
        
    def label_cred_func(self, conn, cur, num_val, formula_dct):
        read_qry = """ SELECT row_col_groupid FROM rawdb WHERE hghtext LIKE '%{0}%'  """.format(num_val)
        cur.execute(read_qry)
        t_data = cur.fetchall()

        def rec_func(form_lst, formula_dct):
            rs_dct = {}
            for fm_str in form_lst:
                rs, cs, gp_s = fm_str.split('_')
                rs_dct[(rs, gp_s)] = 1
                if fm_str in formula_dct:
                    ch_form_lst = formula_dct.get(fm_str)
                    ch_rs_dct = {}
                    if ch_form_lst:
                        ch_rs_dct = rec_func(ch_form_lst, formula_dct)
                    rs_dct.update(ch_rs_dct)
            return rs_dct

        rs_dct = {}
        for r_tup in t_data:
            row, col, groupid = r_tup[0].split('_')
            rs_dct[(row, groupid)] = 1
            if r_tup[0]  in formula_dct:
                op_forml_lst = formula_dct.get(r_tup[0])
                if op_forml_lst:
                    f_rgrp = rec_func(op_forml_lst, formula_dct)
                    rs_dct.update(f_rgrp)
        return rs_dct
                
    def search_operation_data_flgs(self, ijson):
        company_id = ijson['company_id']
        doc_id     = ijson['doc_id']
        data_lst = ijson['data']
        db_path = config.Config.doc_wise_db.format(company_id, doc_id) 
        conn, cur = self.connect_to_sqlite(db_path)
        formula_dct, operand_info_dct = self.operation_read_formula_info_flag(conn, cur)
        check_dct = {}
        for row_dct in data_lst:
            name_type = row_dct['type']
            num_val   = row_dct['v']
            del row_dct['type'] 
            del row_dct['v']
            if name_type  == 'Number':
                update_data_dct = self.form_data_builder_data(conn, cur, num_val, check_dct, formula_dct, operand_info_dct, row_dct)
                check_dct.update(update_data_dct)
            elif name_type  == 'Label':
                update_data_dct = self.label_cred_func(conn, cur, num_val, formula_dct)
                check_dct.update(update_data_dct)
        import tree_view_data_builder as pyf
        t_Obj = pyf.GridModel()
        res = t_Obj.read_rawdb_tree(ijson, check_dct, 1) 
        return res
                

if __name__ == '__main__':
    #obj = read_rawdb() 
    obj = GridModel() 
    #ijson = {"company_id":"1_89", "doc_id":"11", 'nl_flg':'label', 'data':[{'s_flg':'Resultant', 'val':'INVESTMEN'}]}
    ijson = {"company_id":"1_89", "doc_id":"11", 'nl_flg':'label', 'data':[{'type':'Number', 'v':142712, 'Resultant':True, 'Operand':True}]}
    print obj.search_operation_data(ijson)
    #print obj.search_operation(ijson)
    #print obj.read_formula_cell_id(ijson)
    #print obj.read_rawdb_tree(ijson)
    #print obj.read_docs(ijson)
    #print obj.cal_bobox_data_new('1', '89', '1_89', '11')
