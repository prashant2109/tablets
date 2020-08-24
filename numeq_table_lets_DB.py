import os, sys, sqlite3, lmdb, ast, json, copy
import shelve
from collections import OrderedDict as OD, defaultdict as DD
from itertools import permutations, combinations

import report_year_sort as rys
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
from igraph  import *

class Table_Lets_DB(object):
        
    def __init__(self):
        self.formula_nong_map = {'1':'Ratio', '2':'Percentage', '3':'Comparison'}

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

    def read_grid_cell_dict_only(self, table_id, grid_json):
        dpg = '_'.join(table_id.split('#'))
        ddict = grid_json.get('data', {})
        r_cs = ddict.keys()
        r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
        rc_d    = {}
        for r_c in r_cs:
            row, col = int(r_c.split('_')[0]), int(r_c.split('_')[1])  
            rc_d.setdefault(row, {})[col]   =  ddict[r_c]
            
        rows = rc_d.keys()
        rows.sort()

        gv_cell_info = {}                
        rc_combs = {}
        section_type_info = {} 
        for row in rows:
            cols    = rc_d[row].keys()
            cols.sort()
            ##sys.exit()
            row_dct = {}
            taxo = ''
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
                section_type_info[(str(row), str(col))] = stype
                if stype not in ('GV', ):continue
                rc_i = '_'.join(map(str, [row, col]))
                xml_id = '#'.join(cell.get('xml_ids', '').split('$$'))
                rc_combs.setdefault('C', {}).setdefault(str(row), []).append((rc_i, xml_id))
                rc_combs.setdefault('R', {}).setdefault(str(col), []).append((rc_i, xml_id))
                rc_combs.setdefault('G', {}).setdefault('G', []).append((rc_i, xml_id))
                section_type_info[(str(row), str(col))] = stype
        return rc_combs, section_type_info

    def test_read_grid_cell_dict_only(self, table_id, grid_json):
        dpg = '_'.join(table_id.split('#'))
        ddict = grid_json.get('data', {})
        r_cs = ddict.keys()
        r_cs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
        rc_d    = {}
        for r_c in r_cs:
            row, col = int(r_c.split('_')[0]), int(r_c.split('_')[1])  
            rc_d.setdefault(row, {})[col]   =  ddict[r_c]
            
        rows = rc_d.keys()
        rows.sort()

        table_xmls_dct = {}
        for row in rows:
            cols    = rc_d[row].keys()
            cols.sort()
            ##sys.exit()
            row_dct = {}
            taxo = ''
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
                if stype not in ('GV', ):continue
                xml_id = '#'.join(cell.get('xml_ids', '').split('$$'))
                table_xmls_dct[xml_id] = '{0}_{1}'.format(row, col)
        return table_xmls_dct
        
    def read_hgh_text_index_wise(self, company_id, doc_id):
        hght_txt_path = '/mnt/eMB_db/company_management/{0}/equality/{1}_lexacross.slv'.format(company_id, doc_id)
        print 'hght_txt_path', [hght_txt_path]
        sh = shelve.open(hght_txt_path)
        data = sh['data']
        sh.close()
        hgh_txt_lst = data['all_tree_node']
        idx_wise_text = {}
        consolidated_idx_lst = []
        for tpl in hgh_txt_lst:
            hgh_txt_idx = tpl[0].strip()
            #print [hgh_txt_idx]
            idx, hgh_txt = hgh_txt_idx.split('@')
            txt_pat = 'NGRAM'
            if '.' in idx:
                txt_pat = 'MATCH_WORDS'
            idx_wise_text[idx] = (hgh_txt, txt_pat)
            consolidated_idx_lst.append((idx, hgh_txt))
        #sys.exit()
        return idx_wise_text, consolidated_idx_lst 
        
    def mysql_connection(self, db_data_lst):
        import MySQLdb
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur
        
    def read_json_file(self, table_id, company_id):
        print 'READ ',(company_id, table_id)
        json_file_path = '/mnt/eMB_db/company_management/{0}/json_files/{1}.json'.format(company_id, table_id)
        json_dct = {}
        if not os.path.exists(json_file_path):
            return json_dct
        with open(json_file_path, 'r') as j:
            json_dct = json.load(j)
        return json_dct

    def read_shelve_file_hgh_equality_only(self, company_id, doc_id, hgh_txt_idx_map, table_json_dct):

        #print ' in read_shelve_file_hgh_equality_only'

        shl_path = '/mnt/eMB_db/company_management/{0}/equality/lextree_{1}.slv'.format(company_id, doc_id)
        print shl_path
        sh = shelve.open(shl_path)
        data = sh['data']
        sh.close() 
        idx_wise_dct = {}
        #table_json_dct = {}
        grid_wise_rcs = {}
        sec_typ_dct = {}
        idx_val_d   = {}
        for grid, rc_dct in data.iteritems():
            gr_id = '_'.join(grid.split('#')[:3])
            #if gr_id not in ['2_10_5']:continue
            grid_json = table_json_dct.get(gr_id, self.read_json_file(gr_id, company_id))
            table_json_dct[gr_id] = grid_json
            rc_combs, section_type_info = self.read_grid_cell_dict_only(gr_id, grid_json)                     
            grid_wise_rcs[gr_id] = rc_combs  
            sec_typ_dct[gr_id] = section_type_info

            for rc, d_lst in rc_dct.iteritems():
                #print
                for idx_tup in d_lst:
                    idx_lst = idx_tup[4]
                    #print rc, idx_tup
                    sted    = idx_tup[2]
                    txt_inf = grid_json['data']['%s_%s'%rc]['data'].lower()[sted[0]:sted[1]]
                    for tmpi, idx in enumerate(idx_lst):
                        rc_s = tuple(map(str, rc))
                        idx_val_d[idx]   = rc
                        idx_wise_dct.setdefault(idx, []).append((gr_id, rc_s))
                        txt_pat = 'NGRAM'
                        if '.' in idx:
                            txt_pat = 'MATCH_WORDS'
                        hgh_txt_idx_map[idx]    = (txt_inf, txt_pat)
                    #print ' xxx: ', [ rc, idx_tup, txt_inf , grid_json['data']['%s_%s'%rc]['data'].lower() ] 
        
        #print idx_wise_dct
        #sys.exit()
        collect_lst_idx = {}
        for ix, grd_lst in idx_wise_dct.iteritems():
            txt_inf, txt_patt = hgh_txt_idx_map.get(ix, ('', ''))
            ix_tup = (ix, txt_inf)
            #print 'ix_tup: ', ix_tup    
            ntext   = ''
            if not txt_inf:
                ntext   = 'Y'
            for grd_tup in grd_lst:
                gr, rc_i =   grd_tup
                #print grd_tup
                t_grid_json = table_json_dct.get(gr, {})['data'] 
                if ntext    == 'Y':
                    txt_inf = t_grid_json['%s_%s'%rc_i]['data'].lower()
                    txt_patt    = 'MATCH_WORDS'
                    ix_tup = (ix, txt_inf)
                stype = sec_typ_dct.get(gr, {}).get(rc_i, '')
                if not stype:continue
                #print grd_tup, stype
                row_col_str = 'C'
                rc_info = rc_i[0]
                if stype == 'HGH':
                    hgh_vgh_i = 'H'
                elif stype == 'VGH':
                    hgh_vgh_i = 'V'
                elif stype == 'GH':
                    hgh_vgh_i = 'G'
                    row_col_str = 'G'
                    rc_info = 'G'
                #sec_typ_dct
                all_rcs = grid_wise_rcs.get(gr, {}).get(row_col_str, {}).get(rc_info, []) 
                for r_c_tup in all_rcs:
                    #xml_dct = t_grid_json.get(r_c, {})
                    #xml_1 = '#'.join(xml_dct.get('xml_ids', '').split('$$'))
                    #g_rc_tup = (gr, r_c)
                    r_c, xml = r_c_tup 
                    g_rc_tup = (gr, xml)
                    ix_l = collect_lst_idx.setdefault(g_rc_tup, {}).setdefault(hgh_vgh_i, {}).setdefault(txt_patt, [])
                    if ix_tup not in ix_l:
                        ix_l.append(ix_tup)
                    collect_lst_idx[g_rc_tup][hgh_vgh_i][txt_patt] = ix_l
                if stype != 'GH':
                    row_col_str = 'R'
                    rc_info = rc_i[1]
                    #hgh_vgh_i = 'V'
                    all_rcs = grid_wise_rcs.get(gr, {}).get(row_col_str, {}).get(rc_info, []) 
                    #print 'all_rcs ', all_rcs
                    for r_c_tup in all_rcs:
                        #xml_dct = t_grid_json.get(r_c, {})
                        #xml_1 = '#'.join(xml_dct.get('xml_ids', '').split('$$'))
                        #g_rc_tup = (gr, r_c)
                        r_c, xml = r_c_tup 
                        g_rc_tup = (gr, xml)
                        ix_l = collect_lst_idx.setdefault(g_rc_tup, {}).setdefault(hgh_vgh_i, {}).setdefault(txt_patt, [])
                        if ix_tup not in ix_l:
                            ix_l.append(ix_tup)
                        collect_lst_idx[g_rc_tup][hgh_vgh_i][txt_patt] = ix_l
        
        if 0:
            for k, v in collect_lst_idx.items():
                for k1, v1 in v.items():
                    print [k, v, k1,  v1]
            #sys.exit()
        return collect_lst_idx 

    def get_num_equa_fact(self, f1, f2):
        r_f1 = int(f2)-int(f1)
        r_f2 = int(f1)-int(f2)
        return r_f1, r_f2 



    def create_undirected_graph(self, n, edges, labels, color_edges, vertex_dict):
            G = Graph()
            G.add_vertices(n)
            #G.to_directed(False)
            G.to_undirected(True)  # safe check
            G.vs["name"] = labels
            #print labels

            #print edges
            #sys.exit()  

            #sys.exit()  
            G.add_edges(edges)
            prop_dict = {}
            for p in labels:
                all_props = vertex_dict.get(p, {}).keys()
                for all_prop in all_props:
                    if all_prop not in prop_dict:
                       prop_dict[all_prop] = []
                    prop_dict[all_prop].append(vertex_dict[p][all_prop])
            
            for k, vs in prop_dict.items():
                G.vs[k] = vs  

            for k, vs in color_edges.items():  
                G.es[k] = vs
            return G

    def create_vertex_map(self, dir_edges, color_edges):
          all_vertex = []
          for edg in dir_edges:
              all_vertex.append(edg[0])
              all_vertex.append(edg[1])
          all_vertex = list(set(all_vertex))
          all_vertex.sort()
          vertice_dict = {}
          for i, e in enumerate(all_vertex):
              vertice_dict[e] = i

          #print "create_vertex_map"
          new_dir_edges = []
          new_prop_dict = {} 
          for ind, edg in enumerate(dir_edges):
              mytup = (vertice_dict[edg[0]], vertice_dict[edg[1]])
              #print ' >>> ', edg, mytup 
              for prop, values in  color_edges.items():
                  #print '             ***** prop: ', prop, values[edg]
                  if prop not in new_prop_dict:
                     new_prop_dict[prop] = [] 
                  new_prop_dict[prop].append(values[edg])
              new_dir_edges.append(mytup)
          #sys.exit()  
          return vertice_dict, new_dir_edges, len(vertice_dict.keys()), all_vertex, new_prop_dict  

    def create_graphs(self, dir_edges, color_edges, vertex_dict):
          # dir_edges directed edge
          vdict, new_dir_edges, no_vertices, vlabels, new_prop_dict = self.create_vertex_map(dir_edges, color_edges)
          g = self.create_undirected_graph(no_vertices, new_dir_edges, vlabels, new_prop_dict, vertex_dict) 
          return g, vlabels 

    def writeGraphML(self, G, fpath):
          G.write_graphml(fpath)
          return 
 
    def form_number_graph_ds(self, num_d, dirname):


        #print num_d.keys()
        #sys.exit()   
        os.system('rm -rf '+dirname)
        os.system('mkdir -p '+dirname)

        all_num_d = {}  
        for k, vs in num_d.items():
            gid = k[0]
            if gid not in all_num_d:
               all_num_d[gid] = []
            all_num_d[gid].append(k)
  
        for gid, vs in all_num_d.items():
            #if (gid != '4_3_11'): continue
            
            mdirname = dirname+'/'+str(gid)+'.graphml'

            print mdirname, len(vs)  
            edgs = []
            factors = {}
            node_dict = {} 
            repeat_data = {} 
            for v in vs:
                o_vs = num_d[v]
                this_cell = v[0]+'^'+v[1]
                #print 'v : ', v  
                for e in o_vs:
                    other_cell = e[1][0]+'^'+e[1][1]
                    #print e
                    factor = e[2]
                    if (this_cell, other_cell) in repeat_data: continue
                    edgs.append((this_cell, other_cell))
                    repeat_data[(this_cell, other_cell)] = 1  
                    if 'factor' not in factors:
                       factors['factor'] = {}
                    factors['factor'][(this_cell, other_cell)] = factor
                    #print ' factor look here: ', (this_cell, other_cell, factor)

            #sys.exit()
            g, labels = self.create_graphs(edgs, factors, node_dict)
            self.writeGraphML(g, mdirname)
            #print 'Writing done: ', g 
            #print mdirname , ' -- ', len(edgs)
            del g
            #sys.exit() 

    def handle_label(self, label_eq_dct, dirname):

        all_ks =  label_eq_dct.keys()

        table_dict = {}
        for k in all_ks[:]:
            if k[0] not in table_dict:
               table_dict[k[0]] = []
            table_dict[k[0]].append(k)

        os.system('rm -rf '+dirname)
        os.system('mkdir -p '+dirname)

        for table, vs in table_dict.items():

            #print '================================================'
            #if table != '2_45_1': continue 
            #print table
            mdirname = dirname+table+'.graphml'  
            #n = len(vs) 
            #G = Graph()
            #G.add_vertices(n)
            #G.to_directed(False)
            #G.to_undirected(True)  # safe check
            #nvs = map(lambda x:x[0]+'_'+x[1], vs[:])
            #G.vs["name"] = nvs[:]
            #G.vs["txmatch"] =

            #print table, vs 

            #sys.exit()
            ar = [] 
            for v in vs:
                #print '========================================'
                nodev = v[0]+'^'+v[1]
                #print 'XMLID: : ', nodev
                mdict = label_eq_dct[v]      

                #print mdict
                #sys.exit()  
                for mkey, vs in mdict.items():
                    vh = mkey
                    for mtype, tx_vals in vs.items():
                        #print 'match_type: ', mtype
                        for tx_val in tx_vals:
                            tx_level = tx_val[0]
                            tx_txt = tx_val[1]
                            #print ' tx_level: ', tx_level
                            #print ' tx_txt: ', tx_txt
                            ar.append((len(ar), nodev, mtype, tx_level, tx_txt, vh)) # d, node, matchtype, txlvl, tx_txt       
                            print [ (len(ar), nodev, mtype, tx_level, tx_txt, vh) ] 
        
                
            n = len(ar)
            G = Graph()
            G.add_vertices(n)
            G.to_directed(False)
            G.to_undirected(True)  # safe check
            G.vs["name"] = map(lambda x:x[0], ar[:])
            G.vs["node"] = map(lambda x:x[1], ar[:])
            G.vs["matchtype"] = map(lambda x:x[2], ar[:])
            G.vs["tx_level"] = map(lambda x:x[3], ar[:])
            G.vs["txt"] = map(lambda x:x[4], ar[:])
            G.vs["VH"] = map(lambda x:x[5], ar[:])

            self.writeGraphML(G, mdirname)
            del G 
            print 'label: ', mdirname
            #sys.exit() 
             
    def form_num_label_info(self, ijson):
        company_id          = ijson['company_id']
        if company_id in ('20_118', 'TestDB'):
            company_id = 'TestDB'
        try:
            project_id, deal_id = company_id.split('_')
        except:project_id, deal_id = '', ''
        doc_id          = ijson['doc_id']
        
        ##############################################################################################
        try:
            db_path = '/mnt/eMB_db/company_management/{0}/equality/{1}_numeq.db'.format(company_id, doc_id)
            conn, cur = self.connect_to_sqlite(db_path)
            read_qry = """ SELECT gp1, row1, col1, fact1, gp2, row2, col2, fact2 FROM equalitydb; """
            cur.execute(read_qry)
            t_data = cur.fetchall()
            conn.close()
        except:t_data = ()
        #print t_data
        num_eq_dct = {}
        #  ('NUMBEREQ', (5131_10_1,  1_0), fact, {}),
        table_json_dct = {}
      
        print 'Eq Len: ', len(t_data)  
        #t_data = [] # doc 42 - hardcoding 
        for row_data in t_data:  
            gp1, row1, col1, fact1, gp2, row2, col2, fact2 = row_data


            if fact1 != '0': continue 
            if fact2 != '0': continue 

            #print row_data
            #sys.exit() 

            r_c1 = '{0}_{1}'.format(row1, col1)
            r_c2 = '{0}_{1}'.format(row2, col2)
            table1  = '_'.join(gp1.split('#')[:3])
            table2  = '_'.join(gp2.split('#')[:3])
            if table1 not in table_json_dct:
                table_json_dct[table1]  = self.read_json_file(table1, company_id)
            if table2 not in table_json_dct:
                table_json_dct[table2]  = self.read_json_file(table2, company_id)
            t1_grid_json    = table_json_dct[table1]
            t2_grid_json    = table_json_dct[table2]
            if (not t1_grid_json) or (not t2_grid_json):continue
            #print t1_grid_json, [table1, r_c1]
            xml_1_dct = t1_grid_json['data'].get(r_c1, {})
            xml_2_dct = t2_grid_json['data'].get(r_c2, {})
            
            xml_1 = '#'.join(xml_1_dct.get('xml_ids', '').split('$$'))
            xml_2 = '#'.join(xml_2_dct.get('xml_ids', '').split('$$'))
            #print xml_1, xml_2  
            if 1:#(table1 in  ['4_29_1', '4_6_1']) and  table2  in  ['4_29_1', '4_6_1']:
                table1_rc_tup = (table1, xml_1)
                table2_rc_tup = (table2, xml_2)
                r_f1, r_f2 = self.get_num_equa_fact(fact1, fact2)
                dtup_1 = ('N', table2_rc_tup, r_f1)
                num_eq_dct.setdefault(table1_rc_tup, []).append(dtup_1)
                dtup_2 = ('N', table1_rc_tup, r_f2)
                num_eq_dct.setdefault(table2_rc_tup, []).append(dtup_2)

        if 0:
            print 'check here' 
            for k, vs in num_eq_dct.items():
                if k[0] != '4_3_11': continue 
                print k
                for v in vs:
                    if v[2] == 0:
                       print ' >> ', v  
            #print num_eq_dct
            sys.exit()
        hgh_txt_idx_map, consolidated_idx_lst = self.read_hgh_text_index_wise(company_id, doc_id)
        label_eq_dct = self.read_shelve_file_hgh_equality_only(company_id, doc_id, hgh_txt_idx_map, table_json_dct)
        return num_eq_dct, label_eq_dct, consolidated_idx_lst


    def test_form_num_label_info(self, ijson, num_dir="/tmp/teststore/", lable_dir="/tmp/teststore2/", sh_dir="/tmp/teststore3/"):
        company_id          = ijson['company_id']
        if company_id in ('20_118', 'TestDB'):
            company_id = 'TestDB'
        try:
            project_id, deal_id = company_id.split('_')
        except:project_id, deal_id = '', ''
        doc_id          = ijson['doc_id']
        
        ##############################################################################################
        db_path = '/mnt/eMB_db/company_management/{0}/equality/{1}_numeq.db'.format(company_id, doc_id)
        conn, cur = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT gp1, row1, col1, fact1, gp2, row2, col2, fact2 FROM equalitydb; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        #print t_data
        num_eq_dct = {}
        #  ('NUMBEREQ', (5131_10_1,  1_0), fact, {}),
        table_json_dct = {}
        
        for row_data in t_data:  
            gp1, row1, col1, fact1, gp2, row2, col2, fact2 = row_data


            #print row_data
            #sys.exit() 

            r_c1 = '{0}_{1}'.format(row1, col1)
            r_c2 = '{0}_{1}'.format(row2, col2)
            table1  = '_'.join(gp1.split('#')[:3])
            table2  = '_'.join(gp2.split('#')[:3])
            if table1 not in table_json_dct:
                table_json_dct[table1]  = self.read_json_file(table1, company_id)
            if table2 not in table_json_dct:
                table_json_dct[table2]  = self.read_json_file(table2, company_id)
            t1_grid_json    = table_json_dct[table1]
            t2_grid_json    = table_json_dct[table2]
            if (not t1_grid_json) or (not t2_grid_json):continue
            #print t1_grid_json, [table1, r_c1]
            xml_1_dct = t1_grid_json['data'].get(r_c1, {})
            xml_2_dct = t2_grid_json['data'].get(r_c2, {})
            
            xml_1 = '#'.join(xml_1_dct.get('xml_ids', '').split('$$'))
            xml_2 = '#'.join(xml_2_dct.get('xml_ids', '').split('$$'))
            print xml_1, xml_2  
            if 1:#(table1 in  ['4_29_1', '4_6_1']) and  table2  in  ['4_29_1', '4_6_1']:
                table1_rc_tup = (table1, xml_1)
                table2_rc_tup = (table2, xml_2)
                r_f1, r_f2 = self.get_num_equa_fact(fact1, fact2)
                dtup_1 = ('N', table2_rc_tup, r_f1)
                num_eq_dct.setdefault(table1_rc_tup, []).append(dtup_1)
                dtup_2 = ('N', table1_rc_tup, r_f2)
                num_eq_dct.setdefault(table2_rc_tup, []).append(dtup_2)

        if 0:
            print 'check here' 
            for k, vs in num_eq_dct.items():
                if k[0] != '4_3_11': continue 
                print k
                for v in vs:
                    if v[2] == 0:
                       print ' >> ', v  
            #print num_eq_dct
            sys.exit()

        #sys.exit()  
        self.form_number_graph_ds(num_eq_dct, num_dir)

        #sys.exit()
        hgh_txt_idx_map, consolidated_idx_lst = self.read_hgh_text_index_wise(company_id, doc_id)

        label_eq_dct, table_id_xml_dct = self.test_read_shelve_file_hgh_equality_only(company_id, doc_id, hgh_txt_idx_map, table_json_dct)    

        self.handle_label(label_eq_dct, lable_dir)

        os.system('mkdir -p '+sh_dir)
         
        sh = shelve.open(sh_dir+"ctree.sh", "n")
        sh["data"] = consolidated_idx_lst
        sh.close()
        
        print "done writing in shelve file: ", sh_dir+"ctree.sh"       

        #print label_eq_dct
        #sys.exit()  
        print 
        return num_eq_dct, label_eq_dct, consolidated_idx_lst, table_id_xml_dct
        
    def find_error_tables_map_label_equality(self, ijson):
        company_id = ijson['company_id']
        dc_id = ijson['doc_id']
        db_path = '/mnt/eMB_db/company_management/{0}/table_info.db'.format(company_id)
        conn, cur = self.connect_to_sqlite(db_path)        
        read_qry = """ SELECT doc_id, page_no, grid_id FROM table_mgmt WHERE doc_id='%s'; """%(dc_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        all_docs = {}
        all_doc_grids = {}
        for row_data in t_data:
            doc_id, page_no, grid_id = row_data
            all_docs[doc_id] = 1
            t_id = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)
            all_doc_grids.setdefault(str(doc_id), {})[t_id] = 1
        #return {}, t_data
           
        res_dct = {}    
        label_does_not_exists = {}
        for doc in all_docs:
            #if doc != 4:continue
            doc = str(doc)
            ijson['doc_id'] = doc
            num_eq_dct, label_eq_dct, consolidated_idx_lst   = self.form_num_label_info(ijson)
            
            grid_wise_label_info = {}
            for tab_xml_tup in label_eq_dct.keys():
                tb_id, xml = tab_xml_tup
                grid_wise_label_info.setdefault(tb_id, {})[xml] = 1   
            print '#######################################################'
            
            doc_grids = all_doc_grids[doc]
            table_id_xml_dct = {}
            for gr_id in doc_grids:
                grid_json = self.read_json_file(gr_id, company_id)
                table_xml_dct = self.test_read_grid_cell_dict_only(gr_id, grid_json)
                table_id_xml_dct[gr_id] = table_xml_dct
            
            for grid, xml_dct in table_id_xml_dct.iteritems():
                table_lbl_info = grid_wise_label_info.get(grid, {})
                if not table_lbl_info:
                    res_dct.setdefault(str(doc), {}).setdefault(grid, {}).setdefault("GRID DOES NOT EXISTS IN LABEL INFO", {})['ALL RCs'] = 1
                    continue
                for xm, rc in xml_dct.iteritems():
                    if xm not in table_lbl_info:
                        res_dct.setdefault(str(doc), {}).setdefault(grid, {}).setdefault("XML DOES NOT EXISTS IN LABEL INFO", {})[rc] = 1
        return res_dct, t_data

if __name__ == '__main__':
    obj = Table_Lets_DB() 
    ijson = {"company_id":"1117", "doc_id":5131}
    ijson = {"company_id":"1053730", "doc_id":144}
    #obj.form_num_label_info(ijson)
    print obj.find_error_tables_map_label_equality(ijson)
    

