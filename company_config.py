import os, sys
#####################

import config

import db.get_conn as get_conn
conn_obj    = get_conn.DB()

import report_year_sort as rys


class CompanyInfo:  
    
    def read_industry(self, cur, industry_id):
        #ID industryName
        read_qry = """ SELECT industryName FROM industrytype WHERE ID={0}; """.format(industry_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()
        industry = None
        if t_data:
            industry = t_data[0]    
        return industry
        
    def read_ticker(self, cur, company_id):
        # company_id    ticker 
        read_qry = """ SELECT ticker FROM Ticker WHERE company_id={0}; """.format(company_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()
        ticker = None
        if t_data:
            ticker = t_data[0]
        return ticker
        
    def read_country(self, cur, country_id):
        read_qry = """ SELECT country FROM country WHERE id={0}; """.format(country_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()
        country = None
        if t_data:
            country = t_data[0]
        return country

    def read_currency(self, currency_id):
        read_qry = """ SELECT currency FROM Currency WHERE id={0}; """.format(currency_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()
        currency = None
        if t_data:
            currency = t_data[0]
        return currency

    def read_company_info(self, conn, cur, company_id, phs):
        read_qry = """ SELECT company_display_name, industry, country, currency FROM company_mgmt WHERE row_id={0}; """.format(company_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()
        company_display_name, industry_id, country_id, currency_id = t_data
        industry = ''
        if industry_id:
            industry = self.read_industry(cur, industry_id)
        
        country = ''
        if country_id:
            country = self.read_country(cur, country_id)
        
        currency = ''
        if currency_id:
            currency = self.read_currency(currency_id)
        
        ticker = self.read_ticker(cur, company_id)
        conn.close()
        
        key_lst = ['Company Name', 'Industry', 'Ticker', 'Country', 'Currency']
        val_lst = [company_display_name, industry, ticker, country, currency]
            
        data_lst = []
        sn = 1
        #phs = ['12M2019', '11M2019', '10M2019', '9M2019']
        for ix, r in enumerate(key_lst):
            val = val_lst[ix]
            if not val: continue
            r_d = {'rid':sn, 'cid':sn, 'key':{'v':r}, 'value':{'v':val}}
            for idx, ph in enumerate(phs):
                r_d['key_%s'%(idx)] = {'v':r}
                r_d['value_%s'%(idx)] = {'v':val}
            data_lst.append(r_d)
            sn += 1
        
        col_def = [] #{'k':'key', 'n':'Description'}, {'k':'value', 'n':'Value'}]
        for idx, ph in enumerate(phs):
            col_def.append({'k':'key_'+str(idx), 'g':'Description', 'n':ph})
        for idx, ph in enumerate(phs):
            col_def.append({'k':'value_'+str(idx), 'g':'Value', 'n':ph})
        
        map_inf = {}
        return data_lst, col_def, map_inf
    
        
    def read_client_info(self, cur, company_id, project_id, phs):
        read_qry = """ SELECT cm.row_id, cd.client_name, cd.client_id FROM client_details AS cd INNER JOIN client_mgmt AS cm ON cd.project_id=cm.row_id WHERE cd.company_id={0} AND cm.row_id={1}; """.format(company_id, project_id)
        cur.execute(read_qry)
        t_data = cur.fetchall()
            
        #phs = ['12M2019', '11M2019', '10M2019', '9M2019']
        data_lst = []
        sn = 1
        for rw_dt in t_data:
            p_id, client_name, client_id = rw_dt
            row_dct = {'rid':sn, 'cid':sn,  'value':{'v':client_name}, 'id':{'v':client_id}}
            for idx, ph in enumerate(phs):
                row_dct['id_%s'%(idx)] = {'v':client_id}
                row_dct['value_%s'%(idx)] = {'v':client_name}
            data_lst.append(row_dct)
            sn +=1
        col_def = [] #[{'k':'id', 'n':'Client ID'}, {'k':'value', 'n':'Client Name'}]
        for idx, ph in enumerate(phs):
            col_def.append({'k':'id_'+str(idx), 'g':'Description', 'n':ph})
        for idx, ph in enumerate(phs):
            col_def.append({'k':'value_'+str(idx), 'g':'Client Name', 'n':ph})

        map_inf = {}
        return data_lst, col_def, map_inf 
        
    def read_sec_sector(self, cur, sec_sector_id):
        read_qry = """ SELECT sector FROM sec_sector WHERE ID={0}; """.format(sec_sector_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()
        sec_sector = ''
        if t_data:
            sec_sector = t_data[0]
        return sec_sector    

    def read_sec_industry(self, cur, sec_industry_id):
        read_qry = """ SELECT industryName FROM sec_industry WHERE ID={0}; """.format(sec_industry_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()
        sec_industry = ''
        if t_data:
            sec_industry = t_data[0]
        return sec_industry 
        
    def read_sec_info(self, conn, cur, company_id, phs):
        read_qry = """ SELECT sec_cik, sec_name, sec_sector, sec_industry FROM company_mgmt WHERE row_id={0}; """.format(company_id)
        cur.execute(read_qry)
        t_data = cur.fetchone()
        sec_cik, sec_name, sec_sector_id, sec_industry_id = t_data
        sec_sector = ''
        if sec_sector_id:
            sec_sector = self.read_sec_sector(cur, sec_sector_id)     
        sec_industry = ''
        if sec_industry_id:
            sec_industry = self.read_sec_industry(cur, sec_industry_id)
        
        key_lst = ['SEC CIK', 'SEC Name', 'SEC Sector', 'SEC Industry']
        val_lst = [sec_cik, sec_name, sec_sector, sec_industry]

        #phs = ['12M2019', '11M2019', '10M2019', '9M2019']
        data_lst = []
        sn = 1
        for ix, r in enumerate(key_lst):
            val = val_lst[ix]
            if not val: continue
            r_d = {'rid':sn, 'cid':sn, 'key':{'v':r}, 'value':{'v':val}}
            for idx, ph in enumerate(phs):
                r_d['key_%s'%(idx)] = {'v':r}
                r_d['value_%s'%(idx)] = {'v':val}
            data_lst.append(r_d)
            sn += 1
        
        col_def = [] #[{'k':'key', 'n':'Description'}, {'k':'value', 'n':'Value'}]
        for idx, ph in enumerate(phs):
            col_def.append({'k':'key_'+str(idx), 'g':'Description', 'n':ph})
        for idx, ph in enumerate(phs):
            col_def.append({'k':'value_'+str(idx), 'g':'Value', 'n':ph})
        
        map_inf = {}
        return data_lst, col_def, map_inf
        
    def read_doc_info_ph(self, company_id):
        cid_path = config.Config.comp_path.format(company_id)
        db_path = os.path.join(cid_path, 'document_info.db')
        conn, cur   = conn_obj.sqlite_connection(db_path)
        read_qry = """ SELECT period_type, period FROM document_meta_info; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        dist_phs = {}
        for row_data in t_data:
            period_type, period = row_data
            if not period_type or not period:continue
            ph = '{0}{1}'.format(period_type, period)
            dist_phs[ph]= 1
            
        dist_phs = rys.year_sort(dist_phs.keys())
        dist_phs.reverse()
        return dist_phs 
 
    def get_company_data(self, ijson):
        type_i = ijson['type']
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        dist_phs = self.read_doc_info_ph(company_id)
        dbinfo = config.Config.company_info_db
        cur, conn = conn_obj.MySQLdb_conn(dbinfo)
        data_lst = []
        col_def = [{'k':'key', 'n':'Description'}, {'k':'key', 'n':'Value'}]
        map_inf = {}
        if type_i == 'basic_info':
            data_lst, col_def, map_inf = self.read_company_info(conn, cur, company_id, dist_phs)     
        elif type_i == 'client_info':
            data_lst, col_def, map_inf = self.read_client_info(cur, company_id, project_id, dist_phs)
        elif type_i == 'sec_info':        
            data_lst, col_def, map_inf = self.read_sec_info(conn, cur, company_id, dist_phs)
        
        col_def.insert(0, {'k':'check', 'n': 'check', "v_opt":3, 'pin': 'pinnedLeft'})
        res = [{'message':'done', 'data':data_lst, 'col_def':col_def, 'map':map_inf, 'group':'Y'}]
        return res

if __name__ == '__main__':
    c_Obj = CompanyInfo()    
    ijson = {"type":"basic_info", "company_id":"1053724", "project_id":"5"}
    print c_Obj.get_company_data(ijson)

