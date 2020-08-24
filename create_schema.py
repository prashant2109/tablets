import os, sys, sqlite3

class Schema:
    def create_rawdb(self, cur):
        crt_stmt = """ CREATE TABLE IF NOT EXISTS rawdb(row_col_groupid TEXT, row INTEGER, col INTEGER, groupid INTEGER, docid INTEGER, cellph TEXT, cellcsvc TEXT, cellcsvs TEXT, cellcsvv TEXT, celltype TEXT, pghtext TEXT, pghxmlid TEXT, pghbbox TEXT, ghtext TEXT, ghxmlid TEXT, ghbbox TEXT, pvghtext TEXT, pvghxmlid TEXT, pvghbbox TEXT, hghtext TEXT, hghxmlid TEXT, hghbbox TEXT, vghtext TEXT, vghxmlid TEXT, vghbbox TEXT, gvtext TEXT, gvxmlid TEXT, gvbbox TEXT, numval TEXT, pg INTEGER, gridids TEXT, taxoid INTEGER, srctype TEXT, valuetype TEXT, datatype TEXT, tl_type TEXT, direction TEXT, res_opr TEXT, comp_type TEXT);  """
        cur.execute(crt_stmt)
        

    def create_equality_db(self, cur):
        crt_stmt  = """  CREATE TABLE IF NOT EXISTS equalityInfo(eid INTEGER, table_id1 TEXT, xml_ref1 TEXT, r_c1 TEXT, fid1 TEXT, table_id2 TEXT, xml_ref2 TEXT, r_c2 TEXT, fid2 TEXT, scale INTEGER, fType1 TEXT, fType2 TEXT, vType1 TEXT, vType2 TEXT, relationship_Type TEXT, equality_type TEXT, equality_id TEXT, equality_index TEXT);  """
        cur.execute(crt_stmt)
        return
    
    def create_formulainfo(self, cur):
        crt_stmt = """ CREATE TABLE IF NOT EXISTS formulainfo(fid TEXT, resultant_info TEXT, formula TEXT, opernands TEXT, groupid INTEGER, formula_type VARCHAR(50), formula_sign TEXT, formula_key TEXT);  """
        cur.execute(crt_stmt)
        return 

    def create_MostReferencedTable(self, cur):
        crt_stmt = """ CREATE TABLE IF NOT EXISTS  MostReferencedTable(table_id TEXT, table_score INTEGER); """
        cur.execute(crt_stmt)
        return
        
    def create_checksuminfo(self, cur):
        crt_stmt = """ CREATE TABLE IF NOT EXISTS checksuminfo(fid TEXT, checksum TEXT, operand_values TEXT, computed_value TEXT);  """         
        cur.execute(crt_stmt)
        return
        
    def create_rowinfo(self, cur):
        crt_stmt = """ CREATE TABLE IF NOT EXISTS rowinfo(row_groupid INTEGER, row INTEGER, groupid INTEGER, dynamiclabel TEXT, rowsignature TEXT, level TEXT);  """
        cur.execute(crt_stmt)
        return

    def create_colinfo(self, cur):
        crt_stmt = """ CREATE TABLE IF NOT EXISTS colinfo(col_groupid INTEGER, col INTEGER, groupid INTEGER, colsignature TEXT, reporres TEXT, user_tag TEXT, user_grpid TEXT);  """
        cur.execute(crt_stmt)
        return

    def create_pagedet(self, cur):
        crt_stmt = """  CREATE TABLE IF NOT EXISTS pagedet(pageno INTEGER, pagesize TEXT);  """
        cur.execute(crt_stmt)
        return
        

    def create_validateflgs(self, cur):
        crt_stmt = """ CREATE TABLE IF NOT EXISTS validateflgs(row_col TEXT, groupid INTEGER, row INTEGER, col INTEGER, errorclass TEXT, errormsg TEXT, errortype TEXT, validation_flag VARCHAR(20));  """
        cur.execute(crt_stmt)
        return
    
    def exe_func(self, cur):
        #db_path = '/root/prashant/s1.db' 
        #conn = sqlite3.connect(db_path)
        #cur = conn.cursor()
        self.create_rawdb(cur)
        self.create_equality_db(cur)
        self.create_MostReferencedTable(cur)
        self.create_formulainfo(cur)
        self.create_colinfo(cur)
        self.create_rowinfo(cur)
        self.create_pagedet(cur)
        self.create_checksuminfo(cur)
        self.create_validateflgs(cur)        
        #conn.close()
        return 

if __name__ == '__main__':
    s_Obj = Schema()
    s_Obj.exe_func()















