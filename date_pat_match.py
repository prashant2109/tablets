import os
class date_pattern_match:
   def __init__(self):
      fp=open('/root/tablets/tablets_mapping/pysrc/EntityIdentify1.txt','r')
      lines=fp.readlines()
      self.all_row_cols={}
      for line in lines:
        all_cols=line.strip('\n').split('\t')         
        self.all_row_cols[all_cols[0]] = all_cols[1:]

      self.month_word_int = {}   
      fp=open('/root/tablets/tablets_mapping/pysrc/month_word_int.txt','r')
      lines=fp.readlines()
      for line in lines:
          all_cols=line.strip('\n').split('\t')         
          for c in all_cols[1:]: 
              self.month_word_int[c.lower()] = all_cols[0]

   def date_pattern_match(self,pat_id,input_str,f_tup_ls):
    
      patt_list = self.all_row_cols.get(pat_id, []) 
      date_ar = ["", "", ""] # DD MM YYYY

      #print patt_list
      #print f_tup_ls
      if len(f_tup_ls)==len(patt_list):
         for i in range(len(f_tup_ls)):
             mx = patt_list[i]
             if "Structure" in mx:continue
             if mx == 'day_num:day_num':
                e = input_str[f_tup_ls[i][0]:f_tup_ls[i][1]]
                if len(e)==1:
                   e = "0"+e
                date_ar[0] = e

             if mx == 'month_num:month_num':
                e = input_str[f_tup_ls[i][0]:f_tup_ls[i][1]]
                if len(e)==1:
                   e = "0"+e
                date_ar[1] = e

             if mx == 'month_word:month_word':
                e = input_str[f_tup_ls[i][0]:f_tup_ls[i][1]]
                m_int = self.month_word_int.get(e.lower(), "") 
                date_ar[1] = m_int

             if mx == 'year_num4:year_num4':
                e = input_str[f_tup_ls[i][0]:f_tup_ls[i][1]]
                date_ar[2] = e

             if mx == 'year_num2:year_num2':
                e = input_str[f_tup_ls[i][0]:f_tup_ls[i][1]]
                if len(e)==2:
                   if 90<=int(e)<=99:
                      e = '19'+e
                   elif 1<=int(e)<90:
                      e = '20'+e
                date_ar[2] = e
      date_ar.reverse() 
      s = ""
      for d in date_ar:
         if d:
            s += d + "-"

      return s.strip("-")
                   

if __name__=="__main__":
 obj=date_pattern_match()
 pat_id='date7'
 input_str='GNMA REMIC 2010-14 DI 4.5% DEC 2033'
 f_tup_ls=[(27, 29), (30, 33), (34, 38)]
 #print input_str[34:38]
 print obj.date_pattern_match(pat_id,input_str,f_tup_ls)
