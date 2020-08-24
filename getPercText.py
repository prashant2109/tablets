import string
class getPercText:

      def getCouponFreq2(self, text):
          text = text.strip()
          if not text:
             return ''
          text_sp = text.split()
          for text_elm in text_sp:
              if text_elm in ['1/2']:
                 return '2'
              if text_elm in ['1/4']:
                 return '4'
          return ''

      def getMBSType(self, text):
          if 'freddie' in text.lower():
             return 'CONV15'
          if 'fannie' in text.lower():
             return 'CONV15'
          return ''


      def datePatternNormaliseTwo(self, col_ar):
          new_col_ar = []
          for col_elm in col_ar: 
              if '/':
                 col_elm_sp = col_elm.split('/') 
                 try:
                    new_col_ar.append((int(col_elm_sp[0]), int(col_elm_sp[1]), int(col_elm_sp[2])))
                 except:
                    new_col_ar.append(())
              elif '-':
                 col_elm_sp = col_elm.split('-')
                 try:
                    new_col_ar.append((int(col_elm_sp[0]), int(col_elm_sp[1]), int(col_elm_sp[2])))
                 except:
                    new_col_ar.append(())
              elif '.':
                 col_elm_sp = col_elm.split('.')
                 try:
                    new_col_ar.append((int(col_elm_sp[0]), int(col_elm_sp[1]), int(col_elm_sp[2])))
                 except:
                    new_col_ar.append(())
              else:
                 new_col_ar.append(())
           
          year_col = 2
          month_col = 0
          dd_col = 1 
          allow_flg = 1
          for new_col_tup in new_col_ar:
              if not new_col_tup:
                 continue
              if not (new_col_tup[month_col] <= 12):
                 allow_flg = 0
                 break
              if not (new_col_tup[dd_col] <= 31):
                 allow_flg = 0
                 break
          if allow_flg:
             new_new_col_ar = []
             for new_col_tup in new_col_ar:
                 print new_col_tup 
                 if not new_col_tup:
                    new_new_col_ar.append(('', 0)) 
                    continue
                 m = str(new_col_tup[month_col])
                 d = str(new_col_tup[dd_col])
                 y = str(new_col_tup[year_col])
                 if len(m)==1:
                    m = '0'+m  
                 if len(d)==1:
                    d = '0'+d  
                 if len(y)==1:
                    y = '200'+y  
                 elif len(y)==2:
                    y = '20'+y  
                 new_new_col_ar.append((y+'-'+m+'-'+d, 1)) 
             return new_new_col_ar 

          year_col = 0
          month_col = 2
          dd_col = 1 
          allow_flg = 1
          print new_col_ar
          for new_col_tup in new_col_ar:
              if not new_col_tup:
                 allow_flg = 0
                 break
              if not (new_col_tup[month_col] <= 12):
                 allow_flg = 0
                 break
              if not (new_col_tup[dd_col] <= 31):
                 allow_flg = 0
                 break

          if allow_flg:
             new_new_col_ar = []
             for new_col_tup in new_col_ar:
                 if not new_col_tup:
                    new_new_col_ar.append(('', 0)) 
                    continue
                 m = str(new_col_ar[month_col])
                 d = str(new_col_ar[dd_col])
                 y = str(new_col_ar[year_col])
                 if len(m)==1:
                    m = '0'+m  
                 if len(d)==1:
                    d = '0'+d  
                 if len(y)==1:
                    y = '200'+y  
                 elif len(y)==2:
                    y = '20'+y  
                 new_new_col_ar.append((y+'-'+m+'-'+d, 1)) 
             return new_new_col_ar 


           
          new_new_col_ar = []  
          for new_col_tup in new_col_ar:
              new_new_col_ar.append(('', 0))
          return new_new_col_ar
            
           

      def datePatternNormalise(self, col_ar):
          
          len_dict = {}
          allow_flg = 1
          for col_elm in col_ar:
              col_elm = col_elm.strip()  
              if '.' in col_elm:
                 allow_flg = 0
                 break
              if col_elm:
                 if (len(col_elm)!=8):
                    allow_flg = 0
                    break
                 else:
                    j1 = col_elm[0:4]
                    j2 = col_elm[4:6]
                    j3 =  col_elm[6:]
                    try:
                           m = int(j1) 
                           m = int(j2) 
                           m = int(j3)
                    except:
                           allow_flg = 0
                           break
                
          if allow_flg==1:
             new_col_ar = []
             for col_elm in col_ar:
                 col_elm = col_elm.strip()
                 if col_elm:
                    new_col_ar.append((col_elm[0:4], col_elm[4:6], col_elm[6:]))
                 else:
                    new_col_ar.append(())
             
              
             # first element should start with 19 or 20
             f_flg = 1
             for new_col_tup in new_col_ar:
                 y = new_col_tup[0]
                 if y[0:2] in ['19', '20']:
                    pass
                 else:
                    f_flg = 0
                    break
                           
             m_flg = 1
             for new_col_tup in new_col_ar:
                 m = new_col_tup[1]
                 if (int(m)>=1) and (int(m)<=12):
                    pass
                 else:
                    m_flg = 0
                    break

             d_flg = 1
             for new_col_tup in new_col_ar:
                 m = new_col_tup[2]
                 if (int(m)>=1) and (int(m)<=31):
                    pass
                 else:
                    d_flg = 0
                    break

                       
   
             m_flg2 = 1
             for new_col_tup in new_col_ar:
                 m = new_col_tup[2]
                 if (int(m)>=1) and (int(m)<=12):
                    pass
                 else:
                    m_flg2 = 0
                    break
             
             d_flg2 = 1
             for new_col_tup in new_col_ar:
                 m = new_col_tup[1]
                 if (int(m)>=1) and (int(m)<=31):
                    pass
                 else:
                    d_flg2 = 0
                    break
             
              

             if (f_flg and m_flg and d_flg):
                new_col_ar2 = []
                for new_col_tup in new_col_ar:
                    if new_col_tup:
                       new_col_ar2.append(new_col_tup[0]+'-'+new_col_tup[1]+'-'+new_col_tup[2])  
                    else:
                       new_col_ar2.append('')      
                return new_col_ar2, 1      
             elif (f_flg and m_flg2 and d_flg2):
                new_col_ar2 = []
                for new_col_tup in new_col_ar:
                    if new_col_tup:
                       new_col_ar2.append(new_col_tup[0]+'-'+new_col_tup[2]+'-'+new_col_tup[1])  
                    else:
                       new_col_ar2.append('')      
                return new_col_ar2, 1     
             return col_ar, 0  
          return col_ar, 0  
            

      def openTypeGroupSingle(self, col_ar):
          # - to be used  
          ddict = {}
          ddict2 = {}
          ind = 1 
          for text_word in col_ar[1:]: 
              if not text_word.strip(): 
                  ind = ind + 1
                  continue
              ar = self.gettypeSingle(text_word)
              patt_ar = []
              for ar_tup in ar:
                  patt_ar.append(ar_tup[1])
              ddict[ind] = (ar, patt_ar)
              kkey = '_'.join(patt_ar)
              dx = ddict2.get(kkey, [])
              if not dx:
                 ddict2[kkey] = []
              ddict2[kkey].append((ar, ind))
              ind = ind + 1
          return ddict2     

      def gettypeSingle(self, text_word):
        
          
          word_elm = ''
          mystr = ''
          ar = []
          for text_elm in text_word:

              if text_elm in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                 if mystr:
                    if mystr[-1] != 'C':
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += 'C'
                    else:
                       word_elm += text_elm  
                 else:
                    word_elm += text_elm  
                    mystr += 'C' 
              elif text_elm in 'abcdefghijklmnopqrstuvwxyz':
                 if mystr:
                    if mystr[-1] != 'c':
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += 'c' 
                    else:
                       word_elm += text_elm  
                 else:
                    mystr += 'c'
                    word_elm += text_elm  
              elif text_elm in '0123456789':
                 if mystr:
                    if mystr[-1] != 'n':
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += 'n' 
                    else:
                       word_elm += text_elm  
                 else:
                    mystr += 'n'
                    word_elm += text_elm 
              elif text_elm in ['+']:
                 if mystr:
                    if mystr[-1] != 's':
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += 's' 
                    else:
                       word_elm += text_elm  
                 else:
                    mystr += 's'
                    word_elm += text_elm

              elif text_elm in ['-']:
                 if mystr:
                    if mystr[-1] != 'm':
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += 'm' 
                    else:
                       word_elm += text_elm  
                 else:
                    mystr += 'm'
                    word_elm += text_elm

              elif text_elm in ['%']:
                 if mystr:
                    if 1:
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += 'P' 
                 else:
                    mystr += 'P'
                    word_elm += text_elm

              elif text_elm in ['.']:
                 if mystr:
                    if 1:
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += '.' 
                 else:
                    mystr += '.'
                    word_elm += text_elm
              
              elif text_elm in ['/']:
                 if mystr:
                    if 1:
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += 'S' 
                 else:
                    mystr += 'S'
                    word_elm += text_elm
              elif text_elm in [',']:
                 if mystr:
                    if 1:
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += ',' 
                 else:
                    mystr += ','
                    word_elm += text_elm
              elif text_elm in ['(']:
                 if mystr:
                    if 1:
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += '(' 
                 else:
                    mystr += '('
                    word_elm += text_elm
              elif text_elm in [')']:
                 if mystr:
                    if 1:
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += ')' 
                 else:
                    mystr += ')'
                    word_elm += text_elm
              elif text_elm in ['?']:
                 if mystr:
                    if 1:
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += '?' 
                 else:
                    mystr += '?'
                    word_elm += text_elm
              else:
                 if mystr: 
                    if mystr[-1] != 'g':
                       ar.append((word_elm, mystr[-1]))
                       word_elm = text_elm 
                       mystr += 'g' 
                    else:
                       word_elm += text_elm  
                 else:
                    mystr += 'g'
                    word_elm += text_elm  
          if word_elm:
             ar.append((word_elm, mystr[-1])) 
          return ar


      def gettype(self, text_word):
          
          mystr = ''
          mywrd = []
          for text_elm in text_word:

              if text_elm in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                 if mystr:
                    if mystr[-1] != 'C':
                       mystr += 'C' 
                       mywrd.append(text_elm)
                    else:
                       mywrd[-1] += text_elm
                        
                 else:
                    mystr += 'C' 
                    mywrd.append(text_elm)
              elif text_elm in 'abcdefghijklmnopqrstuvwxyz':
                 if mystr:
                    if mystr[-1] != 'c':
                       mystr += 'c'
                       mywrd.append(text_elm)
                    else:
                       mywrd[-1] += text_elm    
                 else:
                    mystr += 'c'
                    mywrd.append(text_elm)
              elif text_elm in '0123456789':
                 if mystr:
                    if mystr[-1] != 'n':
                       mystr += 'n'
                       mywrd.append(text_elm)
                    else:
                       mywrd[-1] += text_elm    
                 else:
                    mystr += 'n'
                    mywrd.append(text_elm)
              elif text_elm in ['+']:
                 if mystr:
                    if mystr[-1] != 's':
                       mystr += 's' 
                       mywrd.append(text_elm)
                    else:
                       mywrd[-1] += text_elm  
                 else:
                    mystr += 's'
                    mywrd.append(text_elm)
              elif text_elm in ['=']:
                 if mystr:
                    if mystr[-1] != 'E':
                       mystr += 'E' 
                       mywrd.append(text_elm)
                    else:
                       mywrd[-1] += text_elm  
                 else:
                    mystr += 'E'
                    mywrd.append(text_elm)


              elif text_elm in ['-']:
                 if mystr:
                    if mystr[-1] != 'm':
                       mystr += 'm' 
                       mywrd.append(text_elm)
                    else:
                       mywrd[-1] += text_elm  
                 else:
                    mystr += 'm'
                    mywrd.append(text_elm)
              

              elif text_elm in ['.', ':', ',', '?', '!', "'", '"', '(', ')', '&', ';', '[', ']', '*', '#']:
                 if mystr:
                    if 1:
                       mystr += text_elm  
                       mywrd.append(text_elm)
                 else:
                    mystr += text_elm 
                    mywrd.append(text_elm)
              elif text_elm in ['%']:
                 if mystr:
                    if 1:
                       mystr += 'P' 
                       mywrd.append(text_elm)
                 else:
                    mystr += 'P' 
                    mywrd.append(text_elm)
              elif text_elm in ['/']:
                 if mystr:
                    if 1:
                       mystr += 'S'  
                       mywrd.append(text_elm)
                 else:
                    mystr += 'S' 
                    mywrd.append(text_elm)

              elif text_elm not in string.ascii_letters:
                 if mystr:
                    if mystr[-1] != 'c':
                       mystr += 'c'
                       mywrd.append(text_elm)
                    else:
                       mywrd[-1] += text_elm    
                 else:
                    mystr += 'c'
                    mywrd.append(text_elm)                
              else:
                 if mystr: 
                    if mystr[-1] != 'g':
                       mystr += 'g' 
                       mywrd.append(text_elm)
                    else:
                       mywrd[-1] += text_elm 
                 else:
                    mystr += 'g'
                    mywrd.append(text_elm)
         
          id_ar = []
          prev_ind = 0 
          for mywrd_elm in mywrd:
              id_ar.append((prev_ind, len(mywrd_elm)+prev_ind))
              prev_ind = prev_ind + len(mywrd_elm)      
          return mystr, mywrd, id_ar        
              

      def openTypeGroup(self, col_ar):
          # - to be used - input col_ar
          len_dict = {}
          ind = 0
          for col_elm in col_ar:
              #if not col_elm.strip():
              #   print "---------", col_elm
              #   ind = ind + 1
              #   continue
              col_sp = col_elm.split()
              ar = []
              data_dict_ar = []
              if col_elm.strip()=='':
                  data_dict_ar.append({'NA':col_elm})
                  ar.append('NA')
              for col_sp_elm in col_sp:
                  type_col_elm = self.gettype(col_sp_elm)
                  ar.append(type_col_elm)
                  data_dict_ar.append({type_col_elm:col_sp_elm})
              ar_tup = tuple(ar)
              lx = len_dict.get(ar_tup, [])
              if not lx:
                 len_dict[ar_tup] = []
              len_dict[ar_tup].append((data_dict_ar[:], ind))  

              ind = ind + 1   

          len_key_tup_ar = len_dict.keys()

          cons_dict = {}
          for len_key_tup_elm in len_key_tup_ar: 
              ar = list(len_key_tup_elm)
              ar.sort()
              len_ar = len(ar)
              if not cons_dict:
                 cons_dict[len_key_tup_elm] = [len_key_tup_elm]
              else:
                 flg = 0 
                 append_k = ()
                 for k, vs in cons_dict.items():
                     d = len(list(k))
                     if (d == len_ar):
                        gflg = 0 
                        for v in vs:
                            ar1 = list(v)
                            ar1.sort()
                            mat_flg = 0
                            for i in range(0, d):
                                if (ar[i] != ar1[i]):
                                   mat_flg = 1
                                   break

                            if mat_flg==0:
                               append_k = k 
                               gflg = 1
                               break
                        if (gflg == 1):
                           # - match foung
                           flg = 1
                           break
                           
                 if flg==0:
                    cons_dict[len_key_tup_elm] = [len_key_tup_elm]
                 else:
                    cons_dict[append_k].append(len_key_tup_elm)
               
          cons_data_dict = {}     
          for k in cons_dict.keys():
              vs = cons_dict[k]
              cons_data_dict[k] = []
              for v in vs:
                  cons_data_dict[k] += len_dict[v]
          return len_dict, cons_data_dict         

      def col_underscore_joined(self, col_ar):
          # - to be used - input text
          # this needs to be used
        
          ddict = {}
          ind = 0
          for col_elm in col_ar:
              if col_elm.strip():
                 ddict[ind] = self.underscore_joined(col_elm)
              else:
                 ddict[ind] = [] 
              ind = ind + 1
          return ddict     

      def underscore_joined(self, text):
          if len(text.split())==1:
             text_sp = text.split('_')
             return text_sp 
          return []   


      def getWithInBracket(self, col_ar):
          # - to be used - input col_ar 
          bracket_dict = {} 
          ind = 0
          for col_elm in col_ar:
              col_elm_sp = col_elm.split('(')
              elms = []
              for col_elm_elm in col_elm_sp[1:]: 
                  mystr = ''
                  for col_elm_elm_elm in col_elm_elm:
                      if (col_elm_elm_elm == ')'):
                          break
                      mystr += col_elm_elm_elm 
                  if mystr.strip():
                     elms.append(mystr.strip())
              bracket_dict[ind] = elms[:]       
              ind = ind + 1
          return bracket_dict      
                     

      def getRegion(self, col_ar):
          # - to be used - input col_ar 

          f = open('region.txt', 'r')
          flines = f.readlines()
          f.close()

          mydict = {}
          for fline in flines[1:]:
              fline = fline.strip('\n')
              fline_sp = fline.split('\t')
              for fline_elm in fline_sp[1:]:
                  if fline_elm.strip():
                     mydict[fline_elm.lower()] = fline_sp[0].strip()

          cur_ar = mydict.keys() 
          cur_dict = {}       
          ind = 0       
          for col_elm in col_ar:
              if not col_elm.strip():
                 ind = ind + 1
                 continue
              col_elm_sp = col_elm.split()
              ar = []
              for col_elm_elm in col_elm_sp:
                  col_elm_elm_sp = col_elm_elm.split('/')
                  for col_elm_elm_elm in col_elm_elm_sp:
                      if col_elm_elm_elm.strip():
                         ar.append(col_elm_elm_elm)

              new_ar = []
              for ar_elm in ar:
                  ar_elm_sp = ar_elm.split('_')
                  for ar_elm_elm in ar_elm_sp:
                      if (ar_elm_elm.strip()):
                         new_ar.append(ar_elm_elm) 
                         
              ar = new_ar[:]    
              t_ar = []           
              for cur_elm in cur_ar:
                  for ar_elm in ar:
                     if not ar_elm.strip(): continue 
                     if (cur_elm == ar_elm.lower()):
                        t_ar.append(mydict[cur_elm])
                        break
                     else:
                        ar_elm_l = ar_elm.lower() 
                        t_sp = ar_elm_l.split(cur_elm) 
                        if not (t_sp[0].strip()):
                           t_ar.append(mydict[cur_elm])
                           break
              cur_dict[ind] = t_ar[:]       
              ind = ind + 1
          return cur_dict 

      def getCountry(self, col_ar):
          # - to be used - input col_ar 

          f = open('country.txt', 'r')
          flines = f.readlines()
          f.close()

          mydict = {}
          for fline in flines[1:]:
              fline = fline.strip('\n')
              fline_sp = fline.split('\t')
              for fline_elm in fline_sp[1:]:
                  if fline_elm.strip():
                     mydict[fline_elm.lower()] = fline_sp[0].strip()

          cur_ar = mydict.keys() 
          cur_dict = {}       
          ind = 0       
          for col_elm in col_ar:
              if not col_elm.strip():
                 ind = ind + 1
                 continue
              col_elm_sp = col_elm.split()
              ar = []
              for col_elm_elm in col_elm_sp:
                  col_elm_elm_sp = col_elm_elm.split('/')
                  for col_elm_elm_elm in col_elm_elm_sp:
                      if col_elm_elm_elm.strip():
                         ar.append(col_elm_elm_elm)

              new_ar = []
              for ar_elm in ar:
                  ar_elm_sp = ar_elm.split('_')
                  for ar_elm_elm in ar_elm_sp:
                      if (ar_elm_elm.strip()):
                         new_ar.append(ar_elm_elm) 
                         
              ar = new_ar[:]    
              t_ar = []           
              for cur_elm in cur_ar:
                  for ar_elm in ar:
                     if not ar_elm.strip(): continue 
                     if (cur_elm == ar_elm.lower()):
                        t_ar.append(mydict[cur_elm])
                        break
                     else:
                        ar_elm_l = ar_elm.lower() 
                        t_sp = ar_elm_l.split(cur_elm) 
                        if not (t_sp[0].strip()):
                           t_ar.append(mydict[cur_elm])
                           break
              cur_dict[ind] = t_ar[:]       
              ind = ind + 1
          return cur_dict 



      def getCurrency(self, col_ar):
          # - to be used - input col_ar 
          f = open('currency.txt', 'r')
          flines = f.readlines()
          f.close()
          cur_ar_dict = {}
          for fline in flines[1:]:
              if fline.strip():
                 cur_ar_dict[fline.strip().lower()] = 1
          cur_ar = cur_ar_dict.keys()       
                 
          cur_dict = {}       
          ind = 0       
          for col_elm in col_ar:
              if not col_elm.strip():
                 ind = ind + 1
                 continue
              col_elm_sp = col_elm.split()
              ar = []
              for col_elm_elm in col_elm_sp:
                  col_elm_elm_sp = col_elm_elm.split('/')
                  for col_elm_elm_elm in col_elm_elm_sp:
                      if col_elm_elm_elm.strip():
                         ar.append(col_elm_elm_elm)

              new_ar = []
              for ar_elm in ar:
                  ar_elm_sp = ar_elm.split('_')
                  for ar_elm_elm in ar_elm_sp:
                      if (ar_elm_elm.strip()):
                         new_ar.append(ar_elm_elm) 
                         
              ar = new_ar[:]    
              t_ar = []           
              for cur_elm in cur_ar:
                  for ar_elm in ar:
                     if not ar_elm.strip(): continue 
                     if (cur_elm == ar_elm.lower()):
                        t_ar.append(ar_elm)
                        break
                     else:
                        ar_elm_l = ar_elm.lower() 
                        t_sp = ar_elm_l.split(cur_elm) 
                        if not (t_sp[0].strip()):
                           t_ar.append(ar_elm)
                           break
              cur_dict[ind] = t_ar[:]       
              ind = ind + 1
          return cur_dict 

      
      def getIRSSpotEntityPropSpace(self, col_ar):
          row_dict = {}
          ind = 0
          for col_elm in col_ar:
              row_dict[str(ind)] = []
              col_elm = col_elm.strip()
              if col_elm:
                 col_elm = col_elm.replace('_', ' ') 
                 col_elm = col_elm.replace('-', ' ') 
                 col_elm = ' '.join(col_elm.split()) 
                 col_elm_sp = col_elm.split()
                 if ' irs ' in ' '+col_elm.lower()+' ':
                        row_dict[str(ind)].append('Interest rate swaps')
              ind = ind + 1         
          return row_dict 


 
      def getTRSSpotEntityPropSpace(self, col_ar):
          row_dict = {}
          ind = 0
          for col_elm in col_ar:
              row_dict[str(ind)] = []
              col_elm = col_elm.strip()
              if col_elm:
                 col_elm = col_elm.replace('_', ' ') 
                 col_elm = col_elm.replace('-', ' ') 
                 col_elm = ' '.join(col_elm.split()) 
                 col_elm_sp = col_elm.split()
                 if ' trs ' in ' '+col_elm.lower()+' ':
                        row_dict[str(ind)].append('total return swaps')
              ind = ind + 1         
          return row_dict 


 

      def getFxSpotEntityPropSpace(self, col_ar):
          row_dict = {}
          ind = 0
          for col_elm in col_ar:
              row_dict[str(ind)] = []
              col_elm = col_elm.strip()
              if col_elm:
                 col_elm = col_elm.replace('_', ' ') 
                 col_elm = col_elm.replace('-', ' ') 
                 col_elm = ' '.join(col_elm.split()) 
                 col_elm_sp = col_elm.split()
                 if ' fx spot ' in ' '+col_elm.lower()+' ':
                        row_dict[str(ind)].append('Currency Options')
              ind = ind + 1         
          return row_dict 


          


      def getFuturesEntityPropSpace(self, col_ar):
          # - to be used - input col_ar 
          row_dict = {}
          ind = 0
          for col_elm in col_ar:
              row_dict[str(ind)] = []
              col_elm = col_elm.strip()
              if col_elm:
                 col_elm = ' '.join(col_elm.split()) 
                 col_elm_sp = col_elm.split()
                 got_it = 0
                 for col_elm_elm in col_elm_sp:
                     if col_elm_elm.lower() in ['future', 'futures']:
                        row_dict[str(ind)].append('FUTURE')
                        got_it = 1
                        break
                 if (got_it == 0):
                    if ' alsi fut ' in ' '+col_elm.lower()+' ':
                        row_dict[str(ind)].append('FUTURE')
                  
              ind = ind + 1         
          return row_dict 


          

      def getOptionEntityPropNonSpace(self, col_ar):
          # - to be used - input col_ar 
          row_dict = {}  
          ind = 0
          for col_elm in col_ar:
              row_dict[ind] = []
              col_elm = col_elm.strip()
              if col_elm:
                 #col_elm_sp = col_elm.split()
                 col_elm_l = col_elm.lower()
                 if ('put' in col_elm.lower()):
                    col_elm_l_sp = col_elm_l.split('put')    
                    if len(col_elm_l_sp)==2:             
                       if not (col_elm_l_sp[1].strip()):  
                          row_dict[ind].append('PUT')
                       else:
                          elm = col_elm_l_sp[1][0].strip()  
                          if elm.lower() not in 'abcdefghijklmnopqrstuvwxyz':
                             row_dict[ind].append('PUT')
                 elif ('call' in col_elm.lower()): 
                    col_elm_l_sp = col_elm_l.split('call')    
                    if len(col_elm_l_sp)==2:               
                       if not (col_elm_l_sp[1].strip()):  
                          row_dict[ind].append('CALL')
                       else:
                          elm = col_elm_l_sp[1][0].strip()  
                          if elm.lower() not in 'abcdefghijklmnopqrstuvwxyz':
                             row_dict[ind].append('CALL')
                 elif ('cal' in col_elm.lower()):
                    col_elm_l_sp = col_elm_l.split('cal')    
                    if len(col_elm_l_sp)==2:               
                       if not (col_elm_l_sp[1].strip()):  
                          elm = col_elm_l_sp[0][-1].strip()  
                          if elm.lower() not in 'abcdefghijklmnopqrstuvwxyz':
                             row_dict[ind].append('CALL')
                       else:
                          elm = col_elm_l_sp[1][0].strip()  
                          if elm.lower() not in 'abcdefghijklmnopqrstuvwxyz':
                             row_dict[ind].append('CALL')
              ind = ind + 1
          return row_dict     


      def getOptionEntityPropSpace(self, col_ar):
          # - to be used - input col_ar 
          row_dict = {}
          ind = 0
          for col_elm in col_ar:
              row_dict[ind] = []
              col_elm = col_elm.strip()
              if col_elm:
                 col_elm_sp = col_elm.split()
                 for col_elm_elm in col_elm_sp:
                     if col_elm_elm.lower() in ['put']:
                        row_dict[ind].append('PUT')
                     if col_elm_elm.lower() in ['call', 'cal']:
                        row_dict[ind].append('CALL')
              ind = ind + 1         
          return row_dict 


      def removeRedundantCols(self, col_ar_ar):
          # - to be used - input ar of col_ar 
          
          col_dict = {}
          ind = 0
          for ar in col_ar_ar:
              col_dict[ind] = ar[:]
              ind = ind + 1 
          
          considered_dict = {}
          col_keys = col_dict.keys()
          col_keys.sort()
          for i in range(0, len(col_keys)):
                    for j in range(0, len(col_keys)):
                        if (i < j): 
                           ar1 = col_keys[i]
                           ar2 = col_keys[i+1]
                           flg1, flg2 = self.compareTwoCols(ar1, ar2)
                           if ((flg1 == 1) or (flg2 == 1)):
                              considered_dict[(i, j)] = (flg1, flg2)

          del_ar = {}                
          ckeys = considered_dict.keys()                    
          for ind_tup in ckeys[:]:
              val_tup = considered_dict[ind_tup]
              if (val_tup[0] == 0) and (val_tup[1] == 0):
                 del_ar[ind_tup[0]] = 1
                 
                 
          for ind_tup in ckeys[:]:
              val_tup = considered_dict[ind_tup]
              if (val_tup[0] == 0) and (val_tup[1] == 1):
                 #remove - 1
                 #2 - should not be in del_ar  
                 dx = del_ar.get(ind_tup[1], [])
                 if not dx:
                    del_ar[ind_tup[0]] = 1
                     
              elif (val_tup[0] == 1) and (val_tup[1] == 0):
                 #remove - 2
                 #1 - should not be in del_ar  
                 dx = del_ar.get(ind_tup[0], [])
                 if not dx:
                    del_ar[ind_tup[1]] = 1
                    
          col_keys = del_ar.keys()
          for col_key in col_keys:
              del col_dict[col_key]
          keys = col_dict.keys()
          keys.sort()
          dat_ar = []
          for k in keys:
              dat_ar.append(col_dict[k][:])
          return dat_ar     


      def compareTwoCols(self, col1_ar, col2_ar):
          d = len(col1_ar)
          flg1 = 0
          flg2 = 0
          for i in range(0, d):
              e1 = col1_ar[i].strip()
              e2 = col2_ar[i].strip()
              e1 = ' '.join(e1.split())
              e2 = ' '.join(e2.split())
              e1_l = e1.lower()
              e2_l = e2.lower()
              if (e1.strip()) and (flg1 == 0):
                  if (e1_l == e2_l):
                     pass
                  else:
                     flg1 = 1 

              if (e2.strip()) and (flg2 == 0):
                  if (e1_l == e2_l):
                     pass
                  else:
                     flg2 = 1 
          return flg1, flg2 

                        


      def isTextCol(self, text_col_ar):
          # - to be used - input col_ar 
          for text_col_elm in text_col_ar:
              text_col_elm = text_col_elm.strip()
              if len(text_col_elm.split())>1:
                 
                 return 1
          return 0       

      def freqTextTextMoreThanOne(self, text_col_ar):
          # - to be used - input col_ar 
          mydict = {}
          mydict_col = {}
          ind = 0
          for text_col_elm in text_col_ar: 
              text_col_elm = text_col_elm.strip()
              if text_col_elm:
                 text_col_elm_sp = text_col_elm.split()
                 d = len(text_col_elm_sp)
                 for i in range(1, d+1):
                     j = 0 
                     while (j+i <= d):
                           al_t_flg = 0
                           for t in text_col_elm_sp[j:j+i]:
                               if (t[0].lower() in 'abcdefghijklmnopqrstuvwxyz'):
                                  pass  
                               else:
                                  al_t_flg = 1
                                  break
                           if (al_t_flg == 0):       
                               mystr = ' '.join(text_col_elm_sp[j:j+i])
                               mystr_l = mystr.lower() 
                               mx = mydict.get(mystr_l, 0)
                               if not mx:
                                  mydict[mystr_l] = 0
                                  mydict_col[str(mystr_l)] = []
                               mydict_col[str(mystr_l)].append((text_col_elm, ind)) 
                               mydict[mystr_l] += 1

                           j = j + 1
              ind = ind + 1             
          ar = []                 
          for m, vs in mydict.items():
              if (vs > 1):
                 ar.append(m)
              else:
                 del mydict_col[str(m)] 
          return mydict_col 

      def freqWordTextMoreThanOne(self, text_col_ar):
          # - to be used - input col_ar 
          mydict = {}
          mydict_col = {}
          ind = 0
          for text_col_elm in text_col_ar: 
              text_col_elm = text_col_elm.strip()
              if text_col_elm:
                 text_col_elm_sp = text_col_elm.split()
                 for text_col_elm_elm in text_col_elm_sp:
                     if (len(text_col_elm_elm)>1):
                        if (text_col_elm_elm[0].lower() in 'abcdefghijklmnopqrstuvwxyz'):
                           text_col_elm_elm_l = text_col_elm_elm.lower() 
                           mx = mydict.get(text_col_elm_elm_l, 0)
                           if not mx:
                              mydict[text_col_elm_elm_l] = 0
                              mydict_col[text_col_elm_elm_l] = []
                           mydict_col[text_col_elm_elm_l].append((text_col_elm_elm, ind)) 
                           mydict[text_col_elm_elm_l] += 1
                          
              ind = ind + 1             
          ar = []                 
          for m, vs in mydict.items():
              if (vs > 1):
                 ar.append(m)
              else:
                 del mydict_col[m] 
          return mydict_col 


      def getNumAlphaNonSpace(self, text_elm):
          # - to be used - input text 
          data_ar = []
          #text_sp = text.split()
          #for text_elm in text:
          ar = ['', '']
          for e in text_elm:
                  if e in '0123456789.':
                     ar[0] += e
                     ar[1] = ''
                  elif e in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                     ar[1] += e 
                  else: 
                     if (ar[0].strip() and ar[1].strip()):
                        data_ar.append(ar[:])
                     ar = ['', ''] 
          if (ar[0].strip() and ar[1].strip()):
                 data_ar.append(ar)
          return data_ar        

       
      def getNumAlphaSpace(self, text):
          # - to be used - input text 
          data_ar = []
          text_sp = text.split()
          for text_elm in text_sp:
              ar = ['', '']
              for e in text_elm:
                  if e in '0123456789.':
                     ar[0] += e 
                     ar[1] = ''
                  elif e in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                     ar[1] += e 
                  else: 
                     if (ar[0].strip() and ar[1].strip()):
                        data_ar.append(ar[:])
                     ar = ['', ''] 
              if (ar[0].strip() and ar[1].strip()):
                 data_ar.append(ar)
          return data_ar        

      def getTkrTkrValue(self, col_ar):
          # - to be used - input col_ar


          f = open('currency.txt', 'r')
          flines = f.readlines()
          f.close()
          cur_ar = []
          for fline in flines[1:]:
              if fline.strip():
                 if fline.strip() not in cur_ar: 
                    cur_ar.append(fline.strip())
          #for text_elm in text_sp:
          ar = {}
          if 1:    
              i = 0
              for cur_elm1 in cur_ar[:]:
                  j = 0
                  for cur_elm2 in cur_ar[:]:
                      if (i != j):
                         ar[cur_elm1+cur_elm2] =  (cur_elm1, cur_elm2)       
                         ar[cur_elm2+cur_elm1] =  (cur_elm1, cur_elm2)
                      j = j + 1
                  i = i + 1    
           
          ddict = {}
          ind = 0
          for text in col_ar:
              if text.strip():
                 elms = []
                 text = text.strip()
                 text_sp = text.split()
                 for text_elm in text_sp:
                     ax = ar.get(text_elm, ())
                     if ax:
                        elms.append(ax)
                 ddict[ind] = elms[:] 
              else:
                 ddict[ind] = [] 
              ind = ind + 1
          return ddict     


      def getCurrValue(self, text):
          # - to be used - input text 


          f = open('currency.txt', 'r')
          flines = f.readlines()
          f.close()
          cur_ar = []
          for fline in flines[1:]:
              if fline.strip():
                 if fline.strip() not in cur_ar: 
                    cur_ar.append(fline.strip())

          elms = cur_ar[:]
          text = text.strip()
          freq_ar = []
          # only before
          for elm in elms: 
              text_sp = text.split(elm)
              #ind = 0
              if len(text_sp)>1:
                 for text_elm in text_sp[1:]:
                     text_elm_rev = text_elm
                     #text_elm_rev.reverse() 
                     #print text_elm_rev 
                     if text_elm_rev.strip():
                        i = 0
                        num_elm_ar = []
                        while (i<len(text_elm_rev.strip())):
                            d = text_elm_rev[i]
                            if d in '0123456789.':
                               if (d == '.'):
                                  if (len(num_elm_ar)==1):
                                     if num_elm_ar[0]=='.':
                                        num_elm_ar = [] 
                                  else:
                                     if num_elm_ar and ('.' in num_elm_ar):
                                        num_elm = ''.join(num_elm_ar)
                                        freq_ar.append((elm, float(num_elm)))
                                        num_elm_ar = []
                               num_elm_ar.append(d)
                            elif not d.strip():
                               if (len(num_elm_ar)==1) and (num_elm_ar[0] == '.'):
                                  num_elm_ar = []
                               else:
                                  if num_elm_ar: 
                                     num_elm = ''.join(num_elm_ar)
                                     freq_ar.append((elm, float(num_elm)))
                                  num_elm_ar = []
                               break   
                            i = i + 1 
                        #print num_elm_ar     
                        if (len(num_elm_ar)==1) and (num_elm_ar[0] == '.'):
                           num_elm_ar = []
                        elif num_elm_ar:   
                           num_elm = ''.join(num_elm_ar)
                           freq_ar.append((elm, float(num_elm)))
                           num_elm_ar = []
          #print freq_ar 
          return freq_ar               

      def getPointsValue(self, text):
          # - to be used - input text 
          text = text.strip()

          freq_ar = []
          # only before
          elms = ['p']
          for elm in elms: 
              text_sp = text.split(elm)
              #ind = 0
              if len(text_sp)>1:
                 for text_elm in text_sp[:-1]:
                     text_elm_rev = text_elm
                     #text_elm_rev.reverse() 
                     #print text_elm_rev 
                     if text_elm_rev.strip():
                        i = len(text_elm_rev)-1
                        num_elm_ar = []
                        while i>=0:
                            d = text_elm_rev[i]
                            if d in '0123456789.':
                               if (d == '.'):
                                  if (len(num_elm_ar)==1):
                                     if num_elm_ar[0]=='.':
                                        num_elm_ar = [] 
                                  else:
                                     if num_elm_ar and ('.' in num_elm_ar):
                                        num_elm_ar.reverse()
                                        num_elm = ''.join(num_elm_ar)
                                        freq_ar.append(float(num_elm))
                                        num_elm_ar = []
                               num_elm_ar.append(d)
                            elif not d.strip():
                               if (len(num_elm_ar)==1) and (num_elm_ar[0] == '.'):
                                  num_elm_ar = []
                               else:
                                  if num_elm_ar: 
                                     num_elm_ar.reverse()
                                     num_elm = ''.join(num_elm_ar)
                                     freq_ar.append(float(num_elm))
                                  num_elm_ar = []
                               break   
                            i = i - 1
                        #print num_elm_ar     
                        if (len(num_elm_ar)==1) and (num_elm_ar[0] == '.'):
                           num_elm_ar = []
                        elif num_elm_ar:   
                           num_elm_ar.reverse()
                           num_elm = ''.join(num_elm_ar)
                           freq_ar.append(float(num_elm))
                           num_elm_ar = []
          #print freq_ar 
          return freq_ar               


      def getNumberDecimal(self, text):
          if not text.strip(): 
             return [], []
          text_sp = text.split()
          num_ar = []
          text_ar = []
          ind = 0
          for text_elm in text_sp:
              #if '.' in text_elm:
              try:
                d = float(text_elm)
                flg = 1
              except:
                flg = 0
              if (flg == 0):
                 num_ar.append(text_elm)
                 text_ar.append(' '.join(text_sp[0:ind]))
              ind = ind + 1   
          return num_ar, text_ar     


      def getCANDOProps(self, col_ar):
          # - to be used  
          ddict2 = {}
          ind = 1 
          for text_word in col_ar[1:]: 
              ddict2[ind] = []
              if not text_word.strip(): 
                  ind = ind + 1
                  continue
              ar = self.getCANDOprops_text(text_word)
              ddict2[ind] = ar
              ind = ind + 1
          return ddict2     
      
      def getFTSEJSEProps(self, col_ar):
          # - to be used  
          ddict2 = {}
          ind = 1 
          for text_word in col_ar[1:]: 
              ddict2[ind] = []
              if not text_word.strip(): 
                  ind = ind + 1
                  continue
              ar = self.getFTSEJSEprops_text(text_word)
              ddict2[ind] = ar
              ind = ind + 1
          return ddict2     
 
      def getCANDOprops_text(self, text):
          if not text.strip():
             return ''
          text = text.replace('(', ' ( ')
          text = text.replace(')', ' ) ')
          text = text.strip()
          text = ' '.join(text.split())
          text = text.lower() 
          if 'can-do' in text:
             return 'CAN DO'
          elif 'can do' in text:
             return 'CAN DO'
          return '' 


      def getLEGprop(self, text):
          if not text.strip(): return ''
          text_sp = text.split() 
          for text_elm in text_sp:
              if 'leg' == text_elm.lower():
                 return 'LEG'
          return ''   

      def getBPSprop(self, text):
          text = text.strip()
          if not text.strip(): return ''
          text_sp = text.split()  
          for text_elm in text_sp:
              if len(text_elm) > 3:
                 if text_elm[-3:].lower() == 'bps':
                    if text_elm[-4] in '0123456789':
                       m_str = text_elm[:-3]
                       #m_str.reverse()
                       m_str = m_str[::-1]
                       elm = ''
                       for i in range(0, len(m_str)):
                           if (m_str[i] in '0123456789.'):
                              elm += m_str[i]
                           else:
                              break  
                       if elm not in ['.']: 
                          elm = elm[::-1] 
                          if elm:
                             return elm  
              elif len(text_elm) > 2:
                 if text_elm[-2:].lower() == 'bp':
                    if text_elm[-3] in '0123456789':
                       m_str = text_elm[:-2]
                       #m_str.reverse()
                       m_str = m_str[::-1]
                       elm = ''
                       for i in range(0, len(m_str)):
                           if (m_str[i] in '0123456789.'):
                              elm += m_str[i]
                           else: 
                              break 
                       if elm not in ['.']:
                          elm = elm[::-1] 
                          if elm:  
                             return elm
          return ''
            
                               
                        
                 
 
      def getFTSEJSEprops_text(self, text):
          if not text.strip():
             return ''
          text = text.strip()
          text = ' '.join(text.split())   
          text = text.lower()
          if 'alsi40' in text:
              return 'ALSI40'
          elif 'alsi top40' in text:
              return 'ALSI40'
          elif 'alsi' in text:
              return 'ALSI'
          if 'dtop' in text:
              return 'DTOP'   
          return ''
        
      #def getFTSEJSEprops(self, col_ar):

      def getRateValue(self, text):
          # - to be used - input text 
          text = text.strip()

          freq_ar = []
          # only before
          elms = ['%']
          for elm in elms: 
              text_sp = text.split(elm)
              #ind = 0
              if len(text_sp)>1:
                 for text_elm in text_sp[:-1]:
                     text_elm_rev = text_elm
                     #text_elm_rev.reverse() 
                     #print text_elm_rev 
                     if text_elm_rev.strip():
                        i = len(text_elm_rev)-1
                        num_elm_ar = []
                        while i>=0:
                            d = text_elm_rev[i]
                            if d in '0123456789.,':
                               if (d == '.'):
                                  if (len(num_elm_ar)==1):
                                     if num_elm_ar[0] in ['.']:
                                        num_elm_ar = [] 
                                  else:
                                     if num_elm_ar and ('.' in num_elm_ar):
                                        num_elm_ar.reverse()
                                        num_elm = ''.join(num_elm_ar)
                                        freq_ar.append(float(num_elm))
                                        num_elm_ar = []
                               elif (d == ',') and ('.' in num_elm_ar):
                                        num_elm_ar.reverse()
                                        num_elm = ''.join(num_elm_ar)
                                        freq_ar.append(float(num_elm))
                                        num_elm_ar = []
                               if d==',':
                                  d = '.'
                               num_elm_ar.append(d)
                            elif not d.strip():
                               if (len(num_elm_ar)==1) and (num_elm_ar[0] == '.'):
                                  num_elm_ar = []
                               else:
                                  if num_elm_ar: 
                                     num_elm_ar.reverse()
                                     num_elm = ''.join(num_elm_ar)
                                     freq_ar.append(float(num_elm))
                                  num_elm_ar = []
                               break   
                            i = i - 1
                        print num_elm_ar     
                        if (len(num_elm_ar)==1) and (num_elm_ar[0] == '.'):
                           num_elm_ar = []
                        elif num_elm_ar:   
                           num_elm_ar.reverse()
                           num_elm = ''.join(num_elm_ar)
                           freq_ar.append(float(num_elm))
                           num_elm_ar = []
          #print freq_ar 
          return freq_ar        , text_sp[0]       

if __name__=="__main__":
   
   obj = getPercText() 
   text = "hjh 7% klklkl"
   #obj.getRateValue(text)
   #print obj.getRateValue('Freddie Mac 0% Perpetual')
   #print obj.getRateValue('DOW CHEMICAL')
   #sys.exit()
   #print obj.getNumAlphaNonSpace('STD SWAP 3M JB+ 1.63% 20/09/2014')
   #print obj.getCurrValue('Osaka Gas Ord JPY50')
   #print obj.getPointsValue('khjh jhjhjh jhjhj 90p')
   #print obj.gettypeSingle('The Company')
   #print obj.openTypeGroup(['The Company'])
   #print obj.gettype('-12.00%')
   print obj.gettype("Don't Delay! What your business needs to know about expanded HIPAA requirements")
   #print obj.gettype('7,00')
   #print obj.getFTSEJSEprops_text('DTOP 5400')
   #print obj.getBPSprop('BLUE DIAMOND JIBAR+285.5BPS 08')
   #print obj.getLEGprop('STD SWAP RECEIVABLE LEG JIBAR+')
   #ar = obj.datePatternNormalise(['20100630','20100630','20100630'])
   #ar = obj.datePatternNormaliseTwo(['11/15/11', '06/07/32', '09/28/13'])
   #print ar
   #print obj.getRateValue('3,125% sjhdjshdjshj djshdjsh') 

