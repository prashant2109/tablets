import ServiceMatchingNew
dt_obj  = ServiceMatchingNew.UploadFile()
class DTSIG():
    def dt_sig(self, txt):
        a, b, c, d = dt_obj.getSheetData(txt)
        ftxt    = []
        for i, c in enumerate(txt):
            if i in d:
                if ftxt and ftxt[-1][0]: #ftxt[-1][0] == d[i][0]:
                    ltup    = ftxt[-1]
                    if ltup[0][-1] == d[i][0]:
                        ltup  = (ltup[0]+d[i][0], ltup[1]+d[i][1])
                        ftxt[-1]    = ltup
                    else:
                        ftxt.append(d[i])
                else:
                    ftxt.append(d[i])
            else:
                ftxt.append(('', c))
        s   = ''
        for r in ftxt:
            if r[0] and r[0][0] == 'n' and len(r[0]) <= 4:
                s   += '{n}'
            else:
                s   += r[1]
        return s

if __name__ == '__main__':
    obj =  DTSIG()
    txt = 'Next Payment 4/15/2020'
    print obj.dt_sig(txt)
                
