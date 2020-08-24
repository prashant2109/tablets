import copy
phs = ['Q1', 'Q1ESD', 'STDQ1', 'Q2', 'Q2ESD', 'STDQ2', 'H1','STDH1', 'H1ESD', 'Q3', 'Q3ESD', 'STDQ3', 'M9', 'M9ESD', 'STDM9', 'Q4', 'Q4ESD','STDQ4', 'STDQ', 'H2', 'H2ESD', 'STDH2','FY', 'FYESD', 'STDFY', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October','November','December', '1M', '2M', '3M', '4M', '5M', '6M', '7M', '8M', '9M', '10M', '11M', '12M']
def year_sort(data_year_sequence):
    qhs = copy.deepcopy(phs)
    #hist_dict, forecast_dict = get_historical_forecast_year_dict(data_year_sequence)
    #hist_li = get_sorted_historical_and_forecast_year_li(hist_dict)
    #forecast_li = get_sorted_historical_and_forecast_year_li(forecast_dict)
    #return hist_li+forecast_li
    #print 'RRRRRRRRRRRRRRRRRRRRR', data_year_sequence
    ph_dict = {}
    for ph in data_year_sequence:
        #print 'PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP', ph
        year = int(ph[-4:])
        qh = ph[:-4]
        if qh not in qhs:
            qhs.append(qh)
        if qh in qhs:
            idx = qhs.index(qh)
        else:
            idx = len(qhs) + 1
            qhs.append(qh)
        if year not in ph_dict:
            ph_dict[year] = []
        ph_dict[year].append((idx, qh, ph))
    sorted_phs = []
    for year in sorted(ph_dict.keys()):
        qhk = ph_dict[year]
        qhk.sort()
        for p in qhk:
            sorted_phs.append(p[2])
    return sorted_phs

def get_historical_forecast_year_dict(data_year_sequence):
    hist_dict = {}
    forecast_dict = {}
    for each_year in data_year_sequence:
        each = each_year    
        if not each:continue
        if 'E' == each[-1]:
            x = int(each[-5:-1])
            if not forecast_dict.get(x, {}):
                forecast_dict[x] = {} 
            y = each[:-5] 
            if not forecast_dict[x].get(y, []):
                forecast_dict[x][y] = []
            forecast_dict[x][y].append(each_year)
        else:
            try:
               x = int(each[-4:])
            except: continue
            if not hist_dict.get(x, {}):
                hist_dict[x] = {}
            y = each[:-4] 
            if not hist_dict[x].get(y, {}):
                hist_dict[x][y] = []
            hist_dict[x][y].append(each_year)
    return hist_dict, forecast_dict


def get_sorted_historical_and_forecast_year_li(tmp_dict):
    ks = tmp_dict.keys()
    ks.sort()
    qyear = ['Q1', 'Q2', 'Q3', 'Q4']
    hyear = ['H1', 'H2']
    fyear = ['FY']
    myear = ['M9']
    final_sort_years = []
    for k in ks:
        vs = tmp_dict[k]
        vks = vs.keys()
        if 'Q1' in vks or 'Q2' in vks or 'Q3' in vks or 'Q4' in vks:
            for vk in qyear:
                if tmp_dict[k].get(vk, {}):
                    final_sort_years += tmp_dict[k][vk]

        if 'H1' in vks or 'H2' in vks:
            for vk in hyear:
                if tmp_dict[k].get(vk, {}):
                    final_sort_years += tmp_dict[k][vk]

        if 'FY' in vks: 
            for vk in fyear:
                if tmp_dict[k].get(vk, {}):
                    final_sort_years += tmp_dict[k][vk]
        if 'M9' in vks: 
            for vk in myear:
                if tmp_dict[k].get(vk, {}):
                    final_sort_years += tmp_dict[k][vk]
    return final_sort_years

if __name__ == "__main__":
    li = ['1M2010', '2M2010', '3M2010', '4M2010', '5M2010', '6M2010', '7M2010', '8M2010', '9M2010', '10M2010', '11M2010', '12M2010']
    li = ['FY2017', 'Q12017']
    print year_sort(li)
