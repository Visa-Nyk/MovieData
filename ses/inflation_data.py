#(rahamäärä*indeksien suhde)/euromarkka-muunnoskerroin*100
##(1000*239166/116)/594.573
from requests import post
import json
import pandas as pd
import time
from datetime import date

def get_inflation_data():
    res={"year":[],"index":[]}
    for year in range(1970, date.today().year):        
        query_json={"query":[{"code":"Vuosi","selection":{"filter":"item","values":[str(year)]}}],"response":{"format":"json"}}
        sleep=2
        resp=post("https://pxnet2.stat.fi/PXWeb/api/v1/fi/StatFin/hin/khi/vv/statfin_khi_pxt_11xy.px", json=query_json).json()
        while 'error' in resp:
            time.sleep(sleep)
            resp=post("https://pxnet2.stat.fi/PXWeb/api/v1/fi/StatFin/hin/khi/vv/statfin_khi_pxt_11xy.px", json=query_json).json()
            sleep+=1
        val=(resp["data"][0]["values"][0])
        res["year"].append(year)
        res["index"].append(val)
    pd.DataFrame(res).to_csv("inflation.csv",index=False)