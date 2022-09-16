from bs4 import BeautifulSoup
import re,requests
import pandas as pd
from collections import defaultdict
import time


def get_ratings_and_n_by_demo(td):
    demo_rating = td.find("div", class_="bigcell").getText()
    if demo_rating != "-":
        demo_n = td.find("div", class_ = "smallcell").find("a").getText()
    else:
        demo_rating = demo_n = "0"
    return demo_rating, demo_n
    

def calc_mean_for_combination(group1_n, group1_avg, group2_n, group2_avg):
    return(group1_n * group1_avg + group2_n * group2_avg) / (group1_n + group2_n)

def combine_under_18_and_18_29(ratings):
    vars=["all", "women", "men"]
    for var in vars:
        u18=var+"_under_18"
        o18=var+"_18-29"
        u29_ratings=calc_mean_for_combination(ratings["n_"+u18],ratings["rating_"+u18],ratings["n_"+o18],ratings["rating_"+o18])
        u29_n=ratings["n_"+u18]+ratings["n_"+o18]
        ratings["n_"+var+"_under_29"]=u29_n
        ratings["rating_"+var+"_under_29"]=u29_ratings
    return ratings
    
def get_demo_ratings_for_list_of_movie_ids(l = ["tt0000574", "tt0001892", "tt0002101", "tt0002130", "tt0002186", "tt0002199", "tt0002423", "tt0002445", "tt0002452", "tt0002461"]):
    dd=defaultdict(list)
    for i, id in enumerate(l):
        print(f"haettu {i+1}/{len(l)}")
        res={}
        try:
            res=get_demo_ratings_by_imdb_id(id)
        except Exception as e:
            print("Error: " + id)
        for key in res:
            dd[key].append(res[key])
        time.sleep(2)
        if i % 1000 == 0:
            ratings= pd.DataFrame(dd)        
            ratings.to_csv(f"scraped_ratings_{i}.csv",index=False)
            ratings=combine_under_18_and_18_29(ratings)
            ratings.to_csv(f"scraped_ratings_com_{i}.csv",index=False)
    ratings= pd.DataFrame(dd)        
    ratings.to_csv(f"scraped_ratings_{i}.csv",index=False)
    ratings=combine_under_18_and_18_29(ratings)
    ratings.to_csv(f"scraped_ratings_com_{i}.csv",index=False)
            
            
    
    return ratings
    

def get_demo_ratings_by_imdb_id(id):
    soup = BeautifulSoup(requests.get(f"https://www.imdb.com/title/{id}/ratings").content, 'html.parser')
    
    tables = soup.find_all("table") 
    rating_tab = tables[0]
    cur_rating = 10
    result = {}
    result["tconst"] = id
    for i, tr in enumerate(rating_tab.find_all("tr")):
        if i > 0:
            td = tr.find_all("td")[2]
            num = td.find("div", class_ = "leftAligned").getText()
            result[f"n_ratings_{cur_rating}"] = int(num.strip().replace(",", ""))
            cur_rating -= 1
    demo_tab = tables[1]
    sex=["","all","men","women"]
    
    age=["","","_under_18","_18-29","_30-45","_over_45"]
    for i, tr in enumerate(demo_tab.find_all("tr")):
        if i>0:
            for j,td in enumerate(tr.find_all("td")):
                if j>0:
                    demo_rating,demo_n=get_ratings_and_n_by_demo(td)
                    demo_string=sex[i]+age[j]    
                    result["rating_"+demo_string]=float(demo_rating.strip())
                    result["n_"+demo_string]=int(demo_n.strip().replace(",",""))
    
    us_tab=tables[2].find_all("tr")[1]    
    for i,td in enumerate(us_tab.find_all("td")[-2:]):
        demo_rating,demo_n=get_ratings_and_n_by_demo(td)
        demo_string="us" if i==0 else "non_us"
        
        result["rating_"+demo_string]=float(demo_rating.strip())
        result["n_"+demo_string]=int(demo_n.strip().replace(",",""))
        
    return result