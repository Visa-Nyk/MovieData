import os
from datetime import date
from urllib.request import urlretrieve

import pandas as pd

from inflation_data import get_inflation_data

YEAR = date.today().year

def month_helper(row):
    try:
        return row.month
    except Exception as e:
        return -1
        


##defines a semi-arbitrary hierarchy of genres. The first genre that appears in this list that is defined as a genre for a given movie is selected as the primary genre        
def single_genre_helper(genres):
    all_genres=['Animation', 'Documentary', 'Musical', 'Comedy', 'Action', 'Horror', 'Thriller', 'Adventure', 'Sci-Fi', 'Fantasy', 'Romance', 'War', 'Drama', 'Short', 'Biography', 'Sport', 'Music',  'History', 'Western', 'Crime', 'Mystery', 'Family', 'Film-Noir', 'Adult', 'News']
    for genre in all_genres:
        if genre in genres:
            return genre

def get_price_index():    
    price_index={}
    if not os.path.isfile("inflation.csv"):
        get_inflation_data()
    with open("inflation.csv","r") as inf_data:
        for i,row in enumerate(inf_data):
            if i>0:
                values=row.split(",")
                price_index[int(values[0])]=int(values[1])
    return price_index


## Preprocesses the data. Adjusts monetary values with inflation data and viewer data by population data. Fetches the inflation data and the population data if not available.     
def load_and_preprocess_ses_data():            
    ses_data=pd.read_excel("SES.xlsx")
    #basically just an alias for getting rid of all NA-rows that appear because excel-file is defined to have more rows than its actual contents.
    ses_data=ses_data[ses_data["Alkuperäinen nimi"].notna()]
    #gets rid of  two undefined variables at the end of the df
    ses_data=ses_data.iloc[:,:-2]
    has_release_date=pd.to_numeric(ses_data["Ensi-iltavuosi"],errors="coerce").notna()
    ses_data=ses_data[has_release_date]
    ses_data=ses_data[ses_data["Ensi-iltavuosi"]>=1972]
    
    ses_data.rename(columns={"Lipputulot €":"Lipputulot"},inplace=True)
    price_index=get_price_index()
    for var in ["Budjetti","Lipputulot","Tuotannon tuet"]:
        ses_data[var+"_inf_adj"]=ses_data.apply(lambda row:row[var]*(price_index[YEAR - 1]/price_index[YEAR - 1 if row["Ensi-iltavuosi"] not in price_index else row["Ensi-iltavuosi"]]), axis=1).round()
    if not os.path.isfile("vaesto.xlsx"):
        urlretrieve("https://www.tilastokeskus.fi/static/media/uploads/tup/suoluk/suomilukuina_tau_vrm015.xlsx","./vaesto.xlsx")
    v=pd.read_excel("vaesto.xlsx",header=1)

    vd={}
    for y in range(1970,YEAR):
        vd[y]=v[v["Vuosi"]==y]["Väkiluku"].item()
    
    ses_data["Katsojat per 100000"]=ses_data.apply((lambda x:100000*x["Katsojat"]/vd[YEAR - 1 if x["Ensi-iltavuosi"] not in vd else x["Ensi-iltavuosi"]]),axis=1)
    
    ses_data["Ensi-iltakuu"]=ses_data["Ensi-ilta"].apply(month_helper)
   
            
    # ids = list(ses_data["IMDB id"])
    # ratings = get_demo_ratings_for_list_of_movie_ids(ids)
    # ses_data =  ses_data.merge(ratings, how="left",left_on="IMDB id", right_on="id")
    return ses_data

if __name__ == "__main__":
    fmi = load_and_preprocess_ses_data()
    fmi.to_csv("ses.csv", index = False)