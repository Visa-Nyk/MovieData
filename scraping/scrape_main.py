from datetime import datetime
import dateutil.parser as p
import requests

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

def get_data(movie_id):
    site = BeautifulSoup(requests.get(f"https://www.imdb.com/title/{movie_id}").content,  "html.parser")
    details = site.find("section", {"data-testid":"Details"})

    row = {"tconst": movie_id}
    row["country"] = []
    row["language"] = []
    row["release"] = ""
    if details is not None:
        info = details.find("ul")
        try:
            for li in info.find_all("li"):
                if not row["country"] and li.find(lambda tag: tag.name == "span" and "countr" in tag.text.lower()):
                    for li in li.find("div").find("ul").find_all("li"):
                        link = li.find("a")
                        row["country"].append(link.getText().strip())
                if not row["language"] and li.find(lambda tag: tag.name == "span" and "language" in tag.text.lower()):
                    for li in li.find("div").find("ul").find_all("li"):
                        link = li.find("a")
                        row["language"].append(link.getText().strip())
                if not row["release"] and li.find(lambda tag: tag.name == "a" and "release" in tag.text.lower()):
                    link = li.find("div").find("ul").find("a")
                    row["release"] = link.getText().strip().split(" (")[0]
        except Exception as e:
            print(movie_id)
            print(e)

    bo = site.find("section", {"data-testid":"BoxOffice"})
    row["budget"] = ""
    row["gross_us"] = ""
    row["opening_gross"] = ""
    row["gross"] = ""
    if bo is not None:
        info = bo.find("ul")
        try:
            for li in info.find_all("li"):
                if not row["budget"] and li.find(lambda tag: tag.name == "span" and "budget" in tag.text.lower()):
                    span = li.find("div").find("ul").find("li").find("span")
                    row["budget"] = (span.getText().strip())
                if not row["gross"] and li.find(lambda tag: tag.name == "span" and "gross worldwide" in tag.text.lower()):
                    span = li.find("div").find("ul").find("li").find("span")
                    row["gross"] = (span.getText().strip())
                elif not row["gross_us"] and li.find(lambda tag: tag.name == "span" and "gross" in tag.text.lower()):
                    span = li.find("div").find("ul").find("li").find("span")
                    row["gross_us"] = (span.getText().strip())
                if not row["opening_gross"] and li.find(lambda tag: tag.name == "span" and "opening" in tag.text.lower()):
                    spans = li.find("div").find("ul").find_all("li")            
                    row["opening_gross"] = spans[0].find("span").getText().strip()
                    row["release"] = spans[1].find("span").getText().strip()
        except Exception as e:
            print(movie_id)
            print(e)
    
    return row 
    
    
def main(list_of_movies =  ['tt5536610', 'tt6523720', 'tt0110006', 'tt0082484', 'tt1477076']):
    data = pd.DataFrame()
    for i, id in enumerate(list_of_movies, start = 1):
        print(f"{i}/{len(list_of_movies)}")
        if i % 1000 == 0:
            data.to_csv(f"scraped_data_checkpoint_{i}.csv", index = False)
            
        data = data.append(get_data(id), ignore_index = True)
    now = datetime.now().strftime("%Y_%m_%d_%H_%M")
    data.to_csv(f"scraped_data_{now}.csv", index = False)
    return data

def clean_formatting(path_to_csv):
    release = pd.read_csv(path_to_csv)
    release["country"] = release["country"].apply(eval)
    release["language"] = release["language"].apply(eval)
    release["budget_currency"] = release["budget"].str[0]
    release["budget"] = release["budget"].str[1:].str.replace(r"\D+", "").apply(float,1)
    for col in release.columns:
        if "gross" in col and release[col].dtype == "object":
            release[col] = np.where(release[col].str.contains("$"), release[col].str[1:].str.replace(r"\D+", "").apply(float, 1), float("nan"))
    release["release"] = release["release"].apply(p.parse)
    release.to_csv("release_info.csv.gz", compression = "gzip", index = False)