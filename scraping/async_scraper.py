## this works and is kinda cool but sends too many requests at once to be usable for a decent sized data

import re
import time
import requests
import asyncio
from asyncio import AbstractEventLoop
from collections import defaultdict

import pandas as pd
from bs4 import BeautifulSoup
import aiohttp

def main(movies = ["tt0000574", "tt0001892", "tt0002101", "tt0002130", "tt0002186", "tt0002199", "tt0002423", "tt0002445", "tt0002452", "tt0002461"]):
    # Create loop
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(get_demo_ratings_for_movies(movies, loop))
    return result


async def get_html(id: str) -> str:
    url = f"https://www.imdb.com/title/{id}/ratings"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            html = await resp.text()
            return html

def get_ratings_and_n_by_demo(td):
    demo_rating = td.find("div", class_ = "bigcell").getText()
    if demo_rating !=  "-":
        demo_n = td.find("div", class_ = "smallcell").find("a").getText()
    else:
        demo_rating = demo_n = "0"
    return demo_rating, demo_n
    

def calc_mean_for_combination(group1_n: int, group1_avg: float, group2_n: int, group2_avg: float) -> float:
    return (group1_n * group1_avg + group2_n * group2_avg) / (group1_n + group2_n)


def combine_under_18_and_18_29(ratings):
    variables = ["", "_women", "_men"]
    for var in variables:
        u18 = var + "_under_18"
        o18 = var + "_18-29"
        u29_ratings = calc_mean_for_combination(ratings["n" + u18], ratings["rating" + u18], ratings["n" + o18], ratings["rating" + o18])
        u29_n = ratings["n" + u18] + ratings["n" + o18]
        ratings["n" + var + "_under_29"] = u29_n
        ratings["rating" + var + "_under_29"] = u29_ratings
    return ratings
    
async def get_demo_ratings_for_movies(l: list, loop):
    dd = defaultdict(list)
    tasks = []
    for i, id in enumerate(l):
        tasks.append((loop.create_task(get_html(id)), id))
    
    for task, id in tasks:
        html = await task
        try:
            ratings = get_demo_ratings(html, id)
            for key in ratings:
                dd[key].append(ratings[key])
        except Exception as e:
            print(id)
            print(e)
    ratings =  pd.DataFrame(dd)        
    ratings.to_csv("ratings.csv", index = False)
    ratings = combine_under_18_and_18_29(ratings)
    
    return ratings
    

def get_demo_ratings(html_content: str, id: str) -> dict: 
    soup = BeautifulSoup(html_content, 'html.parser')
     
    tables = soup.find_all("table") 
    rating_tab = tables[0]
    cur_rating = 10
    result = {}
    result["id"] = id
    for i, tr in enumerate(rating_tab.find_all("tr")):
        if i > 0:
            td = tr.find_all("td")[2]
            num = td.find("div", class_ = "leftAligned").getText()
            result[f"n_ratings_{cur_rating}"] = int(num.strip().replace(",", ""))
            cur_rating -= 1
    demo_tab = tables[1]
        
    sex = ["", "", "_men", "_women"]
    age = ["", "", "_under_18", "_18-29", "_30-45", "_over_45"]
    for i, tr in enumerate(demo_tab.find_all("tr")):
        if i > 0:
            for j, td in enumerate(tr.find_all("td")):
                if j > 0:
                    demo_rating, demo_n = get_ratings_and_n_by_demo(td)
                    demo_string = sex[i] + age[j]    
                    result["rating" + demo_string] = float(demo_rating.strip())
                    result["n" + demo_string] = int(demo_n.strip().replace(",", ""))
    
    us_tab = tables[2].find_all("tr")[1]    
    for i, td in enumerate(us_tab.find_all("td")[-2:]):
        demo_rating, demo_n = get_ratings_and_n_by_demo(td)
        demo_string = "_US" if i == 0 else "_non_US"
        
        result["rating" + demo_string] = float(demo_rating.strip())
        result["n" + demo_string] = int(demo_n.strip().replace(",", ""))
        
    return result