import requests
from bs4 import  BeautifulSoup
import pandas as pd

url="https://fullratio.com/stocks/sectors/technology"

r=requests.get(url)
#print(r)
html=r.content

soup= BeautifulSoup(html,"html.parser")
#print(soup.prettify())
# next_page=soup.find("a",class_="page-item next").get("href")
#
# print(next_page)

table=soup.find("table",class_="table table-striped table-scroll-wrap")
title=table.find_all("th")
#print(title)
#print(table)
header=[]

for i in title:
    name=i.text
    header.append(name)
#print(header)
df=pd.DataFrame(columns=header)
#print(df)

rows=table.find_all("tr")
#print(row)


for i in rows[1:]:
    first_td=i.find_all("td")[0].find("span",class_="label label-secondary").text.replace('\n',"")
    data= i.find_all("td")[1:]
    row=[tr.text for tr in data]
    row.insert(0,first_td)
    l=len(df)
    df.loc[l]=row
print(df)








