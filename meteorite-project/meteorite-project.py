import io
import reverse_geocoder as rg
import pandas as pd
import requests
import json
import os
from collections import defaultdict

def get_countries(coordinates):
    countries = defaultdict(lambda :0)
    geo = rg.RGeocoder(mode=2, verbose=True, stream=io.StringIO(open('./input/rg_cities1000.csv', encoding='utf-8').read()))
    results = geo.query(coordinates)
    for i in results:
        countries[i['cc'].lower()]+=1
    return countries

def is_published(country_code, year):
    result = False
    url = "http://api.worldbank.org/countries/"+country_code+"/indicators/IP.JRN.ARTC.SC?format=json&date="+str(year)
    response = requests.get(url)
    if response.status_code == 200:
        response_native = json.loads(response.text)
        if 'page' in response_native[0] and response_native[0]['page'] > 0 :
            result = True
    return result



def get_data():
    url = "https://data.nasa.gov/resource/y77d-th95.json?$limit=50000"
    if os.path.isfile('./input/input.json'):
        # Reading data back
        print("Reading data back from file...\n")
        with open('./input/input.json', 'r') as f:
            data = json.load(f)
    else:
        # Call API and get json response:
        print("Sending api request to get data...\n")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            with open('./input/input.json', 'w') as f:
             json.dump(data, f)


    # Store response in DataFrame
    df = pd.DataFrame(data)
    df = df[(df.reclat != 0.0) & (df.reclong != 0.0)]
    df["year"] = df["year"].str[:4]
    df_2008 = df[(df['year'].isin(["2008"]))]
    df_2010 = df[(df['year'].isin(["2010"]))]
    return df_2008, df_2010

def main():

    print("Processing started...\n")
    df_2008,df_2010 = get_data()

    #converting to float data type
    df_2008.reclat = df_2008.reclat.astype(float).fillna(0.0)
    df_2008.reclong = df_2008.reclong.astype(float).fillna(0.0)
    df_2010.reclat = df_2010.reclat.astype(float).fillna(0.0)
    df_2010.reclong = df_2010.reclong.astype(float).fillna(0.0)

    # getting country code and meteorite count
    coor_2008 = list(zip(df_2008['reclat'], df_2008['reclong']))
    coor_2010 = list(zip(df_2010['reclat'], df_2010['reclong']))

    countries_2008 = get_countries(coor_2008)
    countries_2010 = get_countries(coor_2010)
    countries_2008_documented = []
    countries_2010_documented = []

    print("2008 countries with meteorite count...")
    [ print(i + ": "+str(countries_2008[i])) for i in countries_2008 ]

    print("\n2010 countries with meteorite count...")
    [ print(i + ": "+str(countries_2010[i])) for i in countries_2010 ]

    for country in countries_2008:
        if is_published(country,2008):
            countries_2008_documented.append((country,countries_2008[country]))

    for country in countries_2010:
        if is_published(country,2010):
            countries_2010_documented.append((country,countries_2010[country]))

    countries_2008_documented = sorted(countries_2008_documented, key=lambda tup: tup[1], reverse=True)[:5]
    countries_2010_documented = sorted(countries_2008_documented, key=lambda tup: tup[1], reverse=True)[:5]

    print("\nTop 5 2008 countries documented...")
    print(countries_2008_documented)
    print("\nTop 5 2010 countries documented...")
    print(countries_2010_documented)

    countries_2008_documented_set = set([i[0] for i in countries_2008_documented])
    countries_2010_documented_set = set([i[0] for i in countries_2010_documented])

    print("\n countries unique to 2008...")
    print(str(countries_2008_documented_set.difference(countries_2010_documented_set)))

    print("\n countries unique to 2010...")
    print(str(countries_2010_documented_set.difference(countries_2008_documented_set)))

    print("\n countries common to both years...")
    print(str(countries_2008_documented_set & countries_2010_documented_set))


if __name__ == "__main__":
    main()