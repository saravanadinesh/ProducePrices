#-------------------------------------------------------------------------------------------------------------------------------------
# usda_mmn_utils.py
# Description:
#   This is a collection of utility functions that can be used by other code. 
# TODO: Use .format method or a better way to pass parameters Ref: https://github.com/systemcatch/eiapy/blob/master/eiapy.py
# ------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import numpy as np
import requests
import json
import os
from datetime import datetime, timedelta
import pickle

# ------------------------------------------------------------------------------------------------------------------------------------
# get_mars_response
# Description:
#   This is the basic MARS API request function. 'MARS' is the name USDA uses for its API for interacting with their "My Market News"
#   reports. 
# Input:
#   url_add_on_str: This is an optional string parameter that extends the root url which is needed to access deeper parts of their 
#   database
# Output:
#   resp: The raw response   
# Note: 
#   USDA MARS API uses HTML basic auth. So you need to use this "API key", not as an API key, but as a username. 
# ------------------------------------------------------------------------------------------------------------------------------------
def get_mars_response(url_add_on_str = None):
    url = 'https://marsapi.ams.usda.gov/services/v1.2/reports'
    if url_add_on_str is not None:
        url = url+url_add_on_str
    username = os.getenv("USDA_MARS_API_KEY")   # You need to store your API key as a User Variable in Windows or store it in .bashrc 
    headers={'username':username}   
    resp = requests.get(url, auth = (username, "")) # This is the API call to the USDA MMN database
    return(resp)

# ------------------------------------------------------------------------------------------------------------------------------------
# get_markets_list
# Description:
#   Returns a list of proprietary market names and their corresponding market ids that USDA calls 'slug_id's
# Inputs: None
# Output
#   markets_df
#       A dataframe containing two fields: market_name and slug_id
# ------------------------------------------------------------------------------------------------------------------------------------
def get_markets_list():
    if os.path.exists('markets_db.csv'):    # We need to get fresh data from USDA for this function only once
        try:
            markets_df = pd.read_csv('markets_db.csv', dtype=str)
        except Exception as e:
            print("Exception <" + e + "> occurred while trying to read markets_db.csv file")
    else:   # This will execute only if you accidentally delete the markets_db.csv file 
        resp = get_mars_response()  # This is the API call to the USDA MMN database

        # Clean up the data and retrieve only relevant information
        raw_data = pd.DataFrame.from_dict(json.loads(resp.text))
        raw_data['markets'] = raw_data['markets'].str[0]
        raw_data['market_types'] = raw_data['market_types'].str[0]
        filtered_df = raw_data[raw_data['market_types'] == "Terminal"]  # There are cattle auction data and others that we don't care about
        markets_df = filtered_df[['slug_id', 'report_title', 'markets']].rename(columns={'report_title': 'market_name'})

        # Convert from USDA term for terminal markets to proprietary names that we will use throughout this repo
        markets_df['markets'] = markets_df['markets'].str.replace("Terminal Market", "")
        for index, row in markets_df.iterrows():
            if "Fruit" in row['market_name']:
                markets_df.at[index,'market_name'] = row['markets'] + "fruits"
            elif "Nuts" in row['market_name']:
                markets_df.at[index,'market_name'] = row['markets'] + "nuts"
            elif "Vegetables" in row['market_name'] or "Vegetable" in row['market_name']:
                markets_df.at[index,'market_name'] = row['markets'] + "vegetables"
            elif "Onions and Potatoes" in row['market_name']:
                markets_df.at[index, 'market_name'] = row['markets'] + "onions and potatoes"
            elif "Asian Vegetable" in row['market_name']:
                markets_df.at[index, 'market_name'] = row['markets'] + "asian vegetables"
            elif "Herbs" in row['market_name']:
                markets_df.at[index, 'market_name'] = row['markets'] + "herbs"
            elif "Tropical F&V" in row['market_name']:
                markets_df.at[index, 'market_name'] = row['markets'] + "tropical f&v"
            elif "Asian Vegetable" in row['market_name']:
                markets_df.at[index, 'market_name'] = row['markets'] + "asian vegetables"

        markets_df.drop(columns=['markets'],inplace=True)
        markets_df.to_csv("markets_db.csv", index=False)    # Create the database. Once this is created, we don't need to create it ever again

    return(markets_df)

# ------------------------------------------------------------------------------------------------------------------------------------
# get_slug_id
# Description
#   For a given market_name, finds its corresponding slug_id
# Input
#   market_name
#       Propreitary market name (string). The function get_markets_list() outputs propreitary names and their slug-ids. 
# Output
#   slug_id 
#       The market identification number (string)  corresponding to the terminal market in USDA database. USDA calls this "slug_id" for  
#       unknown reason. It is a 4 digits numeric ID. To see a list of terminal makets and their corresponding slug_ids, use the function
#       get_markets_list(). This parameter is mandatory if market_name isn't specified.
# ------------------------------------------------------------------------------------------------------------------------------------
def get_slug_id(market_name):
    markets_df = get_markets_list()
    markets_dict = dict(markets_df.values)
    try:
        slug_id = list(markets_dict.keys())[list(markets_dict.values()).index(market_name)]
    except:
        print("The market_name you supplied is not a valid market name")

    return(slug_id)

# ------------------------------------------------------------------------------------------------------------------------------------
# get_market_name
# Description
#   For a given market_name, finds its corresponding slug_id
# Input
#   slug_id 
#       The market identification number (string)  corresponding to the terminal market in USDA database. USDA calls this "slug_id" for  
#       unknown reason. It is a 4 digits numeric ID. To see a list of terminal makets and their corresponding slug_ids, use the function
#       get_markets_list(). This parameter is mandatory if market_name isn't specified.
# Output
#   market_name
#       Propreitary market name (string) that corresponds to the slug_id. 
# ------------------------------------------------------------------------------------------------------------------------------------
def get_market_name(slug_id):
    markets_df = get_markets_list()
    markets_dict = dict(markets_df.values)
    try:
        market_name = markets_dict[slug_id]
    except:
        print("The slug_id you supplied is not a valid slug_id")

    return(market_name)

# ------------------------------------------------------------------------------------------------------------------------------------
# get_prices_1y
# Description:
#   Downloads the price data for a commodity available in USDA database for the given year. The function doesn't offer any flexibility
#   with respect to using filters, such as requesting data only related to organic produce, specific variety of the produce etc.,
#   simply because the data retreived without filters is only a few MBs. The function implements caching of data to avoid 
#   unnecessary API requests to the USDA database. This caching is opaque to the user. 
#
# Inputs:
#   commodity
#       The name of the commodity as it appears in the USDA database. To see a list of possible commodities, use the function 
#       get_commodities_list(). This parameter is mandatory. 
#   slug_id 
#       The market identification number (string)  corresponding to the terminal market in USDA database. USDA calls this "slug_id" for  
#       unknown reason. It is a 4 digits numeric ID. To see a list of terminal makets and their corresponding slug_ids, use the function
#       get_markets_list(). This parameter is mandatory.
#   year
#       Year as a number (and not a string) in YYYY format. This parameter is mandatory
#   debug_prints:
#       This is used to display debug prints. The default value is True. User can set it to False if needed. 
# ------------------------------------------------------------------------------------------------------------------------------------
def get_prices_1y(commodity, slug_id, year, debug_prints=True): 
    market_name = get_market_name(slug_id)
    market_name1 = "_".join(market_name.split())
    cache_filename = market_name1 + "_"+ commodity + "_" + str(year) + ".csv"
    local_dir_name = os.path.dirname(__file__)
    file_path = local_dir_name + "/cache/" + cache_filename

    if os.path.exists(file_path):   # If the data is available in the cache...
        prices_df = pd.read_csv(file_path)
        return(prices_df)
    
    date_str = "01/01/"+str(year)+":12/31/"+str(year)
    filter_dict = {'commodity':commodity, 'report_begin_date':date_str}
    filter_str = ""
    for key in filter_dict.keys():
        filter_str = filter_str+key+"="
        filter_str = filter_str+filter_dict[key]+";"

    url_append = "/"+slug_id +  "?q="+ filter_str + "&allSections=true"
    response = get_mars_response(url_append)
    raw_data = json.loads(response.text)
    raw_df = pd.DataFrame(raw_data[1]['results'])
    raw_df['date'] = pd.to_datetime(raw_df['report_date'])
    raw_df.loc[:, 'price'] = (raw_df.loc[:,'high_price'].astype('float')+raw_df.loc[:,'low_price'].astype('float'))/2
    prices_df = raw_df[['date','slug_id', 'commodity','variety', 'package','item_size','properties','grade','organic',\
                        'origin','price','unit_sales']]

    prices_df.to_csv(file_path, index=False)
    return(prices_df)

# ------------------------------------------------------------------------------------------------------------------------------------
# get_prices
# Descriptions:
#   OBtains the price data for a commodity available in USDA database for given years by joining together prices of each year. The 
#   function doesn't offer any flexibility with respect to using filters, such as requesting data only related to organic produce, 
#   specific variety of the produce etc., simply because the data retreived without filters is only a few MBs.
#   
# Inputs:
#   commodity
#       The name of the commodity as it appears in the USDA database. To see a list of possible commodities, use the function 
#       get_commodities_list(). This parameter is mandatory. 
#   slug_id 
#       The market identification number (string)  corresponding to the terminal market in USDA database. USDA calls this "slug_id" for  
#       unknown reason. It is a 4 digits numeric ID. To see a list of terminal makets and their corresponding slug_ids, use the function
#       get_markets_list(). This parameter is mandatory.
#   start_year
#       Starting year as a number (and not a string) in YYYY format. This parameter is mandatory
#   end_year
#       Ending year as a number (and not a string) in YYYY format. This parameter is optional. If it is not specified, data just for the start_
#       year will be returned
#   debug_prints:
#       This is used to display debug prints. The default value is True. User can set it to False if needed. 
# Output
#   prices_df
#       A dataframe that contains all the data returned by USDA API including high_price and low_price. The difference between high
#       and low price on any given day is usually insignificant. In case of an error, this dataframe will be empty. Turn on 
#       debug_prints to see the error
#
# Note: This function isn't recommended for obtaining daily updates or if you need data spanning part of one year and part of another
#       year. For such purposes it is better to directly use the USDA MARS API functions.
# ------------------------------------------------------------------------------------------------------------------------------------
def get_prices(commodity, slug_id, start_year, end_year = None, debug_prints=True): 
    start_prices_df = get_prices_1y(commodity=commodity, slug_id=slug_id, year=start_year)
    if end_year is None:
        return(start_prices_df)
    
    temp_dfs=[]
    temp_dfs.append(start_prices_df)
    for year in range(start_year+1, end_year+1):
        temp_df = get_prices_1y(commodity=commodity, slug_id=slug_id, year=year)
        temp_dfs.append(temp_df)
    
    prices_df = pd.concat(temp_dfs, ignore_index=True)
    return(prices_df)
        
# ------------------------------------------------------------------------------------------------------------------------------------
# get_commodities_list
# Description:
#   Returns a list of commodities available in a given terminal market 
# Inputs
#   slug_id 
#       The market identification number (string)  corresponding to the terminal market in USDA database. USDA calls this "slug_id" for   
#       unknown reason. It is a 4 digits numeric ID. To see a list of terminal makets and their corresponding slug_ids, use the function
#       get_markets_list(). This parameter is mandatory if market_name isn't specified.
#   market_name (mutually exclusive with slug_id)
#       Propreitary market name. The function get_markets_list() outputs propreitary names and their slug-ids. This is used only when 
#       slug_id isn't specified. Using slug_id may be preferred if the user wants to iterate over several markets. Using market_name
#       may be preferred for demo code. This parameter is mandatory if slug_id isn't specified.
# Output
#   commodities_list
#       A list (python list) of commodity names
# ------------------------------------------------------------------------------------------------------------------------------------
def get_commodities_list(slug_id, market_name=None):
    # Check for missing arguments and do other sanity checks
    if (slug_id is None) and (market_name is None):
        print("You must pass a slug_id or a market_name")
        return([])
    elif (slug_id is not None) and (market_name is not None):
        print("Passing both arguments is discouraged. In this case, slug_id will be used and market_name will be ignored")
    elif (slug_id is None) and (market_name is not None):
        slug_id = get_slug_id(market_name)

    # Finally we are ready to actually create the commodity list
    filename = "cache/commodities_list_"+slug_id+".pkl"
    if os.path.exists(filename):
        with open(filename, 'rb') as fp:
            commodities_list = pickle.load(fp)
    else:
        # Create the commodities list from ten days worth of data from the market in question. We assume that all commodities ever
        # traded in a market can be found within ten day's worth of data. We use the latest 10 days for this purpose
        end_date = datetime.today().strftime("%m/%d/%Y")
        start_date = (datetime.today() - timedelta(days=10)).strftime("%m/%d/%Y")
        filter_str = "report_begin_date="+start_date+":"+end_date
        url_append = "/"+slug_id +  "?q="+ filter_str + "&allSections=true"
        response = get_mars_response(url_append)
        raw_data = json.loads(response.text)
        raw_df = pd.DataFrame(raw_data[1]['results'])
        prices_df = raw_df[['report_date','slug_id', 'commodity','variety', 'package','item_size','properties','grade','organic',\
                            'origin','low_price','high_price','unit_sales']]
        commodities_list = list(prices_df['commodity'].unique())
        with open(filename, 'wb') as fp:
            pickle.dump(commodities_list, fp)
    return(commodities_list)

    
# ------------------------------------------------------------------------------------------------------------------------------------
# get_package_weight_map
# Description:
#   In the price data returned by the MARS API, the prices do not have a common unit like $/per pound and instead it is price for the
# package it is listed against. Due to various package types every commodity could have, we use this funciton to map package types
# of different commodities to their equivalent in pounds. This is done based on the data supplied by USDA - they went and measured,
# for every commodity, the weight of different package types in pounds. 
#
# Inputs
#   input_df: A dataframe containing four columns
#   commodity
#       The commodity as used in the MARS API. This parameter is mandatory. 
#   variety_list
#       A list of varieties of the given commodity as used in the MARS API. This parameter is mandatory.
#   package_types
#       A list of package types for the commodity. This parameter is mandatory 
#   debug_prints
#       Boolean variable. If set, it will print error information, if there is any error.
# Output
#   A dataframe that contains the mapping between different package types and their corresponding weight in pounds.
#   dataframe keys will be: commodity, variety, package, pounds
# ------------------------------------------------------------------------------------------------------------------------------------
def get_package_weight_map(commodity, variety_list, package_types, debug_prints = False):
    
    # Check if the commodity already has a package to pounds conversion table
    MARS_to_TONW = pd.read_csv("MARS_to_TONW.csv")  # TONW = Table of Net Weights
    mars_to_tonw_df = MARS_to_TONW[MARS_to_TONW['MARS commodity'] == commodity]
    if mars_to_tonw_df.empty:
        if debug_prints == True:
            print("Please populate \'MARS commodity\', \'MARS variety\', \'tonw commodity\' for "+commodity\
                  + "in MARS_to_TONW.csv file")
        return(None)
    else:
        package_to_pounds = pd.read_csv("package_to_pounds.csv")
        temp_df = package_to_pounds[(package_to_pounds['commodity'] == commodity) & (package_to_pounds['variety'].isin(variety_list))]
        if not temp_df.empty:
            if set(variety_list).issubset(set(temp_df.variety.unique())):
                if set(package_types).issubset(set(temp_df.package.unique())):
                    if not np.isnan(temp_df.pounds.values).any():
                        print("I shouldn't be here")
                        return(temp_df)

        commodity_col=[]
        variety_col=[]
        package_col=[]
        pounds_col=[]
        for variety in variety_list:
            for package in package_types:
                pounds = None
                words_list = (package.replace("/"," ")).split(" ")
                if "lb" in package:
                    pounds = int(words_list[words_list.index("lb")-1])
                elif "kg" in package:
                    pounds = int(words_list[words_list.index("kg")-1]*2.2)  # 1kg = 2.2lb
                else:
                    if not mars_to_tonw_df[mars_to_tonw_df["MARS variety"] == variety].empty:
                        tonw_commodity = mars_to_tonw_df[mars_to_tonw_df["MARS variety"] == variety].iloc[0]["tonw commodity"]
                        if tonw_commodity is not None:  # TONW = Table of Net Weights
                            tonw_df = pd.read_excel("table of net weights corrected names.xlsx")
                            sub_tonw_df = tonw_df[(tonw_df["Commodity"] == tonw_commodity) & (tonw_df["Pack Description"] == package)]
                            if not sub_tonw_df.empty:
                                pounds = int(sub_tonw_df.iloc[0]["Package Weight"])

                commodity_col.append(commodity)
                variety_col.append(variety)
                package_col.append(package)
                pounds_col.append(pounds)

        pound_mapper = pd.DataFrame({"commodity":commodity_col, "variety":variety_col, "package":package_col, "pounds":pounds_col})
        if not (set(package_types).issubset(set(pound_mapper.package)) and set(variety_list).issubset(set(pound_mapper.variety))):
            if debug_prints == True:
                print("Some variety or package type are missing in the mapping table")
        
        package_to_pounds_new = pd.concat([package_to_pounds, pound_mapper])
        package_to_pounds_new.drop_duplicates(inplace=True)
        package_to_pounds_new.to_csv("package_to_pounds.csv", index=False)
        return(pound_mapper)

    

