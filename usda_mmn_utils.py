#-------------------------------------------------------------------------------------------------------------------------------------
# usda_mmn_utils.py
# Description:
#   This is a collection of utility functions that can be used by other code. 
# ------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
import requests
import json
import os
from datetime import date
import pickle


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
        url = 'https://marsapi.ams.usda.gov/services/v1.2/reports'
        username = os.getenv("USDA_MARS_API_KEY")   # You need to store your API key as a User Variable in Windows or store it in .bashrc 
        headers={'username':username}   # USDA MARS API uses HTML basic auth. So you need to use this "API key", not as an API key, but
                                        # as a username
        resp = requests.get(url, auth = (username, "")) # This is the API call to the USDA MMN database

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
# get_prices
# Descriptions:
#   Downloads the price data for a commodity available in USDA database for given years . The function doesn't offer any flexibility
#   with respect to using filters, such as requesting data only related to organic produce, specific variety of the produce etc.,
#   simply because the data retreived without filters is only a few MBs. This function implements caching of data to avoid 
#   unnecessary API requests to the USDA database. This caching is opaque to the user. 
# Inputs:
#   commodity
#       The name of the commodity as it appears in the USDA database. To see a list of possible commodities, use the function 
#       get_commodities_list(). This parameter is mandatory.
#   slug_id 
#       The market identification number (string)  corresponding to the terminal market in USDA database. USDA calls this "slug_id" for  
#       unknown reason. It is a 4 digits numeric ID. To see a list of terminal makets and their corresponding slug_ids, use the function
#       get_markets_list(). This parameter is mandatory if market_name isn't specified.
#   start_year
#       Starting year as a number (and not a string) in YYYY format. This parameter is mandatory
#   end_year
#       Ending year as a number (and not a string) in YYYY format. This parameter is optional. If it is not specified, data just for the start_
#       year will be returned
#   market_name (mutually exclusive with slug_id)
#       Propreitary market name. The function get_markets_list() outputs propreitary names and their slug-ids. This is used only when 
#       slug_id isn't specified. Using slug_id may be preferred if the user wants to iterate over several markets. Using market_name
#       may be preferred for demo code. This parameter is mandatory if slug_id isn't specified.
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
def get_prices(commodity, slug_id, start_year, end_year = None, market_name=None, debug_prints=True): 
    if end_year is None:
        end_year = start_year
    date_str = "01/01/"+str(start_year)+":12/31/"+str(end_year)
    filter_dict = {'commodity':"Tomatoes", 'report_begin_date':date_str}
    filter_str = ""
    for key in filter_dict.keys():
        filter_str = filter_str+key+"="
        filter_str = filter_str+filter_dict[key]+";"
    #print(filter_str)

    url = "https://marsapi.ams.usda.gov/services/v1.2/reports"+"/"+slug_id
    username = os.getenv("USDA_MARS_API_KEY")   # You need to store your API key as a User Variable in Windows or store it in .bashrc 
    url = url + "?q="+ filter_str + "&allSections=true"
    response = requests.get(url, auth = (username, ""))

    raw_data = json.loads(response.text)

    raw_df = pd.DataFrame(raw_data[1]['results'])
    prices_df = raw_df[['report_date','slug_id','variety', 'package','item_size','properties','grade','organic','origin','low_price','high_price','unit_sales']]

    market_name = get_market_name(slug_id)
    market_name1 = "_".join(market_name.split())
    cache_filename = market_name1 + str(start_year) + ".csv"
    raw_df.to_csv('cache/'+cache_filename)
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
def get_commodities_list(slug_id, market_name):
    # Check for missing arguments and do other sanity checks
    if (slug_id is None) and (market_name is None):
        print("You must pass a slug_id or a market_name")
        return([])
    elif (slug_id is not None) and (market_name is not None):
        print("Passing both arguments is discouraged. In this case, slug_id will be used and market_name will be ignored")
    elif (slug_id is None) and (market_name is not None):
        markets_df = get_slug_id(market_name)

    # Finally we are ready to actually create the commodity list
    filename = "commodities_list"+slug_id+".csv"
    if os.path.exists(filename):
        commodities_list = pickle.load(filename)
    else:
        # Create the commodities list from a year's worth of data from the market in question. We assume that all commodities ever
        # traded in a market can be found within a year's worth of data. We use the latest full year for this purpose
        prices_df = get_prices()

    

