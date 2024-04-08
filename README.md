# Overview
This is a collection of code for analysing produce prices. At the moment, the repo is centered around USDA Agricultural Market Service's (AMS) database of produce prices. 

# AMS database basics
The AMS database is a collection of reports filed by different wholesale markets around the U.S. and some select global cities. The reports are sometimes referred to as *My Market News* (MMN) reports. The APIs used for getting data from the MMN database are collectively referred to as *MARS*. 

The data is free for use. But one needs to obtain an account in order to use the APIs. Here are the steps to follow:
  1. Go to the [MMN](https://mymarketnews.ams.usda.gov/) homepage and sign up
  2. Login into your account. At the top you will see your name. Click on it and go to *My Profile* and you will find the API key there to use with the MARS API
  3. MARS API uses Basic HTTP Auth method for authentication. So this API Key is used as the username for authentication (and not as an API key)
  4. To use the functions in this repo, you need to store the API key as an environment variable. In Linux, you can export it to your .bashrc. In windows, go to *advanced system settings*, click on *Environment variables* and paste the API key into the *Variable Value* and use USDA_MARS_API_KEY as the *Variable name*

# Repo structure and design
The following are the most important files:
  1. *usda_mmn_utils.py* - This has all the utility functions that interface with the MARS API. The idea of these functions is to make the odd peculiarities related to using the MARS API opaque to the user and also avoid unnecessary replication of code 
  2. *usda_mmn_demo.py* - This is the demo code that shows the intended way to use the utility functions

One of the main features of the repo is caching: There seem to be daily access limits and the access to the data sometimes takes a long time. Since the price data from the past isn't going to change, we cache any data we access using the *usda_mmn_demo.py* locally in the *cache* folder the very first time the data is accessed. If the same data is reqested again by the user, the functions in *usda_mmn_demo.py* retrieve the data from the cache instead of sending an API query to the USDA database. Note that the cached files are not checked into the repo (.gitignore ignores cache files) - so the folder in the repo will always be empty. As every user of the repo starts using the code, their cache folders get populated. 

# Useful links
  1. [MMN homepage](https://mymarketnews.ams.usda.gov/)
  2. A [webinar](https://mymarketnews.ams.usda.gov/sites/default/files/resources/2018-03/MARSAPIWebinar.pdf) explaining the inner workings of the MARS API
  3. [MMN FAQ](https://www.marketnews.usda.gov/mnp/fv-help-03) where one can understand the various terms used in the MMN reports