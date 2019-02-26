import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import time
from datetime import datetime
import glob
import os
# import create_schroders_cert

def timer(wait_time):
    print("waiting for timer: " + str(wait_time) + "s")
    time.sleep(wait_time)

def import_previous_file():
    list_of_files = glob.glob('C:/Users/ballinj/housing/data/zoopla/rental/*.csv')
    latest_file = max(list_of_files, key=os.path.getctime)
    combined_df_old = pd.read_csv(latest_file, index_col=0)
    return combined_df_old 
    
def get_soup(url, payload):
    cert = "C:/Users/ballinj/housing/ca-certificates.crt"
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36'}
    r = requests.get(url, verify=cert, headers=headers, params=payload)
    c = r.content    
    soup = BeautifulSoup(c, 'html.parser')
    return(soup)

def get_no_pages(soup):
    try:
        page_box = soup.find('div', attrs={'class':'paginate bg-muted'})
        no_pages = page_box.findAll('a')
        no_pages = int([page.text.strip() for page in no_pages][:-1][-1])
    except AttributeError:
        no_pages = 1
    return no_pages

def get_no_results(soup):
    no_results = int(soup.find('span', attrs={'class':'listing-results-utils-count'}).text.strip().split(' ')[-1])
    return no_results

def get_listing_df(no_pages):
    index_array = np.arange(1,no_pages+1).tolist()
    listing_ids, links, property_types, addresses, prices_per_month, prices_per_week, featured_properties = [],[],[],[],[],[],[]
    nearest_station_array, nearest_station_dict = [],[]
    for index in index_array:
        url = 'https://www.zoopla.co.uk/to-rent/property/bermondsey/?'
        payload = {'beds_min':'1',
                   'include_shared_accommodation':'false',
                   'price_frequency':'per_month',
                   'price_max':'1750',
                   'q':'Bermondsey, London',
                   'results_sort':'newest_listings',
                   'search_source':'home',
                   'page_size':'100',
                   'pn': index}
        soup = get_soup(url, payload)
        search_results = soup.find('ul', attrs={'class':'listing-results clearfix js-gtm-list'})
        property_boxes = search_results.findAll('div', attrs={'class':'listing-results-wrapper'})
        for property_box in property_boxes:
            listing_ids.append(property_box.parent['data-listing-id'].strip())
            links.append('https://www.zoopla.co.uk' + str(property_box.find('a')['href']))
            addresses.append(property_box.find('a', attrs={'class':'listing-results-address'}).text)
            property_types.append(property_box.find('h2',attrs={'class':'listing-results-attr'}).find('a').text.strip())
            prices = property_box.find('a', attrs={'class':'listing-results-price text-price'}).text.strip()
            prices_per_month.append(prices.split('(')[0].strip())
            prices_per_week.append(prices.split('(')[1].replace(')',"").strip())
            if property_box.parent['class'][2] != "":
                featured_properties.append(property_box.parent['class'][2].strip())
            else:
                featured_properties.append("")
            station_box = property_box.find('div', attrs={'class':'nearby_stations_schools clearfix'})
            stations = station_box.findAll('li')
            property_station_array = []
            for station in stations:
                single_station_array = []
                single_station_array.append(station.find('span', attrs={'class':'nearby_stations_schools_name'})['title'])
                single_station_array.append(station.text.strip().split('   ')[-1])
                property_station_array.append(single_station_array)
            nearest_station_array.append(property_station_array)

            station_dict = {item[0]: item[1] for item in property_station_array}
            nearest_station_dict.append(station_dict)
        
    listing_df = pd.DataFrame(listing_ids, columns=['listing_id'])
    listing_df['address'] = addresses
    listing_df['property_type'] = property_types
    listing_df['property_link'] = links
    listing_df['price_per_month'] = prices_per_month
    listing_df['featured_property'] = featured_properties
    listing_df = listing_df.reset_index(drop=True)
    return listing_df, nearest_station_array, nearest_station_dict

def get_details_df(listing_df):
    dates_available, furnishings, descriptions, initial_scraped_dates, most_recent_scraped_dates, longitudes, latitudes = [],[],[],[],[],[],[]
    agent_name, agent_address, agent_number = [],[],[]
    for index, row in listing_df.iterrows():
        url = row['property_link']
        print(url)
        print(str(index + 1) + ' out of ' + str(len(listing_df)))
        timer(2)
        soup = get_soup(url,payload="")
        page_info = soup.find('div', attrs={'class':'dp-tabs'})
        property_details_tab = page_info.find('section', attrs={'id':'property-details-tab'})
        features = property_details_tab.find('ul', attrs={'class':'dp-features-list ui-list-icons'})
        script = str(soup.findAll('script', attrs={'type':'application/ld+json'})[-1].text)

        try:
            furnishings.append(features.find('svg', attrs={'class':'ui-icon icon-chair'}).parent.find('span').text.strip())
        except AttributeError:
            furnishings.append(None)

        try:
            dates_available.append(features.find('svg', attrs={'class':'ui-icon icon-calendar'}).parent.find('span').text.split('from')[1].strip())
        except AttributeError:
            dates_available.append(None)

        try:
            descriptions.append(property_details_tab.find('div', attrs={'class':'dp-description__text'}).text.strip())
        except AttributeError:
            descriptions.append(None)

        try:
            latitudes.append(script[script.find('latitude')+12:script[script.find('latitude')+12:].find(",")+script.find('latitude')+11])
        except AttributeError:
            latitudes.append(None)

        try:
            longitudes.append(script[script.find('longitude')+13:script[script.find('longitude')+14:].find('"')+script.find('longitude')+14])
        except AttributeError:
            longitudes.append(None)
        
        most_recent_scraped_dates = today
        if int(row['listing_id']) not in combined_df_old['listing_id'].tolist():
            initial_scraped_dates.append(today)
        else:
            initial_scraped_dates.append(combined_df_old[combined_df_old['listing_id']==int(row['listing_id'])]['initial_scrape_date'].tolist()[0])
        

        secondary_details = soup.find('div', attrs={'class':'dp-sidebar-wrapper'}).find('div', attrs={'class':'dp-sidebar-wrapper__contact'})
        agent_details = secondary_details.find('div', attrs={'class':'ui-agent__text'})
        agent_name.append(agent_details.find('h4', attrs={'class':'ui-agent__name'}).text.strip())
        agent_address.append(agent_details.find('address',attrs={'class':'ui-agent__address'}).text.strip())
        
        tel_field = secondary_details.find('p', attrs={'class':'ui-agent__tel ui-agent__text'}).find('a')['href']
        agent_number.append(tel_field[tel_field.find(':')+1:].strip())
        
    details_df = pd.DataFrame(dates_available, columns=['dates_available'])
    details_df['letting_agent'] = agent_name
    details_df['letting_agent_address'] = agent_address
    details_df['letting_agent_number'] = agent_number
    details_df['furnishing'] = furnishings
    details_df['description'] = descriptions
    details_df['longitude'] = longitudes
    details_df['latitude'] = latitudes
    details_df['initial_scrape_date'] = initial_scraped_dates
    details_df['most_recent_scrape_date'] = most_recent_scraped_dates
    return details_df

def get_updated_stations(no_pages):
    index_array = np.arange(1,no_pages+1).tolist()
    total_stations = []
    for index in index_array:
        url = 'https://www.zoopla.co.uk/to-rent/property/bermondsey/?'
        payload = {'beds_min':'1',
                   'include_shared_accommodation':'false',
                   'price_frequency':'per_month',
                   'price_max':'1750',
                   'q':'Bermondsey, London',
                   'results_sort':'newest_listings',
                   'search_source':'home',
                   'page_size':'100',
                   'pn': index}
        soup = get_soup(url, payload)
        search_results = soup.find('ul', attrs={'class':'listing-results clearfix js-gtm-list'})
        property_boxes = search_results.findAll('div', attrs={'class':'listing-results-wrapper'})
        for property_box in property_boxes:
            station_box = property_box.find('div', attrs={'class':'nearby_stations_schools clearfix'})
            stations = station_box.findAll('li')
            for station in stations:
                total_stations.append(station.find('span')['title'])
    total_stations = pd.Series(total_stations)
    total_stations = pd.Series(total_stations.unique()).sort_values()
    return total_stations

def get_station_df(nearest_stations_array, nearest_stations_dict, stations_array):
    station_array = []
    for property in nearest_stations_array:
        station_array_temp_mid = []
        for row in property:
            station_array_temp_small = []
            for station in stations_array:
                try:
                    if row[0] == station:
                        station_array_temp_small.append(row[0])
                    else:
                        station_array_temp_small.append(None)
                except:
                    continue
            station_array_temp_mid.append(station_array_temp_small)
        station_array.append(station_array_temp_mid)

    station_array

    stations_sparse_array = []
    for location in station_array:
        combined_station_array = []
        for a,b in zip(location[0],location[1]):
            if a != None:
                combined_station_array.append(a)
            elif b != None:
                combined_station_array.append(b)
            else:
                combined_station_array.append(None)
        stations_sparse_array.append(combined_station_array)

    distances_sparse_array = []
    dict_counter = 0
    for location in stations_sparse_array:
        distances = []
        for datapoint in location:
            if datapoint in nearest_stations_dict[dict_counter].keys():
                distances.append(nearest_stations_dict[dict_counter][datapoint].replace('(',"").replace(')',""))
            else:
                distances.append(None)
        distances_sparse_array.append(distances)
        dict_counter = dict_counter + 1
    stations_df = pd.DataFrame(distances_sparse_array, columns=stations_array)
    return stations_df

today = datetime.today().strftime("%d-%m-%Y")

url = 'https://www.zoopla.co.uk/to-rent/property/bermondsey/?beds_min=1&include_shared_accommodation=false&price_frequency=per_month&price_max=1750&q=Bermondsey%2C%20London&results_sort=newest_listings&search_source=home&page_size=100'
soup = get_soup(url, payload="")
combined_df_old = import_previous_file()
no_pages = get_no_pages(soup)
listing_df, nearest_stations_array, nearest_stations_dict = get_listing_df(no_pages)
stations_array = get_updated_stations(no_pages)
station_df = get_station_df(nearest_stations_array, nearest_stations_dict, stations_array)
details_df = get_details_df(listing_df)
combined_df = pd.concat([listing_df, details_df, station_df], axis=1)
for index, row in combined_df_old.iterrows():
    if int(row['listing_id']) not in [int(id) for id in combined_df['listing_id'].tolist()]:
        combined_df = combined_df.append(row)
combined_df = combined_df.reset_index(drop=True)
combined_df.to_csv('C:/Users/ballinj/housing/data/zoopla/rental/housing_data_{}.csv'.format(today))