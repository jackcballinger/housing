import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import time
import datetime
import glob
import os
import create_schroders_cert
import json


def timer(wait_time):
    print("waiting for timer: " + str(wait_time) + "s")
    time.sleep(wait_time)

def import_previous_file():
    list_of_files = glob.glob('C:/Users/ballinj/housing/data/rightmove/rental/*.csv')
    latest_file = max(list_of_files, key=os.path.getctime)
    combined_df_old = pd.read_csv(latest_file, index_col=0)
    return combined_df_old

def import_previous_sales_file():
    list_of_files = glob.glob('C:/Users/ballinj/housing/data/rightmove/sales/*.csv')
    latest_file = max(list_of_files, key=os.path.getctime)
    combined_sales_df_old = pd.read_csv(latest_file, index_col=0)
    return combined_sales_df_old

def get_soup(url, params=None):
    cert = "C:/Users/ballinj/housing/ca-certificates.crt"
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36'}
    r = requests.get(url, params=params, verify=cert, headers=headers)
    c = r.content    
    soup = BeautifulSoup(c, 'html.parser')
    return(soup)

def get_no_results(soup):
    no_results = soup.find('span', attrs={'class':'searchHeader-resultCount'}).text.strip()
    return no_results

def get_listing_df(no_results):
    index_array = np.arange(0,int(no_results)+24,24).tolist()
    listing_ids, links, property_types, addresses, prices_per_month, prices_per_week, featured_properties = [],[],[],[],[],[],[]
    for index in index_array:
        url = 'https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E85212&maxBedrooms=2&minBedrooms=1&maxPrice=1750&index=' + str(index) + '&includeLetAgreed=false'
        soup = get_soup(url)
        main_data = soup.find('div', attrs={'class':'main'})
        search_results = soup.find('div', attrs={'class':'l-searchResults'})
        ids = search_results.findAll('a', attrs={'class':'propertyCard-anchor'})#['id']
        for id in ids:
            listing_ids.append(id['id'][4:])

        listing_data = search_results.findAll('div', attrs={'class':'propertyCard-wrapper'})

        for listing in listing_data:
            listing.find('')
            featured_properties.append(listing.find('div', attrs={'class':'propertyCard-moreInfoFeaturedTitle'}).text.strip())

            details = listing.find('div', attrs={'class':'propertyCard-details'})
            addresses.append(listing.find('address').text.strip())
            property_types.append(listing.find('h2').text.strip())
            links.append('https://www.rightmove.co.uk/' + details.find('a')['href'])

            pricing = listing.find('div', attrs={'class':'propertyCard-price'})
            prices_per_month.append(pricing.find('div', attrs={'class':'propertyCard-rentalPrice-primary'}).text.strip())
    listing_df = pd.DataFrame(listing_ids, columns=['listing_id'])
    listing_df['address'] = addresses
    listing_df['property_type'] = property_types
    listing_df['property_link'] = links
    listing_df['price_per_month'] = prices_per_month
    listing_df['featured_property'] = featured_properties
    listing_df = listing_df[~listing_df['property_type'].str.contains('share')]
    listing_df = listing_df[~listing_df['property_type'].str.contains('Parking')]
    listing_df = listing_df[listing_df['address']!=""]
    listing_df = listing_df[listing_df['featured_property']==""]
    listing_df = listing_df.reset_index(drop=True)
    return listing_df

def get_sale_listing_df(no_results):
    index_array = np.arange(0,int(no_results)+24,24).tolist()
    listing_ids, links, property_types, addresses, prices, prices_per_week, featured_properties = [],[],[],[],[],[],[]
    added_reduced_array, letting_agent_name, letting_agent_number, num_pictures = [],[],[],[]
    for index in index_array:
        url = 'https://www.rightmove.co.uk/property-for-sale/find.html?minBedrooms=1&keywords=&sortType=6&minPrice=200000&viewType=LIST&channel=BUY&index=' + str(index) + '&maxPrice=450000&radius=0.0&locationIdentifier=REGION%5E85212'
        soup = get_soup(url)
        main_data = soup.find('div', attrs={'class':'main'})
        search_results = soup.find('div', attrs={'class':'l-searchResults'})
        ids = search_results.findAll('a', attrs={'class':'propertyCard-anchor'})#['id']
        for id in ids:
            listing_ids.append(id['id'][4:])

        listing_data = search_results.findAll('div', attrs={'class':'propertyCard-wrapper'})

        for listing in listing_data:
            featured_properties.append(listing.find('div', attrs={'class':'propertyCard-moreInfoFeaturedTitle'}).text.strip())

            details = listing.find('div', attrs={'class':'propertyCard-details'})
            addresses.append(listing.find('address').text.strip())
            property_types.append(listing.find('h2').text.strip())
            links.append('https://www.rightmove.co.uk/' + details.find('a')['href'])

            pricing = listing.find('div', attrs={'class':'propertyCard-price'})
            prices.append(pricing.find('div', attrs={'class':'propertyCard-priceValue'}).text.strip())
            added_reduced_array.append(listing.find('div', attrs={'class':'propertyCard-branchSummary'}).find('span', attrs={'class':'propertyCard-branchSummary-addedOrReduced'}).text.strip())
            estate_agent = listing.find('div', attrs={'class':'propertyCard-branchSummary'}).find('span', attrs={'class':'propertyCard-branchSummary-branchName'}).text.strip()
            estate_agent = estate_agent[estate_agent.find('by')+3:].strip()
            letting_agent_name.append(estate_agent)
            letting_agent_number.append(listing.find('div', attrs={'class':'propertyCard-contacts'}).find('a', attrs={'class':'propertyCard-contactsPhoneNumber'}).text.strip())
            meta_data = listing.find('div', attrs={'class':'propertyCard-moreInfoMeta'})
            num_pictures.append(meta_data.find('span', attrs={'class':'propertyCard-moreInfoNumber'}).text.strip())
    listing_df = pd.DataFrame(listing_ids, columns=['listing_id'])
    listing_df['address'] = addresses
    listing_df['property_type'] = property_types
    listing_df['property_link'] = links
    listing_df['price'] = prices
    listing_df['added/reduced_date'] = added_reduced_array
    listing_df['agent_name'] = letting_agent_name
    listing_df['agent_number'] = letting_agent_number
    listing_df['no_pictures'] = num_pictures
    listing_df['featured_property'] = featured_properties
    listing_df = listing_df[~listing_df['property_type'].str.contains('share')]
    listing_df = listing_df[~listing_df['property_type'].str.contains('Parking')]
    listing_df = listing_df[listing_df['address']!=""]
    listing_df = listing_df[listing_df['featured_property']==""]
    listing_df = listing_df.reset_index(drop=True)
    return listing_df

def get_property_coordinates(url, listing_df):
    latitudes, longitudes = [],[]
    map_url = url.replace('find','map')
    map_url = map_url.replace('LIST','MAP')
    soup = get_soup(map_url)
    scripts = soup.findAll('script')
    script_list = [script if 'window.jsonModel' in str(script) else '' for script in scripts]
    script_list = [script for script in script_list if script != '']
    script = str(script_list[0])
    script = script[script.find('{'):script.rfind('}')+1]
    properties_json = json.loads(script)
    property_id, coordinates = [],[]
    for row in properties_json['properties']:
        property_id.append(str(row['id']))
        coordinates.append([row['location']['latitude'], row['location']['longitude']])
    coordinates_dict = dict(zip(property_id, coordinates))
    for id in list(listing_df['listing_id']):
        latitudes.append(coordinates_dict[str(id)][0])
        longitudes.append(coordinates_dict[str(id)][1])
    listing_df['latitude'] = latitudes
    listing_df['longitude'] = longitudes
    return listing_df

def format_df(sales_listing_df):
    initial_scraped_dates = []
    sales_listing_df['property_type'] = sales_listing_df['property_type'].map(lambda x: x.rstrip(' for sale'))
    df = pd.DataFrame(sales_listing_df['property_type'].str.split('bedroom',1).tolist(), columns=['no_rooms','property_types'])
    df['property_types'] = df['property_types'].str.replace('hou','house')
    df['no_rooms'] = pd.to_numeric(df['no_rooms'])
    sales_listing_df = pd.concat([sales_listing_df, df], axis=1)
    sales_listing_df = sales_listing_df.drop(columns=['property_type'])
    sales_listing_df['added/reduced_date'] = sales_listing_df['added/reduced_date'].str.replace(' yesterday',' on ' + yesterday)
    sales_listing_df['added/reduced_date'] = sales_listing_df['added/reduced_date'].str.replace(' today',' on ' + today_2)
    df = pd.DataFrame(sales_listing_df['added/reduced_date'].str.split('on',1).tolist(), columns=['reduced/added','date_reduced/added'])
    sales_listing_df = pd.concat([sales_listing_df, df], axis=1)
    sales_listing_df = sales_listing_df[['listing_id', 'address', 'no_rooms','price','property_types','property_link','reduced/added', 'date_reduced/added',
                                         'no_pictures','latitude','longitude', 'agent_name','agent_number']]
    sales_listing_df['date_reduced/added'] = pd.to_datetime(sales_listing_df['date_reduced/added'])
    sales_listing_df['no_pictures'] = pd.to_numeric(sales_listing_df['no_pictures'])
    sales_listing_df['most_recent_scrape_date'] = datetime.datetime.today().date().strftime('%d/%m/%Y')
    most_recent_scraped_dates = today
    for index, row in sales_listing_df.iterrows():
        if int(row['listing_id']) not in sales_listing_df_old['listing_id'].tolist():
            initial_scraped_dates.append(today)
        else:
            initial_scraped_dates.append(sales_listing_df_old[sales_listing_df_old['listing_id']==int(row['listing_id'])]['initial_scrape_date'].tolist()[0])
    sales_listing_df['initial_scrape_date'] = initial_scraped_dates
    return sales_listing_df

def get_details_df(listing_df):
    dates_available, furnishings, letting_types, added_array, reduced_array, descriptions, nearest_stations_array, nearest_stations_dict = [],[],[],[],[],[],[],[]
    initial_scraped_dates, most_recent_scraped_dates, longitudes, latitudes, agent_name, agent_address, agent_numbers = [],[],[],[],[],[],[]
    for index, row in listing_df.iterrows():
        url = row['property_link']#'https://www.rightmove.co.uk/property-to-rent/property-68955643.html'
        print(url)
        print(str(index + 1) + ' out of ' + str(len(listing_df)))
        timer(2)
        #     print(url)
        soup = get_soup(url)

        listing_details = soup.find('div', attrs={'id':'primaryContent'})
        listing_primary_details = listing_details.find('div', attrs={'id':'detailsTabs'})
        description_tab = listing_primary_details.find('div', attrs={'id':'description'})

        #letting information
        letting_information = description_tab.find('tbody')
        letting_information_rows = letting_information.findAll('tr')
        letting_information_array = [row.text.strip().split(':') for row in letting_information_rows]
        for letting_row in letting_information_array:
            letting_row[1] = letting_row[1].strip()
        letting_information_dict = {item[0]: item[1] for item in letting_information_array}

        if 'Letting type' in letting_information_dict.keys():
            letting_types.append(letting_information_dict['Letting type'])
        else:
            letting_types.append(None)

        if 'Furnishing' in letting_information_dict.keys():
            furnishings.append(letting_information_dict['Furnishing'])
        else:
            furnishings.append(None)

        if 'Date available' in letting_information_dict.keys():
            dates_available.append(letting_information_dict['Date available'])
        else:
            dates_available.append(None)

        if 'Added on Rightmove' in letting_information_dict.keys():
            added_array.append(letting_information_dict['Added on Rightmove'])
        else:
            added_array.append(None)

        if 'Reduced on Rightmove' in letting_information_dict.keys():
            reduced_array.append(letting_information_dict['Reduced on Rightmove'])
        else:
            reduced_array.append(None)

        # description
        descriptions.append(description_tab.find('p', attrs={'itemprop':'description'}).text.strip().replace('''\r''',''))

        # nearest stations
        try:
            nearest_stations = soup.find('ul', attrs={'class':'stations-list'})
            station_rows = nearest_stations.findAll('li')
            station_array = [row.text.strip().split('\n') for row in station_rows]
            station_dict = {item[0]: item[1] for item in station_array}
            nearest_stations_array.append(station_array)
            nearest_stations_dict.append(station_dict)
        except AttributeError:
            nearest_stations_array.append([None])
            nearest_stations_dict.append({None})
        
        maps_section = soup.find('a', attrs={'class':'block js-tab-trigger js-ga-minimap'})
        long_lat_href = maps_section.find('img')['src']
        latitudes.append(str(long_lat_href)[str(long_lat_href).find('latitude') + len('latitude='):str(long_lat_href).find('&')])
        longitudes.append(str(long_lat_href)[str(long_lat_href).find('longitude') + len('longitude='):str(long_lat_href).find('&z')])
        
        most_recent_scraped_dates = today
        if int(row['listing_id']) not in combined_df_old['listing_id'].tolist():
            initial_scraped_dates.append(today)
        else:
            initial_scraped_dates.append(combined_df_old[combined_df_old['listing_id']==int(row['listing_id'])]['initial_scrape_date'].tolist()[0])
        
        secondary_details = soup.find('div', attrs={'id':'secondaryAgentDetails'})
        request_details = soup.find('div', attrs={'id':'requestdetails'})
        agent_details = secondary_details.find('div', attrs={'class':'agent-details-display'}).find('div', attrs={'class':'overflow-hidden'}).text.strip()
        agent_details = agent_details.split('\n')
        agent_details = [row.replace('\r',"").strip() for row in agent_details]
        agent_name.append(agent_details[0])
        agent_address.append(", ".join(agent_details[1:]))
        try:
            agent_number = request_details.find('p').find('a')['href']
            agent_number = agent_number[agent_number.find(':')+1:].strip()
            agent_numbers.append(agent_number)
        except AttributeError:
            agent_numbers.append('na')
        
    details_df = pd.DataFrame(dates_available, columns=['dates_available'])
    details_df['furnishing'] = furnishings
    details_df['letting_type'] = letting_types
    details_df['letting_agent'] = agent_name
    details_df['letting_agent_address'] = agent_address
    details_df['letting_agent_number'] = agent_numbers
    details_df['date_added'] = added_array
    details_df['date_reduced'] = reduced_array
    details_df['description'] = descriptions
    details_df['longitude'] = longitudes
    details_df['latitude'] = latitudes
    details_df['initial_scrape_date'] = initial_scraped_dates
    details_df['most_recent_scrape_date'] = most_recent_scraped_dates
    return details_df, nearest_stations_array, nearest_stations_dict

def get_station_df(nearest_stations_array, nearest_stations_dict, all_stations_manual):
    station_array = []
    for property in nearest_stations_array:
        station_array_temp_mid = []
        for row in property:
            station_array_temp_small = []
            for station in all_stations_manual:
                try:
                    if row[0] == station:
                        station_array_temp_small.append(row[0])
                    else:
                        station_array_temp_small.append(None)
                except:
                    continue
            station_array_temp_mid.append(station_array_temp_small)
        station_array.append(station_array_temp_mid)

    stations_sparse_array = []
    for location in station_array:
        combined_station_array = []
        for a,b,c in zip(location[0],location[1],location[2]):
            if a != None:
                combined_station_array.append(a)
            elif b != None:
                combined_station_array.append(b)
            elif c != None:
                combined_station_array.append(c)
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
    stations_df = pd.DataFrame(distances_sparse_array, columns=all_stations_manual)
    return stations_df

def update_stations(all_stations_list, listing_df):
    for index, row in listing_df.iterrows():
        url = row['property_link']
        print(url)
        print(str(index + 1) + ' out of ' + str(len(listing_df)))
        timer(2)
        soup = get_soup(url)   
        nearest_stations = soup.find('ul', attrs={'class':'stations-list'})
        station_rows = nearest_stations.findAll('li')
        station_array = [row.text.strip().split('\n') for row in station_rows]
        for item in station_array:
            if item[0] not in all_stations:
                all_stations.append(item[0])
    return all_stations

today = datetime.datetime.today().strftime("%d-%m-%Y")
yesterday = datetime.datetime.today().date() - datetime.timedelta(1)
yesterday = yesterday.strftime('%d/%m/%Y')
today_2 = datetime.datetime.today().date()
today_2 = today_2.strftime('%d/%m/%Y')

soup = get_soup('https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E85212&maxBedrooms=2&minBedrooms=1&maxPrice=1750&propertyTypes=detached%2Csemi-detached%2Cterraced%2Cflat&secondaryDisplayPropertyType=housesandflats&includeLetAgreed=false')
combined_df_old = import_previous_file()
no_results = get_no_results(soup)
listing_df = get_listing_df(no_results)
details_df, nearest_stations_array, nearest_stations_dict = get_details_df(listing_df)

all_stations_manual = ['Bermondsey',
                       'Borough',
                       'Canada Water',
                       'Elephant & Castle',
                       'Elephant & Castle (Bakerloo)',
                       'Elephant & Castle (Northern)',
                       'London Bridge',
                       'Monument',
                       'Queens Road Peckham',
                       'Rotherhithe',
                       'South Bermondsey',
                       'Southwark',
                       'Surrey Quays',
                       'Tower Gateway',
                       'Tower Hill',
                       'Wapping']

station_df = get_station_df(nearest_stations_array, nearest_stations_dict, all_stations_manual)
combined_df = pd.concat([listing_df, details_df, station_df], axis=1)
for index, row in combined_df_old.iterrows():
    if int(row['listing_id']) not in [int(id) for id in combined_df['listing_id'].tolist()]:
        combined_df = combined_df.append(row)
combined_df = combined_df.reset_index(drop=True)
combined_df.to_csv('C:/Users/ballinj/housing/data/rightmove/rental/housing_data_{}.csv'.format(today))
print('rental data done')

# for sale properties
print('working on sale data')
url = 'https://www.rightmove.co.uk/property-for-sale/find.html?minBedrooms=1&keywords=&sortType=6&minPrice=200000&viewType=LIST&channel=BUY&index=0&maxPrice=450000&radius=0.0&locationIdentifier=REGION%5E85212'
soup = get_soup(url)
sales_listing_df_old = import_previous_sales_file()
no_results = get_no_results(soup)
sales_listing_df = get_sale_listing_df(no_results)
sales_listing_df = get_property_coordinates(url, sales_listing_df)
sales_listing_df = format_df(sales_listing_df)
for index, row in sales_listing_df_old.iterrows():
    if int(row['listing_id']) not in [int(id) for id in sales_listing_df['listing_id'].tolist()]:
        sales_listing_df = sales_listing_df.append(row)
sales_listing_df = sales_listing_df.reset_index(drop=True)
sales_listing_df.to_csv('C:/Users/ballinj/housing/data/rightmove/sales/housing_data_{}.csv'.format(today))
print('sale data done')