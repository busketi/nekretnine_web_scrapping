import threading
import requests
from bs4 import BeautifulSoup
import re
from pprint import pprint
import pandas as pd
import csv
from sqlalchemy import create_engine
import sqlite3

# Define the main link and URL
main_link = "https://www.halooglasi.com/"
url = "https://www.halooglasi.com/nekretnine/prodaja-kuca/beograd"

# Define valid status codes for requests
VALID_STATUSES = [200, 301, 302, 307, 404]

# Function to extract the first column from a CSV file
def extract_first_column(csv_file):
    with open(csv_file, 'r', encoding='cp437') as file:
        csv_reader = csv.reader(file)
        first_column = [row[0] for row in csv_reader]
    return first_column

# Example usage
csv_file_path = 'Free_Proxy_List.csv'
first_column_list = extract_first_column(csv_file_path)
proxies_list = first_column_list[1:]

# Function to check the given proxy against the URL
def get_check(url, proxy, proxy_list):
    try:
        response = requests.get(url, proxies={'http': f"http://{proxy}"}, timeout=30)
        if response.status_code == 200:
            proxy_list.append(proxy)
            print("TRUE")
        else:
            print("FALSE")
            print(proxy)
        return proxy_list
    except Exception as e:
        print(e)
        return proxy_list

# Function to check all proxies in the list
def check_proxies(proxy, proxy_list): 
    proxy_list = get_check("https://www.halooglasi.com/", proxy, proxy_list)
    return proxy_list

# Function to get the next proxy from the list
def get_proxy(proxy_list, index):
    if index + 1 == len(proxy_list):
        return proxy_list[index], 0
    return proxy_list[index], index + 1

# Initialize the proxy list
proxy_list = list()
proxy_list = proxies_list.copy()
print("PROXY_LIST_LEN " + str(len(proxy_list)))
index = 0

# Define the header for the DataFrame
header = ['grad_s', 'lokacija_s', 'mikrolokacija_s', 'ulica_t', 'GeoLocationRPT', 'broj_soba_s',
          'kvadratura_d', 'sprat_od_s', 'tip_objekta_s',
          'Uknjizen', 'Lift', 'Garaza', 'Terasa', 'Pdv', 'izdavanje_prodaja', 'stan_kuca', 'cena_d']

# Function to extract data from the HTML content and return a row for DataFrame
def getData(extracted_data, izdavanje_prodaja, stan_kuca):
    # Extract data using regular expressions
    pattern = r'"GeoLocationRPT":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        GeoLocationRPT = match.group(1)
    else:
        GeoLocationRPT = None

    pattern = r'"broj_soba_s":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        broj_soba_s = match.group(1)
    else:
        broj_soba_s = None

    pattern = r'"grad_s":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        grad_s = match.group(1)
    else:
        grad_s = None

    pattern = r'"kvadratura_d":([^,]+)'
    match = re.search(pattern, extracted_data)
    if match:
        kvadratura_d = match.group(1)
    else:
        kvadratura_d = None

    pattern = r'"lokacija_s":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        lokacija_s = match.group(1)
    else:
        lokacija_s = None

    pattern = r'"mikrolokacija_s":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        mikrolokacija_s = match.group(1)
    else:
        mikrolokacija_s = None

    pattern = r'"sprat_od_s":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        sprat_od_s = match.group(1)
    else:
        sprat_od_s = None

    pattern = r'"tip_objekta_s":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        tip_objekta_s = match.group(1)
    else:
        tip_objekta_s = None

    pattern = r'"ulica_t":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        ulica_t = match.group(1)
    else:
        ulica_t = None

    pattern = r'"Uknjizen":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        Uknjizen = match.group(1)
    else:
        Uknjizen = None

    pattern = r'"Lift":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        Lift = match.group(1)
    else:
        Lift = None

    pattern = r'"Garaza":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        Garaza = match.group(1)
    else:
        Garaza = None

    pattern = r'"Terasa":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        Terasa = match.group(1)
    else:
        Terasa = None

    pattern = r'"Pdv":"([^"]+)"'
    match = re.search(pattern, extracted_data)
    if match:
        Pdv = match.group(1)
    else:
        Pdv = None

    # Create a row for the DataFrame
    row = [grad_s, lokacija_s, mikrolokacija_s, ulica_t, GeoLocationRPT, broj_soba_s, kvadratura_d, sprat_od_s,
           tip_objekta_s, Uknjizen, Lift, Garaza, Terasa, Pdv, izdavanje_prodaja, stan_kuca]

    return row

# Initialize an empty DataFrame
df = pd.DataFrame(columns=header)
# Metod for making database
def MakeDataBase(main_link, url, proxy_list, file_name):
    # Check if it's izdavanje or prodaja
    if "izdavanje" in file_name:
        izdavanje_prodaja = "izdavanje"
    else:
        izdavanje_prodaja = "prodaja"
    
    # Check if it's stan or kuca
    if "stan" in file_name:
        stan_kuca = "stan"
    else:
        stan_kuca = "kuca"
    
    # Define the header for the DataFrame
    header = ['grad_s', 'lokacija_s', 'mikrolokacija_s', 'ulica_t', 'GeoLocationRPT', 'broj_soba_s',
              'kvadratura_d', 'sprat_od_s', 'tip_objekta_s',
              'Uknjizen', 'Lift', 'Garaza', 'Terasa', 'Pdv', 'izdavanje_prodaja', 'stan_kuca', 'cena_d']
    
    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=header)

    # Send a GET request to the main URL
    response = requests.get(url)
    
    # Check if the response status code is 200 (OK)
    if response.status_code == 200:
        print("isp")
        # Parse the HTML content
        soup = BeautifulSoup(response.content, "html.parser")
        soup_str = str(soup)

        # Extract the begin_page and end_page values from the soup_str
        pattern = r'"PageNumber":([^"]+),'
        match = re.search(pattern, soup_str)
        if match:
            begin_page = match.group(1)
        else:
            begin_page = None
        
        pattern = r'"TotalPages":([^"]+),'
        match = re.search(pattern, soup_str)
        if match:
            end_page = match.group(1)
        else:
            end_page = None

        # Iterate over the pages
        for i in range(int(begin_page), int(end_page) + 1):
            url_page = url + "?page=" + str(i)
            
            # Get a proxy and update the index
            proxy, index = get_proxy(proxy_list, index)
            
            # Send a GET request to the page URL with the proxy
            response_page = requests.get(url_page, proxies={'http': f"http://{proxy}"}, timeout=30)
            
            # Check if the response status code is 200 (OK)
            if response_page.status_code == 200:
                # Parse the HTML content of the page
                soup_page = BeautifulSoup(response_page.content, "html.parser")
                nekretnine = soup_page.find_all("div", class_="real-estates")
                
                # Iterate over the nekretnine
                for j, nekretnina in enumerate(nekretnine):
                    print(url + "   PAGE: " + str(i) + "/" + str(end_page) + "     OGLASI_SERIAL_NUMBER: " + str(j))
                    naslovi = nekretnina.find_all("h3", class_="product-title")
                    support_link = naslovi[0].find('a')['href']
                    go_to_link = main_link + support_link
                    
                    # Get a proxy and update the index
                    proxy, index = get_proxy(proxy_list, index)
                    
                    # Send a GET request to the go_to_link URL with the proxy
                    response_end = requests.get(go_to_link, proxies={'http': f"http://{proxy}"}, timeout=30)
                    
                    # Check if the response status code is 200 (OK)
                    if response.status_code == 200:
                        # Parse the HTML content of the go_to_link
                        soup_end = BeautifulSoup(response_end.content, "html.parser")

                        # Extract the JavaScript code from the script tags
                        script_tags = soup_end.find_all("script")
                        javascript_code = ""
                        for script_tag in script_tags:
                            script_content = script_tag.string
                            if script_content:
                                javascript_code += script_content

                        # Use regular expressions to extract the desired information
                        result = re.search(r'QuidditaEnvironment\.CurrentClassified=(\{.*?\});', javascript_code)

                        if result:
                            extracted_data = result.group(1)

                            # Create a new row with the extracted data
                            new_row = pd.Series(getData(extracted_data, izdavanje_prodaja, stan_kuca), index=df.columns)
                            # Add the new row to the DataFrame
                            df = df.append(new_row, ignore_index=True)
                        else:
                            print("No data found.")
                    else:
                        print("Error: END")
            else:
                print("Error: PAGE")

        # Connect to the SQLite database
        conn = sqlite3.connect(file_name)

        # Append DataFrame data to the existing table in the database
        df.to_sql('mytable', conn, if_exists='append', index=False)

        # Commit the changes and close the connection
        conn.commit()
        conn.close()

    else:
        print("Error: Failed to retrieve the web page")


if __name__ == "__main__":
    main_link = "https://www.halooglasi.com/"

    # Iterate over the elem_tip values
    for elem_tip in ["prodaja-stanova", "izdavanje-stanova", "izdavanje-kuca", "prodaja-kuca"]:
        url1 = "https://www.halooglasi.com/nekretnine/" + elem_tip + "/beograd"
        url2 = "https://www.halooglasi.com/nekretnine/" + elem_tip + "/novi-sad"
        url3 = "https://www.halooglasi.com/nekretnine/" + elem_tip + "/nis"

        # Create threads for each location
        t1 = threading.Thread(target=MakeDataBase, args=(main_link, url1, proxy_list[:200], "db_" + elem_tip + "_beograd.db"))
        t2 = threading.Thread(target=MakeDataBase, args=(main_link, url2, proxy_list[200:350], "db_" + elem_tip + "_novi-sad.db"))
        t3 = threading.Thread(target=MakeDataBase, args=(main_link, url3, proxy_list[350:], "db_" + elem_tip + "_nis.db"))
        
        # Start the threads
        t1.start()
        t2.start()
        t3.start()

        # Wait for all threads to complete
        t1.join()
        t2.join()
        t3.join()


