import threading
import requests
from bs4 import BeautifulSoup
import re
from pprint import pprint
import pandas as pd
import csv
from sqlalchemy import create_engine
import sqlite3


main_link = "https://www.halooglasi.com/"
url = "https://www.halooglasi.com/nekretnine/prodaja-kuca/beograd"
VALID_STATUSES = [200, 301, 302, 307, 404]



def extract_first_column(csv_file):
    with open(csv_file, 'r', encoding='cp437') as file:
        csv_reader = csv.reader(file)
        first_column = [row[0] for row in csv_reader]
    
    return first_column

# Example usage
csv_file_path = 'Free_Proxy_List.csv'
first_column_list = extract_first_column(csv_file_path)
proxies_list = first_column_list[1:]




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

def check_proxies(proxy, proxy_list): 
    proxy_list = get_check("https://www.halooglasi.com/", proxy, proxy_list)
    return proxy_list

def get_proxy(proxy_list, index):
    if index + 1 == len(proxy_list):
        return proxy_list[index], 0
    return proxy_list[index], index + 1

proxy_list = list()


proxy_list = proxies_list.copy()
print("PROXY_LIST_LEN "+ str(len(proxy_list)))
index = 0


header = ['grad_s', 'lokacija_s', 'mikrolokacija_s', 'ulica_t', 'GeoLocationRPT', 'broj_soba_s', 
          'kvadratura_d', 'sprat_od_s', 'tip_objekta_s',
          'Uknjizen', 'Lift', 'Garaza', 'Terasa', 'Pdv', 'izdavanje_prodaja', 'stan_kuca', 'cena_d']
def getData(extracted_data, izdavanje_prodaja, stan_kuca):
    

    pattern = r'"GeoLocationRPT":"([^"]+)"'
    match = re.search(pattern, extracted_data)

    if match:
        GeoLocationRPT = match.group(1)
        # print(value)
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

    pattern = r'"kvadratura_d":([^"]+),'
    match = re.search(pattern, extracted_data)

    if match:
        kvadratura_d = match.group(1)
        # print(value)
    else:
        kvadratura_d = None

    pattern = r'"sprat_s":"([^"]+)"'
    match = re.search(pattern, extracted_data)

    if match:
        sprat_od_s = match.group(1)
        # print(value)
    else:
        sprat_od_s = None

    pattern = r'"tip_objekta_s":"([^"]+)"'
    match = re.search(pattern, extracted_data)

    if match:
        tip_objekta_s = match.group(1)
        # print(value)
    else:
        tip_objekta_s = None

    pattern = r'"ulica_t":"([^"]+)"'
    match = re.search(pattern, extracted_data)

    if match:
        ulica_t = match.group(1)
        # print(value)
    else:
        ulica_t = None

    pattern = r'"cena_d":([^"]+),'
    match = re.search(pattern, extracted_data)

    if match:
        cena_d = match.group(1)
        # print(value)
    else:
        cena_d  = None

    pattern = r'"Uknjižen"'
    match = re.search(pattern, extracted_data)

    if match:
        Uknjizen = True
        # print(value)
    else:
        Uknjizen = False
        # print("Substring not found Uknjižen")


    pattern = r'"Lift"'
    match = re.search(pattern, extracted_data)

    if match:
        Lift = True
        # print(value)
    else:
        Lift = False
        # print("Substring not found Lift")

    pattern = r'"Garaža"'
    match = re.search(pattern, extracted_data)
    pattern1 = r'"Parking"'
    match1 = re.search(pattern, extracted_data)

    if match or match1:
        Garaza = True
        # print(value)
    else:
        Garaza = False
        # print("Substring not found Uknjižen")

    pattern = r'"Terasa"'
    match = re.search(pattern, extracted_data)
    pattern1 = r'"Lođa"'
    match1 = re.search(pattern, extracted_data)

    if match or match1:
        Terasa = True
        # print(value)
    else:
        Terasa = False
        # print("Substring not found Uknjižen")


    pattern = r'"Povraćaj PDV-a"'
    match = re.search(pattern, extracted_data)

    if match:
        Pdv = True
        # print(value)
    else:
        Pdv = False
        # print("Substring not found Lift")

    row = [
        grad_s,
        lokacija_s,
        mikrolokacija_s,
        ulica_t,
        GeoLocationRPT,
        broj_soba_s,
        kvadratura_d,
        sprat_od_s,
        tip_objekta_s,
        Uknjizen,
        Lift,
        Garaza,
        Terasa,
        Pdv,
        izdavanje_prodaja,
        stan_kuca,
        cena_d
    ]
    return row
df = pd.DataFrame(columns=header)

def MakeDataBase(main_link, url, proxy_list, file_name):
    if "izdavanje" in file_name:
        izdavanje_prodaja = "izdavanje"
    else: izdavanje_prodaja = "prodaja"
    if "stan" in file_name:
        stan_kuca = "stan"
    else: stan_kuca = "kuca"

    header = ['grad_s', 'lokacija_s', 'mikrolokacija_s', 'ulica_t', 'GeoLocationRPT', 'broj_soba_s', 
            'kvadratura_d', 'sprat_od_s', 'tip_objekta_s',
            'Uknjizen', 'Lift', 'Garaza', 'Terasa', 'Pdv', 'izdavanje_prodaja', 'stan_kuca', 'cena_d']
    df = pd.DataFrame(columns=header)

    # main_link = "https://www.halooglasi.com/"
    # url = "https://www.halooglasi.com/nekretnine/prodaja-kuca/beograd"
    response = requests.get(url)
    if response.status_code == 200:
        print("isp")
        # Proceed with parsing the HTML content
        soup = BeautifulSoup(response.content, "html.parser")
        soup_str = str(soup)
        # print(soup_str)

        pattern = r'"PageNumber":([^"]+),'
        match = re.search(pattern, soup_str)
        if match:
            begin_page = match.group(1)
            # print(value)
        else:
            begin_page = None
        pattern = r'"TotalPages":([^"]+),'
        match = re.search(pattern, soup_str)
        if match:
            end_page = match.group(1)
            # print(value)
        else:
            end_page = None
        # print(begin_page)
        # print(end_page)

        for i in range(int(begin_page), int(end_page)+1):
            # print("PAGE: " + str(i))
            url_page = url + "?page=" + str(i)
            proxy = get_proxy(proxy_list, index)
            response_page = requests.get(url_page, proxies={'http': f"http://{proxy}"}, timeout=30)
            if response_page.status_code == 200:
                soup_page = BeautifulSoup(response_page.content, "html.parser")
                nekretnine = soup_page.find_all("div", class_="real-estates")
                for j,nekretnina in enumerate(nekretnine):
                    print(url + "   PAGE: " + str(i) + "/" + str(end_page) + "     OGLASI_SERIAL_NUMBER: " + str(j))
                    naslovi = nekretnina.find_all("h3",class_="product-title")
                    support_link = naslovi[0].find('a')['href']
                    go_to_link = main_link + support_link
                    proxy = get_proxy(proxy_list, index)
                    response_end = requests.get(go_to_link, proxies={'http': f"http://{proxy}"}, timeout=30)
                    if response.status_code == 200:
                        # print("isp")
                        # Proceed with parsing the HTML content
                        soup_end = BeautifulSoup(response_end.content, "html.parser")

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
                            
                            # Create a new row
                            new_row = pd.Series(getData(extracted_data, izdavanje_prodaja, stan_kuca), index=df.columns)
                            # Add the new row to the DataFrame
                            df = df.append(new_row, ignore_index=True)
                            # print(new_row)
                        else:
                            print("No data found.")
                    else:
                        print("Error: END")
            else:
                print("Error: PAGE")

        # Connect to the database
        conn = sqlite3.connect(file_name)

        # Append DataFrame data to the existing table
        df.to_sql('mytable', conn, if_exists='append', index=False)

        # Commit the changes and close the connection
        conn.commit()
        conn.close()
            
        
        # print(len(nekretnine))
        
        # # Iteriranje kroz pronađene elemente
        # nekretnina =nekretnine[0]
        # naslovi = nekretnina.find_all("h3",class_="product-title")
        # print(type(naslovi[0]))
        # link  = naslovi[0].find("a")
    else:
        print("Error: Failed to retrieve the web page")


if __name__ =="__main__":
    main_link = "https://www.halooglasi.com/"

    # # url1 = "https://www.halooglasi.com/nekretnine/prodaja-stanova/beograd"
    # url2 = "https://www.halooglasi.com/nekretnine/prodaja-stanova/nis"
    # # url3 = "https://www.halooglasi.com/nekretnine/prodaja-stanova/novi-sad"

    # # t1 = threading.Thread(target=MakeDataBase, args=(main_link,url1,proxy_list[:200],"db_prodaja_stanova_Beograd.db"))
    # t2 = threading.Thread(target=MakeDataBase, args=(main_link,url2,proxy_list[200:350],"db_prodaja_stanova_Nis.db"))
    # # t3 = threading.Thread(target=MakeDataBase, args=(main_link,url3,proxy_list[350:],"db_prodaja_stanova_Novi_Sad.db"))

    # # t1.start()
    # # starting thread 2
    # t2.start()

    # # t3.start()
    # # wait until thread 1 is completely executed
    # # t1.join()
    # # wait until thread 2 is completely executed
    # t2.join()
    # t3.join()
    # "prodaja-stanova", "izdavanje-stanova", "izdavanje-kuca", "prodaja-kuca"
    for elem_tip in ["izdavanje-stanova"]:
        url1 = "https://www.halooglasi.com/nekretnine/" + elem_tip + "/beograd"
        url2 = "https://www.halooglasi.com/nekretnine/" + elem_tip + "/novi-sad"
        url3 = "https://www.halooglasi.com/nekretnine/" + elem_tip + "/nis"

        t1 = threading.Thread(target=MakeDataBase, args=(main_link,url1,proxy_list[200:400],"db_" + elem_tip + "_beograd.db"))
        # t2 = threading.Thread(target=MakeDataBase, args=(main_link,url2,proxy_list[200:350],"db_" + elem_tip + "_novi-sad.db"))
        # t3 = threading.Thread(target=MakeDataBase, args=(main_link,url3,proxy_list[350:],"db_" + elem_tip + "_nis.db"))
        t1.start()
        # t2.start()
        # t3.start()

        t1.join()
    # wait until thread 2 is completely executed
        # t2.join()
        # t3.join()
        # for elem_grad in ["nis", "kraljevo"]:

