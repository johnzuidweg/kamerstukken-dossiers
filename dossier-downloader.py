#!/usr/bin/python3
# Enumerates all Kamerstukken for dossier numbers specified in CONFIG_FILE (json)
# Uses https://repository.overheid.nl (REP) for initial download
# Retrieves metadata, including references to appendices from  metadata.xlm using https://zoek.officielebekendmakingen.nl (OB), because appendices are not available on REP.
# Retrieves Staatsblad-publicaties (using OB) and add results to result_set when dossier number in metadata == target dossier number (DOSSIER_NR)
# Optional (by specifing OB_ZOEKTERMEN): searches OB for specified search terms and add results to result_set when dossier number in metadata == target dossier number (DOSSIER_NR)
# Downloads all Staatsblad publications, Kamerstukken and their appendices ('bijlagen') from the result_set using OB
#
# stores result_set in pickle
# Stores result in HTML file
# Logs to logfile
#
# Reasons for using REP instead of OB to enumerate Kamerstukken:
# - found example of Kamerstuk that is missing in OB search results, but is shown in REP:
#   https://repository.overheid.nl/frbr/officielepublicaties/kst/25124/kst-25124-84/1/pdf/kst-25124-84.pdf
# - RSS of OB is limited to 150 results; no way of getting results > 150 in RSS
#   So, the only way of processing large result sets in OB is by parsing HTML output (no so robust as HTML output might easily be changed in the future)
#
# Also, this script enumerates all dossier_numbers and tries to find their names, number of items and the date of the last added item
# It uses REP for initial enumeration
# And OB-search results (all Kamerstukken since last run) for incremental updates
# Outputs to <date>-dossieroverzicht.csv
#
# Tried asyncio and asynhttp to speed things up (by sending parallel / non-blocking requests)
# Did not work out as the server start to send RESET messages when to many requests are send within a short amount of time
# Restricting the amount of requests per second (using AsyncLimiter) still did result in blocks sometimes (and resulted in heavy processer load at client)
# 
# Even sequential (blocking) requests using the Requests module sometimes result in blocks (5xx response codes)
# The script uses Retry (from requests.packages.urllib3.util.retry) to mitigate this issue
#
from bs4 import BeautifulSoup
import requests, os, math, datetime, logging, pickle, time, json, telegram, csv, py7zr
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from zoneinfo import ZoneInfo
# install telegram with pip3 install python-telegram-bot

# Telegram
BOT = # Fill in!
CHAT_ID = # Fill in!

OB_URL = "https://zoek.officielebekendmakingen.nl/"
REP_URL = "https://repository.overheid.nl/frbr/officielepublicaties/kst/"
PWD = # Fill in!
RESULTSDIR = f"{PWD}results/"
CONFIG_FILE = f"{PWD}dossiernummers-en-zoektermen.json"
OB_DATE_STRING = "%a, %d %b %Y %H:%M:%S %z"
LOG_FILE = f"{PWD}dossier-downloader.log"
STUKKEN_PICKLE_FILE = f"{PWD}dossier-stukken.bin"
INFO_PICKLE_FILE = f"{PWD}dossier-info.bin"
MAX_NUM_PER_PAGE = 1000 # max number of results per page (for OB)

LINE_CLEAR = '\x1b[2K' # <-- ANSI sequence to clear the line when using print(string, end='\r') to print multiple strings on the same line (by overwriting the previous string)

# for making 'requests' more robust
retry_strategy = Retry(
    total=8,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0'})
http.mount("https://", adapter)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

class Dossier_info():
    def __init__(self, nr):
        self.nr = nr
        self.title = None
        self.num_items = None
        self.last_date = None

    @property
    def last_date_string(self):
        if self.last_date:
            return self.last_date.strftime("%Y-%m-%d")
        else:
            return None

    def get_result_list(self):
        return [self.nr, self.last_date_string, self.num_items, self.title]

class Dossier:
    def __init__(self, nr, search_terms):
        self.nr = nr
        self.search_terms = search_terms
        self.obs = set()

    def add_rep_kamerstukken(self):
        start = 1
        total = 2
        all_works = set()
        while start < total:
            response = http.get(f"{REP_URL}{self.nr}/?start={start}&format=xml")
            response.encoding = 'UTF-8' # to fix decoding issues
            soup = BeautifulSoup(response.text, 'xml')
            subareas = soup.find_all("subarea")
            pagesize = int(subareas[-1]["pagesize"])
            total = int(subareas[-1]["total"])
            #print(f"{math.ceil(start/pagesize)-1}/{math.ceil(total/pagesize)} lijsten", end = "\r")
            works = soup.find_all("work")
            for work in works:
                all_works.add(str(work.text))
            start = start + pagesize
            #start = total # TEMP, to make it fast for testing
        #print(f"{math.ceil(start/pagesize)-1}/{math.ceil(total/pagesize)} lijsten")
        logging.info(f"{math.ceil(start/pagesize)-1}/{math.ceil(total/pagesize)} lijsten")
        
        #print(end=LINE_CLEAR)
        #print(f"Processing {len(all_works)} links to Kamerstukken")
        
        for i, work in enumerate(all_works):
            kst = Kamerstuk()
            kst.add_info(f"{REP_URL}{self.nr}/{work}/1/metadata/metadata.xml")
            if kst.date_str:
                self.obs.add(kst)

        #print(end=LINE_CLEAR)
        #print(f"Retrieved {len(self.obs)} Kamerstuk(ken) for dossier {self.nr} from {REP_URL}")

    def write_html(self):
        if len(self.obs) == 0:
            #print(f"No Kamerstukken or Staatsblad publications found for dossier {self.nr}")
            logging.warning(f"No Kamerstukken or Staatsblad publications found for dossier {self.nr}")
        else:
            html = """
                <!DOCTYPE html>
                <html>
                <head>"""
            html += f"<title>{self.nr}</title>"
            html += """
                    <style>
                    table {
                      font-family: verdana, sans-serif;
                      border-collapse: collapse;
                      font-size: 10px;
                    }

                    td, th {
                      border: 1px solid #dddddd;
                      text-align: left;
                      padding: 8px;
                    }

                    tr:nth-child(even) {
                      background-color: #dddddd;
                    }
                </style>
                </head>
                <body>
                <table>
                    <thead>
                        <tr>
                            <th>Datum</th>
                            <th>Stuk</th>
                            <th>Vergaderjaar</th>
                            <th>Organisatie</th>
                            <th>Titel</th>
                            <th>Bijlage(n)</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            obs_list = list(self.obs)
            obs_list.sort(key=lambda x: x.date_str, reverse=True)
            for obs in obs_list:
                html += '\n'.join(obs.get_html_list())
            html +="""
                    </tbody>
                </table>
                </body>
                </html>
            """

            with open(f'{RESULTSDIR}{self.nr}/contents.html', 'w') as f:
                f.write(html)

class Bekendmaking:# super-class for Kamerstuk and Stb_pub

    def __init__(self, metadata_link):
        self.nr = None
        self.title = None
        self.date_str = None
        self.vergaderjaar = None
        self.organisatie = None
        self.bijlagen_dict = {}

    def __hash__(self):
        return hash(self.nr)

    def __eq__(self, other):
        return self.nr == other.nr

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_get_bijlage_html(self):
        result_list = []
        for bijlage_nr, bijlage_title  in self.bijlagen_dict.items():
            result_list.append(f"<a href={self.date_str}-{self.nr}-{bijlage_nr}.pdf>{bijlage_nr}</a>:{bijlage_title}")
        return "<br/>".join(result_list)
    
    def get_html_list(self):
    
        return [
            "<tr>",
            f"<td>{self.date_str}</td>",
            f"<td><a href={self.date_str}-{self.nr}.pdf>{self.nr}.pdf</a></td>",
            f"<td>{self.vergaderjaar}</td>",
            f"<td>{self.organisatie}</td>",
            f"<td>{self.title}</td>",
            f"<td>{self.get_get_bijlage_html()}</td>",
            "</tr>"
        ]
        
    def dowload_all(self, dossier_nr):
        self.download_file(self.nr, f"{self.date_str}-{self.nr}.pdf", dossier_nr)
        for bijlage_nr in self.bijlagen_dict.keys():
            self.download_file(bijlage_nr, f"{self.date_str}-{self.nr}-{bijlage_nr}.pdf", dossier_nr)
    
    def download_file(self, nr, filename, dossier_nr):
        response = http.get(f"{OB_URL}{nr}.pdf")
        with open(f"{RESULTSDIR}{dossier_nr}/{filename}", 'wb') as f:
            f.write(response.content)

class Kamerstuk(Bekendmaking):

    def __init__(self):
        self.nr = None
        self.title = None
        self.date_str = None
        self.vergaderjaar = None
        self.organisatie = None
        self.bijlagen_dict = {}
        self.dossier_nr_string = None
        self.ondernummer_string = None
        
        #print(end=LINE_CLEAR)
        #print(f"Processing {metadata_link}", end = "\r")

    def add_info(self, metadata_link):
        #response = http.get(urllib.parse.unquote(metadata_link)) # urllib.parse.unquote to fix strange utf-8 issues...
        response = http.get(metadata_link)
        #response.encoding = 'UTF-8' # to fix decoding issues; did not work out here, used response.content instead...
        soup = BeautifulSoup(response.content, 'xml')
        try:
            doc_type = soup.find("metadata", {"scheme" : "OVERHEIDop.Parlementair"})['content']
        except TypeError:
            #print(f"No DC.type found in {metadata_link}")
            logging.warning(f"No OVERHEIDop.Parlementair found in {metadata_link}")
        else:
            if doc_type != "Kamerstuk": # only proceed when doc_type is Kamerstuk
                if doc_type != "Bijlage":
                    #print(end=LINE_CLEAR)
                    #print(f"{metadata_link}: DC.type is {type}")
                    logging.warning(f"{metadata_link}: DC.type is {type}")
            #if doc_type == "Kamerstuk": # only proceed when doc_type is Kamerstuk
            else: 
                self.nr = metadata_link.split("/")[-2] # works when metadata_link refers to {OB_URL}:
                if self.nr == "metadata": # apparently, metadata_link refers to {REP_URL}:
                    self.nr = metadata_link.split("/")[-4]
                try:
                    self.ondernummer_string = soup.find("metadata", {"name" : "OVERHEIDop.ondernummer"})['content']
                except TypeError:
                    self.ondernummer_string = self.nr.split("-")[-1]
                try:
                    self.dossier_nr_string = soup.find("metadata", {"name" : "OVERHEIDop.dossiernummer"})['content']
                except TypeError:
                    #print(f"No OVERHEIDop.dossiernummer found in {metadata_link}")
                    logging.warning(f"No OVERHEIDop.dossiernummer found in {metadata_link}")
                else:
                    self.date_str = soup.find("metadata", {"name" : "DCTERMS.available"})['content']
                    try:
                        self.title = soup.find("metadata", {"name" : "OVERHEIDop.documenttitel"})['content']
                    except TypeError:
                        self.title = soup.find("metadata", {"name" : "DC.title"})['content'].split(";")[-1].strip()
                    self.vergaderjaar = soup.find("metadata", {"name" : "OVERHEIDop.vergaderjaar"})['content']
                    self.organisatie = soup.find("metadata", {"name" : "DC.creator"})['content']

        # try to add regular bijlagen
        try:
            bijlagen = soup.find_all("metadata", {"name" : "OVERHEIDop.bijlage"})
            for bijlage in bijlagen:
                if not bijlage['content'] in self.bijlagen_dict.keys():
                    self.bijlagen_dict[bijlage['content']] = None
        except AttributeError:
            pass
            
        # try to add old bijlagen
        try:
            bijlagen = soup.find_all("metadata", {"name" : "DCTERMS.relation"})
            for bijlage in bijlagen:
                bijlage_nr = bijlage['content'].split(";")[-1].strip()
                if not bijlage_nr in self.bijlagen_dict.keys():
                    self.bijlagen_dict[bijlage_nr] = None
        except AttributeError:
            pass

        # try to add replacement Kamerstukken (-h1) as bijlagen
        try:
            bijlagen = soup.find_all("metadata", {"name" : "DCTERMS.isReplacedBy"})
            for bijlage in bijlagen:
                bijlage_nr = bijlage['content'].split(";")[-1].strip()
                if not bijlage_nr in self.bijlagen_dict.keys():
                    self.bijlagen_dict[bijlage_nr] = None
        except AttributeError:
            pass

    def add_bijlagen_titles(self):
        for bijlage_nr in self.bijlagen_dict.keys():
            if self.bijlagen_dict[bijlage_nr] == None:
                try:
                    bijlage_response = http.get(f"{OB_URL}{bijlage_nr}/metadata.xml")
                    bijlage_response.encoding = 'UTF-8' # to fix decoding issues
                    bijlage_soup = BeautifulSoup(bijlage_response.text, 'xml')
                    bijlage_title = bijlage_soup.find("metadata", {"name" : "DC.title"})['content']
                    self.bijlagen_dict[bijlage_nr] = bijlage_title
                except TypeError:
                    logging.warning(f"Geen DC.title voor bijlage in {OB_URL}{bijlage_nr}/metadata.xml")

    @property
    def date(self):
        if self.date_str:
            date = datetime.datetime.strptime(self.date_str, "%Y-%m-%d").replace(tzinfo=ZoneInfo('localtime'))
            #print(f"{self.date_str} ==> {date.isoformat()}")
            return date
        else:
            return None

class Stb_pub(Bekendmaking):

    def __init__(self):
        self.nr = None
        self.title = None
        self.date_str = None
        self.vergaderjaar = None
        self.organisatie = None
        self.bijlagen_dict = {}
        self.dossier_links = []
        
        #print(end=LINE_CLEAR)
        #print(f"Processing {metadata_link}", end = "\r")

    def add_info(self, metadata_link):
        reponse = http.get(metadata_link)
        soup = BeautifulSoup(reponse.content, 'xml')
        dossier_links_list = soup.find_all("metadata", {"name" : "OVERHEIDop.behandeldDossier"})
        for dossier_link in dossier_links_list:
            self.dossier_links.append(dossier_link['content'])
        self.nr = metadata_link.split("/")[-2]
        try:
            self.title = soup.find("metadata", {"name" : "DC.title"})['content']
            self.date_str = soup.find("metadata", {"name" : "DCTERMS.available"})['content']
            self.organisatie = soup.find("metadata", {"name" : "DC.creator"})['content']
        except TypeError:
            pass

def get_new_ksts(from_date, search_term, dossier_nr):
    new_ksts = set()
    bijlagen_dict = {}
    base_url = f"{OB_URL}resultaten?q=(c.product-area==\"officielepublicaties\")and(w.publicatienaam==\"Kamerstuk\")"
    if from_date:
        base_url = f"{base_url}and(dt.available>=\"{from_date.strftime('%Y-%m-%d')}\")"
    elif search_term:
        base_url = f"{base_url}and(cql.textAndIndexes=\"{search_term}\")"
    elif dossier_nr:
        base_url = f"{base_url}and(w.dossiernummer==\"{dossier_nr}\")"
    pag_num = 1
    max_page = 1
    while pag_num <= max_page:
        response = http.get(f"{base_url}&pg={MAX_NUM_PER_PAGE}&pagina={pag_num}")
        response.encoding = 'UTF-8' # to fix encoding issues
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            num = int(soup.find("span", {"class": "h1__sub"}).text.split(" ")[-2])
        except AttributeError:
            num = 0
        #print(f"{base_url}&pg={MAX_NUM_PER_PAGE}&pagina={pag_num} gave {num} result(s)")
        max_page = math.ceil(num / MAX_NUM_PER_PAGE) # round up
        pag_num += 1
        links = soup.find_all("a", {"class" : "icon icon--download", "data-nabs-follow" : "false"})
        for link in links:
            kst_nr = link["href"].replace(".pdf", "")
            metafile_link = f"{OB_URL}{kst_nr}/metadata.xml"
            if "b" in kst_nr: # ...so it should be treated as a bijlage...
                #logging.info(f"Processing bijlage {metafile_link}")
                bijlage_response = http.get(metafile_link)
                #bijlage_response.encoding = 'UTF-8' # to fix decoding issues
                bijlage_soup = BeautifulSoup(bijlage_response.content, 'xml')
                try:
                    bijlage_title = bijlage_soup.find("metadata", {"name" : "DC.title"})['content']
                    bijlage_dossiernummer = bijlage_soup.find("metadata", {"name" : "OVERHEIDop.dossiernummer"})['content']
                    bijlage_ondernummer = bijlage_soup.find("metadata", {"name" : "OVERHEIDop.ondernummer"})['content']
                except TypeError:
                    logging.warning(f"Kan bijlage {metafile_link} niet verwerken, geen bijlage?")
                else:
                    try: 
                        bijlagen_dossier_dict = bijlagen_dict[bijlage_dossiernummer]
                    except KeyError:
                        bijlagen_dict[bijlage_dossiernummer] = {bijlage_ondernummer : [{kst_nr : bijlage_title}]}
                    else:
                        try:
                            bijlagen_ondernummer_list = bijlagen_dossier_dict[bijlage_ondernummer]
                        except KeyError:
                            bijlagen_dossier_dict[bijlage_ondernummer] = [{kst_nr : bijlage_title}]
                        else:
                            bijlagen_ondernummer_list.append({kst_nr : bijlage_title})
            elif "kst" in kst_nr: # should be a Kamerstuk
                kst = Kamerstuk()
                kst.add_info(metafile_link)
                if kst.date_str: # if date_str == None, most likely the search results is no (valid) Kamerstuk...
                    new_ksts.add(kst)
                else:
                    logging.warning(f"Ongeldig kamerstuk gevonden zonder datum in {metafile_link}")
            else:
                logging.warning(f"Onbekend documenttype gevonden zonder datum in {metafile_link}")
    # match bijlagen
    for kst in new_ksts:
        #logging.info(f"Try to add bijlagen to {kst.nr}")
        try:
            bijlagen_list = bijlagen_dict[kst.dossier_nr_string][kst.ondernummer_string]
            #logging.info(f"bijlagen_list found with length {len(bijlagen_list)}")
            for bijlage in bijlagen_list:
                #logging.info(f"Added bijlage {bijlage}")
                kst.bijlagen_dict |= bijlage # works since python 3.9
        except KeyError:
            pass
    return new_ksts

def get_new_stb_pubs(from_date, dossier_nr):
    new_stb_pubs = set()
    base_url = f"{OB_URL}resultaten?q=(c.product-area==\"officielepublicaties\")and(w.publicatienaam==\"Staatsblad\")"
    if from_date:
        base_url = f"{base_url}and(dt.available>=\"{from_date.strftime('%Y-%m-%d')}\")"
    elif dossier_nr:
        base_url = f"{base_url}and(cql.textAndIndexes=\"{dossier_nr}\")"
    pag_num = 1
    max_page = 1
    while pag_num <= max_page:
        response = http.get(f"{base_url}&pg={MAX_NUM_PER_PAGE}&pagina={pag_num}")
        response.encoding = 'UTF-8' # to fix encoding issues
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
            num = int(soup.find("span", {"class": "h1__sub"}).text.split(" ")[-2])
        except AttributeError:
            num = 0
        #print(f"{base_url}&pg={MAX_NUM_PER_PAGE}&pagina={pag_num} gave {num} result(s)")
        max_page = math.ceil(num / MAX_NUM_PER_PAGE) # round up
        pag_num += 1
        links = soup.find_all("a", {"class" : "icon icon--download", "data-nabs-follow" : "false"})
        for link in links:
            stb_nr = link["href"].replace(".pdf", "")
            metafile_link = f"{OB_URL}{stb_nr}/metadata.xml"
            stb_pub = Stb_pub()
            stb_pub.add_info(metafile_link)
            if stb_pub.date_str: # if date_str == None, most likely the search results is no (valid) Staatsblad publication
                new_stb_pubs.add(stb_pub)
    return new_stb_pubs

def add_data(dossier_info, kst):
    response = http.get(f"{OB_URL}{kst.nr}/metadata.xml")
    response.encoding = 'UTF-8' # to fix encoding issues
    soup = BeautifulSoup(response.text, 'xml')
    try:
        dossier_info.title = soup.find("metadata", {"name" : "OVERHEIDop.dossiertitel"})["content"]
    except TypeError:
        logging.warning(f"Geen titel gevonden voor dossier {dossier_info.nr} in {OB_URL}{kst.nr}/metadata.xml")
        
    # add date of most recent added item
    response = http.get(url=f"{OB_URL}rss?q=(c.product-area==\"officielepublicaties\")and((w.publicatienaam==\"Kamerstuk\")and(w.dossiernummer==\"{dossier_info.nr}\"))")
    response.encoding = 'UTF-8' # to fix encoding issues
    soup = BeautifulSoup(response.text, 'xml')
    item = soup.find("item")
    if item:
        dossier_info.last_date = datetime.datetime.strptime(str(item.find("pubDate").text), OB_DATE_STRING)

def add_dossiers_info(dossiers_info, new_kst):
    if dossiers_info:
        added_dossier_info = False
        for kst in new_kst:
            dossier_nrs = kst.dossier_nr_string.split(";")
            dossier_nr = dossier_nrs[0] # only process first dossier number
            dossier_info = next((item for item in dossiers_info if item.nr == dossier_nr), None)
            response = http.get(f"{OB_URL}resultaten?q=(c.product-area==\"officielepublicaties\")and(w.publicatienaam==\"Kamerstuk\")and(w.dossiernummer==\"{dossier_nr}\")")
            response.encoding = 'UTF-8' # to fix encoding issues
            soup = BeautifulSoup(response.text, 'html.parser')
            try: 
                num_string = soup.find("span", {"class": "h1__sub"}).text.split(" ")[-2]
            except AttributeError:
                logging.warning(f"0 search results for {OB_URL}resultaten?q=(c.product-area==\"officielepublicaties\")and(w.publicatienaam==\"Kamerstuk\")and(w.dossiernummer==\"{dossier_nr}\")")
                num_string = "0"
            
            if not dossier_info: # new dossier item found that was not already there
                dossier_info = Dossier_info(dossier_nr)
                add_data(dossier_info, kst)
                if dossier_info.title:
                    dossier_info.num_items = num_string
                    dossier_info.last_date = kst.date
                    dossiers_info = [dossier_info, *dossiers_info]
                    BOT.sendMessage(chat_id=CHAT_ID, text=f"New dossier number found: {dossier_nr} with title {dossier_info.title}")
                    time.sleep(2) # Telegram does not like too many messages within a short timeframe
                    logging.info(f"New dossier number found: {dossier_nr} with title {dossier_info.title}")
                    added_dossier_info = True
            else: # existing dossier_info object found, update numer of items and date last addition
                if dossier_info.num_items != num_string:
                    dossier_info.num_items = num_string
                    added_dossier_info = True
                if kst.date:
                    if dossier_info.last_date:
                        if dossier_info.last_date != max(kst.date, dossier_info.last_date):
                            dossier_info.last_date = max(kst.date, dossier_info.last_date)
                            added_dossier_info = True
                    else:
                        dossier_info.last_date = kst.date
                        added_dossier_info = True
                
        if added_dossier_info: # only write new csv when someting was added/modified
            with open(f"{datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S')}-dossieroverzicht.csv",'w', encoding='utf-8-sig') as csvfile:
                csvwriter = csv.writer(csvfile, delimiter=';', dialect='excel', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csvwriter.writerow(['nr', 'datum nieuwste stuk', 'aantal stukken', 'titel'])
                for dossier in dossiers_info:
                    csvwriter.writerow(dossier.get_result_list())
        with open(INFO_PICKLE_FILE, 'wb') as info_pickle: # write pickle anyway (regardless if there were additions/changes or not)
            pickle.dump(dossiers_info, info_pickle, protocol=pickle.HIGHEST_PROTOCOL)

def add_initial_dossier_info_data(dossier_info):
    # add title and num_items
    response = http.get(f"{REP_URL}{dossier_info.nr}/?format=xml")
    response.encoding = 'UTF-8' # to fix encoding issues
    soup = BeautifulSoup(response.text, 'xml')
    try:
        dossier_info.num_items = soup.find("subarea", {"label" : dossier_info.nr})["total"]
    except TypeError:
        #print(f"Geen aantal stukken gevonden voor dossier {dossier_info.nr}")
        logging.warning(f"Geen aantal stukken gevonden voor dossier {dossier_info.nr}")
        logging.warning(f"Status code: {response.status_code}; content:")
        logging.warning(soup.prettify())
    works = soup.find_all("work")
    for work in works:
        response = http.get(f"{REP_URL}{dossier_info.nr}/{work.text}/1/metadata/metadata.xml")
        response.encoding = 'UTF-8' # to fix encoding issues
        soup = BeautifulSoup(response.text, 'xml')
        try:
            dossier_info.title = soup.find("metadata", {"name" : "OVERHEIDop.dossiertitel"})["content"]
            break
        except TypeError:
            pass
    if not dossier_info.title:
        #print(f"Geen titel gevonden voor dossier {dossier_info.nr}")
        logging.warning(f"Geen titel gevonden voor dossier {dossier_info.nr}")
        
    # add date of most recent added item
    response = http.get(url=f"{OB_URL}rss?q=(c.product-area==\"officielepublicaties\")and((w.publicatienaam==\"Kamerstuk\")and(w.dossiernummer==\"{dossier_info.nr}\"))")
    response.encoding = 'UTF-8' # to fix encoding issues
    soup = BeautifulSoup(response.text, 'xml')
    item = soup.find("item")
    if item:
        dossier_info.last_date = datetime.datetime.strptime(str(item.find("pubDate").text), OB_DATE_STRING)

def get_initial_dossiers_info():
    dossiers_info = []
    start = 1
    total = 2
    item_num = 0
    #print("Ophalen dossier-info:")
    while start < total:
        response = http.get(f"{REP_URL}?start={start}&format=xml") 
        response.encoding = 'UTF-8' # to fix encoding issues
        soup = BeautifulSoup(response.text, 'xml')
        subarea = soup.find("subarea")
        pagesize = int(subarea["pagesize"])
        total = int(subarea["total"])
        dossier_items = subarea.find_all("subarea")
        for dossier_item in dossier_items:
            dossier = Dossier_info(dossier_item.text)
            add_initial_dossier_info_data(dossier)
            dossiers_info.append(dossier)
            item_num += 1
            #print(f"{item_num}/{total} dossiers verwerkt", end = "\r")
        start = start + pagesize
        #start = total # TEMP TO TEST
    #print(f"{item_num}/{total} dossiers verwerkt")

    dossiers_info.sort(key=lambda x: x.nr)
    
    return dossiers_info


def main():

    config_json = open(CONFIG_FILE)
    config_data = json.load(config_json)

    try:
        with open(INFO_PICKLE_FILE, 'rb') as info_pickle:
            dossiers_info = pickle.load(info_pickle)
        #print(f"Loaded info of {len(dossiers_info)} dossiers")
        info_date = datetime.datetime.fromtimestamp(os.path.getmtime(INFO_PICKLE_FILE))
    except (FileNotFoundError, EOFError) as e:
        dossiers_info = get_initial_dossiers_info()
    try:
        with open(STUKKEN_PICKLE_FILE, 'rb') as stukken_pickle:
            dossiers = pickle.load(stukken_pickle)
        #print(f"Loaded {len(dossiers)} dossiers")
        stukken_date = datetime.datetime.fromtimestamp(os.path.getmtime(STUKKEN_PICKLE_FILE))
        if dossiers_info:
            from_date = min([stukken_date, info_date]) # oldest of these two dates
        else:
            from_date = stukken_date
        #print(f"Previous run time was {from_date.isoformat()}")
        logging.info(f"Previous run time was {from_date.isoformat()}")
        new_kst = get_new_ksts(from_date, None, None)
        add_dossiers_info(dossiers_info, new_kst)
        new_stb_pubs = get_new_stb_pubs(from_date, None)
    except (FileNotFoundError, EOFError) as e:
        dossiers = []
        new_obs = set()
        
    for dossier in dossiers: # clean old dossiers from pickle that are no longer in config file
        config_dossier = next((item for item in config_data if item['DOSSIER_NR'] == int(dossier.nr)), None)
        if not config_dossier:
            logging.info(f"Removing {dossier.nr} from dossiers (no longer in {CONFIG_FILE}")
            dossiers.remove(dossier)
    for config_dossier in config_data:
        dossier_nr = str(config_dossier['DOSSIER_NR'])
        dossier = next((item for item in dossiers if item.nr == dossier_nr), None)
        if not dossier: # new dossier; in json but not in pickle ==> get everything!
            #print(end=LINE_CLEAR)
            #print(f"New dossier in {CONFIG_FILE}: {dossier_nr}")
            logging.info(f"New dossier in {CONFIG_FILE}: {dossier_nr}")
            dossier = Dossier(dossier_nr, config_dossier['ZOEKTERMEN'])
            dossier.add_rep_kamerstukken()
            for kst in dossier.obs:
                kst.add_bijlagen_titles()
            additional_kst = get_new_ksts(None, None, dossier_nr)
            for config_search_term in dossier.search_terms: 
                additional_kst |= get_new_ksts(None, config_search_term, None)
            for kst in additional_kst:
                if dossier_nr in kst.dossier_nr_string:
                    kst.add_bijlagen_titles()
                    dossier.obs.add(kst)
            additional_stb_pubs = get_new_stb_pubs(None, dossier_nr)
            for stb_pub in additional_stb_pubs:
                add_as_kamerstuk = False
                for dossier_link_string in stb_pub.dossier_links:
                    dossier_link = dossier_link_string.split(";")
                    if dossier_link[0] == dossier_nr:
                        add_as_kamerstuk = True
                        if len(dossier_link) == 2:
                            link_kamerstuk_nr = f"kst-{dossier_link[0]}-{dossier_link[1]}"
                            kamerstuk = next((item for item in dossier.obs if item.nr == link_kamerstuk_nr), None)
                            if kamerstuk:
                                kamerstuk.bijlagen_dict[stb_pub.nr] = stb_pub.title
                                add_as_kamerstuk = False
                if add_as_kamerstuk and stb_pub.date_str: # if Staatsblad publication could no be linked to specific Kamerstuk(ken), but a link to the dossier is present; then add as if it where a kamerstuk
                    dossier.obs.add(stb_pub)
            
            try:
                os.mkdir(f"{RESULTSDIR}{dossier_nr}")
            except FileExistsError:
                pass
            
            for ob in dossier.obs:
                ob.dowload_all(dossier_nr)
            dossier.write_html()
            
            dossiers.append(dossier)
        else: # existing dossier; in json and in pickle ==> get only updates
            additions = False
            #print(end=LINE_CLEAR)
            #print(f"Found dossier {dossier.nr} in pickle!")
            for config_search_term in config_dossier['ZOEKTERMEN']:
                if not config_search_term in dossier.search_terms:
                    # new search term added, search for it!
                    logging.info(f"New search term for {dossier.nr}:{config_search_term}")
                    new_kst |= get_new_ksts(None, config_search_term, None)
            dossier.search_terms = config_dossier['ZOEKTERMEN']
            #print(f"{len(additional_kst)} additional_ksts")
            for kst in new_kst:
                #print(kst.dossier_nr_string)
                if dossier_nr in kst.dossier_nr_string and kst not in dossier.obs:
                    additions = True
                    kst.add_bijlagen_titles()
                    dossier.obs.add(kst)
                    kst.dowload_all(dossier_nr)
                    BOT.sendMessage(chat_id=CHAT_ID, text=f"New kamerstuk for dossier {dossier_nr}: {OB_URL}{kst.nr}.pdf")
                    time.sleep(2) # Telegram does not like too many messages within a short timeframe
                    #print(end=LINE_CLEAR)
                    #print(f"New kamerstuk for dossier {dossier_nr}: {kst.nr}")
                    logging.info(f"New kamerstuk for dossier {dossier_nr}: {kst.nr}")

            for stb_pub in new_stb_pubs:
                add_as_kamerstuk = False
                for dossier_link_string in stb_pub.dossier_links:
                    dossier_link = dossier_link_string.split(";")
                    if dossier_link[0] == dossier_nr:
                        add_as_kamerstuk = True
                        if len(dossier_link) == 2:
                            link_kamerstuk_nr = f"kst-{dossier_link[0]}-{dossier_link[1]}"
                            kamerstuk = next((item for item in dossier.obs if item.nr == link_kamerstuk_nr), None)
                            if kamerstuk and not stb_pub.nr in kamerstuk.bijlagen_dict:
                                additions = True
                                kamerstuk.bijlagen_dict[stb_pub.nr] = stb_pub.title
                                stb_pub.download_file(stb_pub.nr, f"{kamerstuk.date_str}-{kamerstuk.nr}-{stb_pub.nr}.pdf", dossier_nr)
                                BOT.sendMessage(chat_id=CHAT_ID, text=f"New Staatsblad publication for dossier {dossier_nr}: {OB_URL}{stb_pub.nr}.pdf")
                                time.sleep(2) # Telegram does not like too many messages within a short timeframe
                                #print(end=LINE_CLEAR)
                                #print(f"New Staatsblad publication for dossier {dossier_nr}: {stb_pub.nr}")
                                logging.info(f"New Staatsblad publication for dossier {dossier_nr}: {stb_pub.nr}")
                                add_as_kamerstuk = False
                if add_as_kamerstuk and stb_pub.date_str and stb_pub not in dossier.obs: # if Staatsblad publication could no be linked to specific Kamerstuk(ken), but a link to the dossier is present; then add as if it where a kamerstuk
                    additions = True
                    dossier.obs.add(stb_pub)
                    stb_pub.download_file(stb_pub.nr, f"{stb_pub.date_str}-{stb_pub.nr}.pdf", dossier_nr)
                    BOT.sendMessage(chat_id=CHAT_ID, text=f"New Staatsblad publication for dossier {dossier_nr}: {OB_URL}{stb_pub.nr}.pdf")
                    time.sleep(2) # Telegram does not like too many messages within a short timeframe
                    #print(end=LINE_CLEAR)
                    #print(f"New Staatsblad publication for dossier {dossier_nr}: {stb_pub.nr}")
                    logging.info(f"New Staatsblad publication for dossier {dossier_nr}: {stb_pub.nr}")
            
            if additions:
                dossier.write_html()
                with py7zr.SevenZipFile(f"{RESULTSDIR}{dossier.nr}.7z", 'w') as archive:
                    archive.writeall(f"{RESULTSDIR}{dossier.nr}/")
    
    #print(end=LINE_CLEAR)
    #print(f"Number of dossiers to write to {STUKKEN_PICKLE_FILE}: {len(dossiers)}")
    with open(STUKKEN_PICKLE_FILE, 'wb') as stukken_pickle:
        pickle.dump(dossiers, stukken_pickle, protocol=pickle.HIGHEST_PROTOCOL)

if __name__ == "__main__":
    main()
