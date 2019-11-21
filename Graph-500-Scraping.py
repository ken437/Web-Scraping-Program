    
#import tools
from bs4 import BeautifulSoup
  
import urllib.request as request
import pandas
import time
import re
   
import requests

class Graph500Scraping:
  """Scrapes the Graph 500 website and finds a dataframe containing its data"""

  #fields -----------------------------------------------------------------------------
    
  #The URL containing links to lists 1-10
  url_1_to_10 = None
    
  #The URL containing links to lists 11, 12, and 13
  url_11_12_13 = None

  #the dataframe that will be used to store all of this data
  main_dataframe = None

  #constructor -------------------------------------------------------------------------

  def __init__(self):
    """Generates a list of all urls that must be scraped and scrapes those urls"""
    #initialize fields
    url_1_to_10 = "http://graph500.org/?page_id=469"
    url_11_12_13 = "http://graph500.org/?page_id=579"
    main_dataframe = pandas.DataFrame()

    #generate a list of all urls that must be scraped
    all_urls = self.scrape(url_1_to_10)
    all_urls.extend(self.scrape(url_11_12_13))
    
    #scrape every url
    for url in all_urls:
      print("scraping a url:")
      list_of_dictionaries = self.scrape_page(url) #returns None if the url has no table
      if list_of_dictionaries != None: #exclude urls that have no table
        for dictionary in list_of_dictionaries:
          curr_dataframe = pandas.DataFrame(dictionary, index=[0])
          main_dataframe = main_dataframe.append(curr_dataframe, sort=True)
    
    #if the user wishes to display the output dataframe in colab:
    #main_dataframe
    #print_full(main_dataframe)

    self.export(main_dataframe)

  #methods -----------------------------------------------------------------------------

  def create_soup(self, url, parser="html5lib"):
    """open the url and store its soup as a BeautifulSoup object
    @param url the url for which this method will create a soup
    @param parser the parser used to generate this soup
    @return the BeautifulSoup object that will be used to scrape this url"""
    header = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36"}
    page = requests.get(url, headers=header)
    time.sleep(1)
    return BeautifulSoup(page.text, parser)
    
  def scrape(self, url):
    """open this url and list all the sub-urls on the webpage. Returns a list of strings
    @param url one of the main urls (url_1_to_10 or url_11_12_13) that contains
    a list of sub-urls"""
    soup = self.create_soup(url)
    
    #find all contents inside of "a" tags
    urls = soup.find_all("a")
    urls_list = list()
      
    #append each URL to a list
    #remove the first and last few urls (they do not link to tables)
    for i in range(4, len(urls) - 4):
      url_href = urls[i]['href']
      urls_list.append(url_href)
    
    return urls_list
    
  def find_headers(self, url):
    """open this url and find all headers on the table. Returns a list of (non-repeating) strings 
    if the link has headers and None if it has no headers
    @param url the url, which may or may not link to a table
    @return if the url links to a table, a list of the table's headers. If the
    url does not link to a table, None"""
    soup = self.create_soup(url)
    headers = soup.find_all("th")
      
    #if there are no headers, return None
    if len(headers) == 0:
      return None
    
    headers_list = list()
    
    for header in headers:
      header_text = header.get_text()
      #no duplicate headers
      if header_text not in headers_list:
        headers_list.append(header.get_text())
    
    return headers_list
    
  def scrape_page(self, url):
    """Finds each row of the table in this url and stores it as a dictionary. Returns a list of dictionaries
    representing each row of the table unless the url has no table, in which case it returns None.
    @param url the url, which may or may not link to a table, that will be scraped
    @return if the url links to a table, a list of dictionaries with each dictionary
    containing data from one row of the table. If the url does not link to a table,
    None"""
    soup = self.create_soup(url)
    
    headers = self.find_headers(url)
    
    if headers != None:
    
      #each td tag represents an element in the table
      td_tags = soup.find_all("td")
    
      num_columns = len(headers)
      num_rows = len(td_tags) // num_columns
    
      data_list = list()
         
      #for each row:
      for i in range(0, num_rows):
        curr_data = dict()
        #log the source
        curr_data["Source"] = url
    
        #for each column in this row:
        for j in range(0, num_columns):
          curr_tag = td_tags[num_columns * i + j]
          #add this tag to the dictionary under the correct header
          curr_data[headers[j]] = curr_tag.get_text()
    
          data_list.append(curr_data)
        
        return data_list
      
    else:
      return None
    
    
  def print_full(self, dataframe):
    """Prints the entire dataframe without excluding any middle rows
    @param dataframe the dataframe that will be printed"""
    pandas.set_option('display.max_rows', len(x))
    print(dataframe)
    pandas.reset_option('display.max_rows')
    
  def export(self, dataframe):
    """Saves the dataframe to a CSV file
    @param dataframe the dataframe that will be exported"""
    dataframe.to_csv(r'C:\Users\Ken\Documents\Dataframe\graph500_dataframe.csv', index=False)

#constructor call-----------------------------------------------------------------------------------------
Graph500Scraping()
