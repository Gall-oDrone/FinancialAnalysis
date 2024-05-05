import time
import json
import time
import pandas as pd
import uuid

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService

from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager


class Scrapper:
    def __init__(self, debug=False, topics=[]):
        # Initialize the Selenium WebDriver with default options
        self.debug = debug
        self.set_driver_options()
        self.topics = topics
        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=self.options)
        self.df_store = DataFrameStore()

    def set_driver_options(self):
        # Set desired options for the WebDriver
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.90 Safari/537.36")
        self.options.add_argument("--window-size=1920,1080")
        self.options.add_argument("--disable-extensions")
        self.options.add_argument("--proxy-server='direct://'")
        self.options.add_argument("--proxy-bypass-list=*")
        self.options.add_argument("--start-maximized")
        if self.debug == False:
            self.options.add_argument('--headless')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--ignore-certificate-errors')

    def generate_random_id(self):
        # Generate random id
        random_id = uuid.uuid4().int
        return random_id
    
    def isAtBottom(self, max_scrolls=1):
        print("Scrolling down...", end='', flush=True)
        driver = self.driver
        lastHeight = driver.execute_script("return document.documentElement.scrollHeight")
        while True:
            driver.execute_script("var scrollingElement = (document.scrollingElement || document.body);scrollingElement.scrollTop = scrollingElement.scrollHeight;")
            driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(1)
            height = driver.execute_script("return document.documentElement.scrollHeight")
            driver.execute_script("window.scrollTo(0, " + str(height) + ");")
            if lastHeight == height:
                print("finished")
                break
            lastHeight = height
            #max_scrolls -= 1
        
    def getText(self, el):
        try:
            return el.text
        except Exception as e:
            print("error during getting text from html el:", e)

    def extractToText(self, html_element):
        print("extractToText reached")
        wait = WebDriverWait(self.driver, 3.0)
        extracted_text = ''
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, html_element)))
            print("html_element: ", html_element)
            extracted_text = self.driver.find_element(By.XPATH, html_element).text
            print("extracted_text: ", extracted_text)
        except Exception as e:
            print("error during extract_to_text trying alt:", e)
            try:
                html_element = '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/div/div[1]/h1'
                wait.until(EC.presence_of_element_located((By.XPATH, html_element)))
                print("html_element: ", html_element)
                extracted_text = self.driver.find_element(By.XPATH, html_element).text
                print("extracted_text: ", extracted_text)
            except Exception as e:
                print("error during extract_to_text 2nd:", e)
        return extracted_text

    def printXPathAndClass(self, el):
        # Get the XPath of the element
        element_xpath = el.get_attribute("xpath")
        print("Element XPath:", element_xpath)

        # Get the class attribute of the element
        element_class = el.get_attribute("class")
        print("Element Class:", element_class)
    
    def printChildHTMLElements(self, el):
        # Find and print all child elements of the <li> element
        child_elements = el.find_elements(By.XPATH, "./*")
        for child_element in child_elements:
            print(child_element.get_attribute("outerHTML"))
    
    def getHref(self, el):
        # Assuming div_child_4 is the h3 element
        # Find the nested anchor element within the h3 element using XPath
        anchor_element = el.find_element(By.XPATH, ".//a")

        # Retrieve the href attribute value from the anchor element
        href_value = anchor_element.get_attribute("href")

        # Print or use the href value as needed
        print("Href:", href_value)
        return href_value
   
    def selectHTMLElement(self, root_el, query_by, query_html_el):
        return root_el.find_element(query_by, query_html_el)
    
    def selectHTMLElements(self, root_el, query_by, query_html_el):
        return root_el.find_elements(query_by, query_html_el)
    
    def printDataframe(self):
        print("-------------------------------------------------------------------------")
        print("on printDataframe()")
        print("-------------------------------------------------------------------------")
        # Iterate through each row
        for index, row in self.df_store.data_frame.iterrows():
            print(row)
     
    def storeToDatabase(self):
        print("-------------------------------------------------------------------------")
        print("storing to Database")
        print("-------------------------------------------------------------------------")
        pg_conn.reopen_connection()    
        header = self.df_store.data_frame_header
        for index, row in self.df_store.data_frame.iterrows():
            pg_conn.save_to_postgres(row, header)
                        
    def quit_driver(self):
        # Close the WebDriver
        self.driver.quit()
    
    def load_url(self, target_url):
        print("-------------------------------------------------------------------------")
        print(f"loading target url {target_url} ...", end='', flush=True)
        try:
            self.driver.get(target_url)
            print("successed")
        except Exception as nse:
            print("failed")
            print(nse)
            print("-----")
            print(str(nse))
            print("-----")
            print(nse.args)
            print("=====")
        print("-------------------------------------------------------------------------")
        
    def parse_date(self, date_str):
        # Convert the month name to a numerical representation using a dictionary
        month_dict = {
            "Jan": "01",
            "Feb": "02",
            "Mar": "03",
            "Apr": "04",
            "May": "05",
            "Jun": "06",
            "Jul": "07",
            "Aug": "08",
            "Sep": "09",
            "Oct": "10",
            "Nov": "11",
            "Dec": "12",
        }

        date_str = date_str.replace(",", "")

        # Split the date string into month, day, and year
        month, day, year = date_str.split()

        # Get the numerical representation of the month from the dictionary
        month_number = month_dict[month]

        # Create a new date string in the format 'year-month-day' (e.g., '2023-08-01')
        formatted_date_str = f"{year}-{month_number}-{day}"

        # Parse the formatted date string to a datetime object
        parsed_date = datetime.strptime(formatted_date_str, "%Y-%m-%d")

        return parsed_date

    def parse_row_data(self, row_data):
        try:
            date_format = '%Y-%m-%d'  # Format for parsing date strings

            # Remove commas from numeric values
            row_data = [item.replace(",", "") if isinstance(item, str) else item for item in row_data]

            # Parse elements at specific positions into desired data types
            row_data[0] = parse_date(row_data[0])
            row_data[1] = float(row_data[1])
            row_data[2] = float(row_data[2])
            row_data[3] = float(row_data[3])
            row_data[4] = float(row_data[4])
            row_data[5] = float(row_data[5])
            row_data[6] = int(row_data[6])
            return row_data
        except Exception as e:
            print("error during parsing data:", e, "row_data: ", row_data)        
    
        def printInnerHTML(self, xpath):
        # Get the inner HTML of the specific element
        specific_element = driver.find_element(By.XPATH, xpath)

        inner_html = specific_element.get_attribute('innerHTML')

        # Print the inner HTML of the specific element
        print(inner_html)


    def printXPathAndClass(el):
        # Get the XPath of the element
        element_xpath = el.get_attribute("xpath")
        print("Element XPath:", element_xpath)

        # Get the class attribute of the element
        element_class = el.get_attribute("class")
        print("Element Class:", element_class)

        
'''
 NewsScrapper class for srapping raw text data from Yahoo Finance
'''
class NewsScrapper(Scrapper):
    
    def getMainHeadline(self, html_element):
        return self.extractToText(html_element)
    
    def setUlNewsDivContainer(self):
        return '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[3]/div/ul/li[1]/div/div/div[2]'
    
    def setUlNewsSourceAndTimestamp(self):
        return '/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[3]/div/ul/li[1]/div/div/div[2]/div[2]'
    
    def extractUlNewsSourceAndTimestamp(self, el):
        query_by = By.TAG_NAME
        query_html_el = 'span'
        news_source = ''
        calculated_datetime = ''
        calculated_timestamp = ''
        els = self.selectHTMLElements(el, query_by, query_html_el)
        for i, el in enumerate(els):
            if i == 0:
                news_source = self.getText(el)
            elif i == 1:
                news_timestamp = self.getText(el)
                calculated_datetime, calculated_timestamp = calculate_datetime_from_ago_string(news_timestamp)
        print('news_source: ', news_source, ', calculated_datetime: ', calculated_datetime, ', calculated_timestamp', calculated_timestamp)
        return news_source
  
    def extractFullNewsSourceAndTimestamp(self, el):
        query_by = By.TAG_NAME
        query_html_el = 'time'
        class_attribute = 'datetime'
        news_source = ''
        news_datetime = ''
        el = self.selectHTMLElement(el, query_by, query_html_el)
        print('full news_datetime: ', el.get_attribute(class_attribute))
        text = el.get_attribute(class_attribute)
        return text
    
    def clickOnReadMoreArticle(self):
        # Find all elements with the class 'caas-readmore'
        readmore_buttons = self.driver.find_elements(By.CLASS_NAME, "collapse-button")

        # Check if any buttons were found
        if readmore_buttons:
            # Click on the first button found
            readmore_buttons[0].click()
        else:
            print("No buttons with class 'caas-readmore' found")
    
    def selectLiNewsElements(self, el):
        print("-------------------------------------------------------------------------")
        print("on selectLiNewsElements method")
        print("-------------------------------------------------------------------------")
        # html 'el' should be the list item (li) element
        query_by = By.XPATH
        query_html_el = './*'
        clickable_el = ''
        try:
            self.printXPathAndClass(el) # li js-stream-content Pos(r)
            div_parent = self.selectHTMLElements(el, query_by, query_html_el)
            for h, div_chlds1 in enumerate(div_parent):
                print("li div_childs loop h: ", h)
                self.printXPathAndClass(div_chlds1) # div | Py(14px) Pos(r)
                try:
                    div_chlds2 = self.selectHTMLElements(div_chlds1, query_by, query_html_el)
                    for i, div_child_3 in enumerate(div_chlds2):
                        print("div_chlds2 loop i: ", i)
                        if i == 0:
                            self.printXPathAndClass(div_child_3) # Cf
                            div_chlds3 = self.selectHTMLElements(div_child_3, query_by, query_html_el)
                            for j, div_child_4 in enumerate(div_chlds3):
                                if j == 1:
                                    try:
                                        div_chlds3 = self.selectHTMLElements(div_child_3, query_by, query_html_el)
                                        for k, div_child_4 in enumerate(div_chlds3):
                                            print("div_chlds3 loop, k= ", k)
                                            if k == 1:
                                                self.printXPathAndClass(div_child_4) # Ov(h) Pend(44px) Pstart(25px)
                                                div_chlds4 = self.selectHTMLElements(div_child_4, query_by, query_html_el)
                                                # Initialize an empty dictionary to store data for each div_child_5 elements
                                                news_item = {}
                                                news_item["id"] = self.generate_random_id()
                                                for l, div_child_5 in enumerate(div_chlds4):
                                                    print("div_chlds4 loop, l= ", l)
                                                    if l == 0:
                                                        self.printXPathAndClass(div_child_5)
                                                        news_item['category'] = self.getText(div_child_5)
                                                    elif l == 1:
                                                        print("Test: ", self.getText(div_child_5))
                                                        if 'yesterday' in self.getText(div_child_5) or 'days' in self.getText(div_child_5):
                                                            print("News published 'yesterday' or on 'x days' ago reached")
                                                            return
                                                        news_source = self.extractUlNewsSourceAndTimestamp(div_child_5)
                                                        news_item['source'] = news_source
                                                    elif l == 2:
                                                        news_item['headline'] = self.getText(div_child_5)
                                                        news_href = self.getHref(div_child_5)
                                                        news_item['href'] = news_href
                                                    elif l == 3:
                                                        news_item['summary'] = self.getText(div_child_5)
                                                        news_item['content'] = pd.NA
                                                        news_item['datetime'] = pd.NA
                                                        self.df_store.update_data_frame(news_item)
                                                        return
                                    except Exception as e:
                                        print('While defining div_chlds3 on selectLiNewsElements, error is: ', e)
                except Exception as e:
                    print('While defining div_childs1, error is: ', e)

        except Exception as e:
            print('While defining div_parent, error is: ', e)

    def selectFullNewsElements(self, el):
        print("-------------------------------------------------------------------------")
        print('on selectFullNewsElements() method')
        print("-------------------------------------------------------------------------")
        # html 'el' should be a list of div elements from the class 'caas-content-wrapper'
        # el = '/html/body/div[4]/div/div/div/div[2]/div[1]/div/div[1]/div/article/div/div/div/div/div/div[2]'
        query_by = By.XPATH
        query_html_el = './*'
        news_item = {}
        try:
            div_parent = el # caas-content wafer-sticky
            for div_child in div_parent:
                print("full_div_parent loop")
                self.printXPathAndClass(div_child) # caas-content-wrapper neo-grid-container
                div_chlds2 = self.selectHTMLElements(div_child, query_by, query_html_el)
                for i, div_child_2 in enumerate(div_chlds2):
                    print("full_div_chlds2 loop, i=", i)
                    try:
                        if i == 0:
                            self.printXPathAndClass(div_child_2) # col-neofull-offset-3-span-8
                            div_chlds3 = self.selectHTMLElements(div_child_2, query_by, query_html_el)
                            for j, div_child_3 in enumerate(div_chlds3): 
                                print("full_div_chlds3 loop, j=", j)
                                if j == 2:
                                    self.printXPathAndClass(div_child_3) # caas-content-byline-wrapper
                                    div_chlds4 = self.selectHTMLElements(div_child_3, query_by, query_html_el)
                                    for l, div_child_4 in enumerate(div_chlds4): 
                                        print("full_div_chlds4 loop, l=", l)
                                        if l == 0:
                                            self.printXPathAndClass(div_child_4) # caas-attr
                                            div_chlds5 = self.selectHTMLElements(div_child_4, query_by, query_html_el)
                                            for m, div_child_5 in enumerate(div_chlds5): 
                                                print("full_div_chlds5 loop, m=", m)
                                                if m == 0: 
                                                    self.printXPathAndClass(div_child_5) # caas-attr-meta
                                                    div_chlds6 = self.selectHTMLElements(div_child_5, query_by, query_html_el)
                                                    for n, div_child_6 in enumerate(div_chlds6): 
                                                        if n == 1:
                                                            self.printXPathAndClass(div_child_6) # caas-attr-time-style
                                                            news_item['datetime'] = self.extractFullNewsSourceAndTimestamp(div_child_6)
                                                            self.df_store.data_frame.at[self.df_store.row_index, "datetime"] = news_item['datetime']
                        elif i == 1 or div_child_2.get_attribute("caas-body"):
                            print("div_child_2 loop, i=", i)
                            self.printXPathAndClass(div_child_2) # morpheusGridBody col-neofull-offset-3-span-8
                            news_item['content'] = self.getText(div_child_2)
                            self.df_store.data_frame.at[self.df_store.row_index, "content"] = news_item['content']

                    except Exception as e:
                                print('While defining div_p1 on Full news, error is: ', e)

        except Exception as e:
            print('While defining div_parent, error is: ', e)
    
    def viewFullNewsArticle(self):
        #el.click()
        query_html_el='caas-body-content'
        # /html/body/div[3]/div/div/div/div[2]/div[1]/div/div[1]/div/article/div
        query_by=By.CLASS_NAME
        wait = WebDriverWait(self.driver, 1)
        try:
            el = wait.until(EC.presence_of_element_located((query_by, query_html_el)))
            self.printXPathAndClass(el)
            self.clickOnReadMoreArticle()
            query_html_el = 'caas-content-wrapper'
            el = self.selectHTMLElements(el, query_by, query_html_el)
            self.selectFullNewsElements(el)
        except Exception as e:
                print("error couldn't load full news article:", e)
                
    def selectUnorderList(self, html_element=''):
        html_element='/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[3]/div/div/div/ul'
        show_text = False
        print("In selectUnorderList method")
        print("html element:", html_element)
        ul_element = self.driver.find_element(By.XPATH, html_element)
        list_items = ul_element.find_elements(By.TAG_NAME, 'li')
        # Loop through each list item
        for li in list_items:
            if show_text == True:
                # Extract and print the text content of each list item
                li_text = li.text
                print("li text element: ", li_text)
            self.selectLiNewsElements(li)
        
    def loadFullNewsStory(self):
        # Clicks on the button to show the full news
        el = '/html/body/div[3]/div[1]/div/main/div[1]/div/div/div/div/article/div/div/div/div/div/div[2]/div[3]/div/button'
        body_class = 'caas-body'

    def setFullNewContainer(self):
        return '/html/body/div[4]/div/div/div/div[2]/div[1]/div/div[1]/div/article/div/div/div/div/div/div[2]'
    
    def getNewsContentDivContainer(self):
        # div element that contains full news body
        return '/html/body/div[3]/div[1]/div/main/div[1]/div/div/div/div/article/div/div/div/div/div/div[2]/div[3]'

    def getTextContent(self, el):
        div_element = self.driver.find_element(By.XPATH, el)

        # Locate all <p> and <h2> elements within the div
        p_and_h2_elements = div_element.find_elements(By.XPATH, './/p | .//h2')

        # Loop through each <p> and <h2> element
        for element in p_and_h2_elements:
            # Extract and print the text content of each element
            element_text = element.text
            print("Element Text:", element_text)
    
    def retrieveFullNewsArticle(self):
        print("-------------------------------------------------------------------------")
        print("on retrieveFullNewsArticle()")
        print("-------------------------------------------------------------------------")
        # Iterate through each row
        for index, row in self.df_store.data_frame.iterrows():
            # Get the href value from the "href" column
            href = row["href"]
            self.load_url(href)
            self.df_store.row_index = index
            self.viewFullNewsArticle()

    def selenium_scrapper(self, html_element):
        max_time_to_wait_html_element = 1.5
        skipScrollingDown = True
        try:
            WebDriverWait(self.driver,max_time_to_wait_html_element).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            try:
                for topic in self.topics:
                    print(f'Topic: {topic}')
                    target_url = f"https://finance.yahoo.com/topic/{topic}/"
                    self.load_url(target_url)
                    print("driver.current_url != target_url: ", self.driver.current_url != target_url)
                    while self.driver.current_url != target_url:
                        self.driver.get(target_url)
                        time.sleep(1) 
                    if skipScrollingDown == False:
                        self.isAtBottom()
                    self.selectUnorderList()
                    self.printDataframe()
                    self.retrieveFullNewsArticle()
                    self.storeToDatabase()
                    print("scraping data task finished")
                    print("====================================================================")
                print("**All book data was scraped**")
            except NoSuchElementException as nse:
                print(nse)
                print("-----")
                print(str(nse))
                print("-----")
                print(nse.args)
                print("=====")
        except Exception as e:
            print(e)
            print("-----")
            print(str(e))
            print("-----")
            print(e.args)

        except TimeoutException as toe:
            print(toe)
            print("-----")
            print(str(toe))
            print("-----")
            print(toe.args)
        finally:
            '''
            if(DEBUG):
                delete_table("historical", conn)
            '''
            pg_conn.close_connection()
        
    def setTestDataForFullNewsContent(self):
        data = {
            "id": self.generate_random_id(),
            "category": "Test category",
            "source": "Test source",
            "headline": "Test headline",
            "href": "https://finance.yahoo.com/news/microstrategy-saylor-reaps-stock-windfall-120633982.html",
            "summary": "Test summary",
            "content": pd.NA,
            "datetime": pd.NA
        }
        self.df_store.create_data_frame
        self.df_store.update_data_frame(data)
        
    def test_fullNewContent(self):
        # Initialize an empty list to store extracted data
        storeToDB = False
        try: 
            self.setTestDataForFullNewsContent()
            self.retrieveFullNewsArticle()
            if storeToDB == True:
                self.storeToDatabase()
            #last_five_rows = df.tail(3)
            print("====================================================================")
            print("scraping data task finished")
            print("====================================================================")
            print("**All book data was scraped**")
        except NoSuchElementException as nse:
            print(nse)
            print("-----")
            print(str(nse))
            print("-----")
            print(nse.args)
            print("=====")
        except Exception as e:
            print(e)
            print("-----")
            print(str(e))
            print("-----")
            print(e.args)
        except TimeoutException as toe:
            print(toe)
            print("-----")
            print(str(toe))
            print("-----")
            print(toe.args)
        finally:
            if(DELETE_TABLE):
                pg_conn.delete_table()
            pg_conn.close_connection()
 
'''
 StocksScrapper class for srapping raw text data from Yahoo Finance
'''
class StocksScrapper(Scrapper):     
    
    MAIN_SECTION_STOCK_DATA_HTML_EL = "/html/body/div[1]/main/section/section/section"
    HISTORIAL_DATA_BTN = "/html/body/div[1]/div/div/div[1]/div/div[2]/div/div/div[7]/div/div/section/div/ul/li[4]/a"
    STOCKS_HTML_TABLE = "/html/body/div[1]/main/section/section/section/article/div[1]/div[3]/table"
    STOCKS_HTML_TABLE_BODY = "/html/body/div[1]/main/section/section/section/article/div[1]/div[3]/table/tbody"
    NO_RESULTS_FOUND_HTML_SPAN_EL = "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[1]/div/div/section/section/div/div/span/span"
    
    def save_unavailable_book(self, book_name):
        try:
            current_directory = os.getcwd()
            unavailable_books_file = os.path.join(current_directory, "unavailable_books.csv")

            file_exists = os.path.isfile(unavailable_books_file)
            with open(unavailable_books_file, "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(["book"])  # Add header if the file is newly created
                writer.writerow([book_name])
            print(f"Book '{book_name}' added to unavailable_books.csv")
        except Exception as e:
            print(f"Error while saving book '{book_name}' to CSV: {e}")
    
    def get_dynamic_url(self, ticker, period1=1410825600, period2=1690675200, interval="1d",adjclose="true"):
        return f'https://finance.yahoo.com/quote/{ticker.upper()}/history?period1={period1}&period2={period2}&interval={interval}&filter=history&frequency={interval}&includeAdjustedClose={adjclose}'

    def scroll_to_bottom(self, driver):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")


    def check_tab_header(self, driver):
        try:
            element = driver.find_element(By.XPATH, '//*[@id="quote-nav"]')
            #tab = driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div[1]/div/div[2]/div/div/div[7]/section/div/ul/li[3]/a')
            return True
        except Exception as e:
            print(f"financial header or historical data tab does not exist: {e}")
            return False

    def check_html_el_exist(self, driver, xpath):
        wait = WebDriverWait(driver, 3.0)
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            return True
        except NoSuchElementException:
            print("Element does not exist")
            return False

    def nomatchresult(self, driver, book):
        try:
            if (driver.current_url == f"https://finance.yahoo.com/lookup?s={book.upper()}" or check_html_el_exist(driver, NO_RESULTS_FOUND_HTML_SPAN_EL)):
                print(f"no data was found for {book.upper()}")
                return True
        except Exception as nse:
                print(f"{book.upper()} book was found!")
                return False

    def lookup_ticker(self, driver, ticker):
        RejectAll= driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div[1]/div/div[3]/div[2]/div/div/div/div/div/div[1]/div/div/div/form/input')
        action = ActionChains(driver)
        action.click(on_element = RejectAll)
        action.perform()
        time.sleep(5)
        SearchBar = driver.find_element(By.ID, "yfin-usr-qry")
        SearchBar.send_keys(ticker.upper())
        SearchBar.send_keys(Keys.ENTER)

    def select_historical_li(self, driver):
        li_historical_a = driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div[1]/div/div[2]/div/div/div[7]/div/div/section/div/ul/li[4]/a')
        action = ActionChains(driver)
        action.click(on_element = li_historical_a)
        action.perform()
        time.sleep(3)

    def disable_ad(self, driver): 
        wait = WebDriverWait(driver, 3.0)
        try:
            ad_element = '//*[@id="Col1-0-Ad-Proxy"]'
            wait.until(EC.presence_of_element_located((By.XPATH, ad_element)))
            driver.execute_script("arguments[0].style.display = 'none';", ad_element)
        except Exception as e:
            print("ad element was not found")

    def historical_stock_search_selector(self, driver):
        print("selecting historical dropdown menu")
        wait = WebDriverWait(driver, 3.0)
        try:
            selector1 = "/html/body/div[1]/main/section/section/section/article/div[1]/div[1]/div[1]" # Menu container
            wait.until(EC.presence_of_element_located((By.XPATH, selector1)))
            return selector1
        except Exception as e:
            print("selector1 for time period not found trying the second")
            printInnerHTML("/html/body/div[1]/main/section/section/section/article/")
            try:
                selector2 = "/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section"
                wait.until(EC.presence_of_element_located((By.XPATH, selector2)))
                return selector2
            except Exception as e:
                print("selector2 for time period not found")

    def select_historical(self, driver, time_period, freq):
        print("Assessing historical stock prices table data ...", end='', flush=True)
        # disable_ad(driver)
        wait = WebDriverWait(driver, 3.0)
        hs_se = historical_stock_search_selector(driver)
        action = ActionChains(driver)
        hs_se_button = driver.find_element(By.XPATH, f"{hs_se}/button")
        hs_se_button.click()

        time.sleep(3)
        try:
            '''
            TODO Add Frequency HTML button element
            '''
            hs_period_dropdown_div = ''
            if (time_period == '1d'):
                hs_period_dropdown_div = driver.find_element(By.XPATH, f"{hs_se}/div/div/div[2]/section/div[1]/button[1]")
                hs_period_dropdown_div.click()
            elif (time_period == '5d'):
                hs_period_dropdown_div = driver.find_element(By.XPATH, f"{hs_se}/div/div/div[2]/section/div[1]/button[2]")
                hs_period_dropdown_div.click()
            elif (time_period == '1y'):
                hs_period_dropdown_div = driver.find_element(By.XPATH, f"{hs_se}/div/div/div[2]/section/div[1]/button[6]")
                hs_period_dropdown_div.click()
        except Exception as e:
            print("Error on select_historical(): ", e)
            print("====================================================================")
            print("Printing inner HTML")
            print("====================================================================")
            printInnerHTML(hs_se)


        wait.until(EC.presence_of_element_located((By.XPATH, STOCKS_HTML_TABLE)))
        print("Task finished")