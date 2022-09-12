from base64 import encode
import logging as logger
import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor
import time
import requests
from Result import Result
from datetime import datetime as dt
from urllib.parse import urlparse
import csv
import random
from re import compile, sub, IGNORECASE, DOTALL
import chardet
import justext as jt

# TO DO:
# ✅ convert print statements to proper Logging
# ✅ Escape comma's in CSV writer (Ideally should work rith writerows func but needs to verify)
# ✅ Fix data encoding error while write to csv
# ✅ Final output in [URL, content] format
# ✅ Fix header rows are spliting to rows issue
# ✅ Implement parallel processing to increase performance
# ✅ Execute a full execution cycle and record the time - No of URL reults = 1551, No of pdf reults = 1010, Execution completed in 439.48202950000996 seconds 
# ❌ PEP Standards check
# ❌ tdqm for check progress
# ✅ Document the code
# ✅ Generate requirements.txt

# Constant Variables 
TIMEOUT = 40
ENCODE = 'utf-8'


# File paths
script_dir = os.path.dirname(os.path.realpath(__file__))
input_path =  os.path.join(script_dir,  'input_data', 'test_URLs.csv')
output_path = os.path.join(script_dir,  'output_data')

# Iniitaling logger
logger.basicConfig(filename=output_path+'\logger.log', encoding=ENCODE, level=logger.DEBUG)

# User agents list to randomly select an agent for requests.
user_agent_list = [
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0',
'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:53.0) Gecko/20100101 Firefox/53.0',
'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; Trident/5.0)',
'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0; MDDCJS)',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393',
'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)'
]

"""_summary_
Generate a random headers to be passed to the request to mimic different type of browsers
parameters: 
return: Header
"""
def get_random_headers():
    for i in range(1,12):
        #Pick a random user agent
        user_agent = random.choice(user_agent_list)
    
    #Set the headers 
    return {'User-Agent': user_agent}

"""_summary_
Get title using regex.
parameters: contents from the request
return: Title of the web content 
"""
def get_title(content):
    try:
        regex = compile('<title>(.*?)</title>', IGNORECASE|DOTALL)
        res = regex.search(content.decode(chardet.detect(content)['encoding'])).group(1)
    except:
        res = None
    finally:
        return res
 
"""_summary_
Download pdf files and update the reuslts set accordingly
parameters: URL, timeout value - Default 20 seconds
return: Results object
"""
def extract_pdf_data(param_url, tout = TIMEOUT):
    file_name = output_path+ '/' + get_pdf_file_name(param_url)
    try:
        # Send GET request
        r = requests.get(param_url, timeout=tout, headers = get_random_headers())
        # Save the PDF
        if r.status_code == 200:
            with open(file_name, "wb") as f:
                f.write(r.content)
                
            data = Result(status = 'Success',
                    title = None,
                    content= file_name,
                    url=param_url,
                    status_code=r.status_code,
                    scraped_date= dt.now().strftime("%d/%m/%Y"),
                    scraped_time=dt.now().strftime("%H:%M:%S"),
                    content_type=r.headers['content-type'])
        else:
            data = Result(status = 'Error',
                    title = None,
                    content= None,
                    url=param_url,
                    status_code=r.status_code,
                    scraped_date= dt.now().strftime("%d/%m/%Y"),
                    scraped_time=dt.now().strftime("%H:%M:%S"),
                    content_type=r.headers['content-type'])
     
    except Exception as e:
            data = Result(status = 'Error',
                    title = None,
                    content= str(e),
                    url=param_url,
                    status_code=None,
                    scraped_date= dt.now().strftime("%d/%m/%Y"),
                    scraped_time=dt.now().strftime("%H:%M:%S"),
                    content_type=None)
    finally:
        logger.debug(f'{time.perf_counter()} - PID- {os.getpid()}, url- {param_url}')
        return data
   
"""_summary_
Generate a name for pdf file to be saved
parameters: URL
return: file name
"""
def get_pdf_file_name(url):
    res = os.path.basename(urlparse(url).path)
    return res if res.endswith(".pdf") else ''.join((res, ".pdf"))

"""_summary_
Get cleaned text using justtext package
parameters: contents returned from requests
return: cleand text
"""
def get_cleaned_text(request_content):
    text = jt.justext(request_content, jt.get_stoplist("English"))
    return "".join([para.text.replace('\n', '') if not para.is_boilerplate else '' for para in text])

"""_summary_
Download contents of a website using request
parameters: URL
return: Results object
"""
def extract_data(param_url):
    try:
        r = requests.get(param_url, headers = get_random_headers())
        data = Result(status = 'Success',
                        title = get_title(r.content) if len(r.content) > 0 else None,
                        # content= r.content, 
                        content= get_cleaned_text(r.content),
                        url=param_url,
                        status_code=r.status_code,
                        scraped_date= dt.now().strftime("%d/%m/%Y"),
                        scraped_time=dt.now().strftime("%H:%M:%S"),
                        content_type=r.headers['content-type'])
    except Exception as e:
        r = None
        data = Result(status = 'Error',
                      title = None,
                        content= str(e),
                        url=param_url,
                        status_code=404,
                        scraped_date= dt.now().strftime("%d/%m/%Y"),
                        scraped_time=dt.now().strftime("%H:%M:%S"),
                        content_type=None)

    finally:
        logger.info(f'{time.perf_counter()} - PID- {os.getpid()}, url- {param_url}')
        return data

"""_summary_
Remove duplicates from the input set
parameters: input url list
return: Deduplicated url list
"""
def clean_input_data(input_path):
    return pd.read_csv(input_path).drop_duplicates()

"""_summary_
Write CSV files using given headers and list of results objects
parameters: headers, list of results objects
return: None
"""
def write_csv(file, dataLists, header):
    with open(output_path+ r'/'+file, "w", newline="", encoding=ENCODE) as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        [writer.writerow(list) for list in dataLists] 

"""_summary_
The main function - 
parameters: None
return: None
"""
if __name__ == "__main__":
    # Reads the input urls from the csv
    url_list = clean_input_data(input_path)
    
    # Use 1% sample for testing
    # url_list = url_list.sample(frac =.01)
    
    # Split URL list based on PDF files and HTML targets 
    # pdf_filter = url_list['URL'].str.contains(".pdf")
    pdf_filter = url_list['URL'].str.contains(".pdf", case=False)
    pdf_list = url_list[pdf_filter]['URL']
    url_list = url_list[~pdf_filter]['URL']
    
    # Testing purpose only    
    logger.info('pdf_list -'+ str(len(pdf_list)))
    logger.info('url_list -'+ str(len(url_list)))
    
    # start the timer to measure the time taken to execute
    start_time = time.perf_counter()
    
    # output results object holders
    url_results_list = []
    pdf_results_list = []
    
    # Initiate parallel processing 
    # Default value for max_workers is number of processors on the machine
    # with ProcessPoolExecutor(max_workers=1) as exec:
    with ProcessPoolExecutor() as exec:
        try:
            # submit url requests and pdf files requests
            url_data = [exec.submit(extract_data, url) for url in url_list]
            pdf_data = [exec.submit(extract_pdf_data, url) for url in pdf_list]

            # collect url request outputs
            for url_result in url_data:
                url_results_list.append(url_result.result())

            # collect pdf request outputs
            for pdf_result in pdf_data:
                pdf_results_list.append(pdf_result.result())
        
        # catching any generic exceptions
        except Exception as e:
            logger.debug(e)
    
    # logging number of results       
    logger.debug(f'No of URL results = {len(url_results_list)}')
    logger.debug(f'No of pdf results = {len(pdf_results_list)}')
    
    # Record the finish time and log the total execution time
    finish_time = time.perf_counter()
    logger.debug("Execution completed in {} seconds ".format(finish_time-start_time))
    
    # Define headers for output CSV file
    # header = ['scraped_date', 'scraped_time','status', 'status_code', 'url', 'content_type', 'title', 'content']
    header = ['url', 'content']
    
    # Write to CSV 
    write_csv('data.csv', pdf_results_list + url_results_list, header)