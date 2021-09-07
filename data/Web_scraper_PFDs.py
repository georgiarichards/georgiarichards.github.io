#!/usr/bin/env python
# coding: utf-8

# # Scraping the Courts and Tribunals Judiciary Website to collect Prevent Future Death (PFD) reports 

# In[1]:


from requests import get
from requests import ConnectionError
from bs4 import BeautifulSoup
import re
from time import sleep
from time import time
import csv
import pandas as pd

from tqdm.auto import tqdm
    
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
 
def get_url(url):
    response = get(url, verify = False)
    html = response.content
    soup = BeautifulSoup(html, "html.parser")
    return soup

def retries(record_url, tries=3):
    for i in range(tries):
        try:
            soup = get_url(record_url)
            return soup
        except (ConnectionError, SSLError):
            if i < tries - 1:
                sleep(2)
                continue
            else:
                return 'Con error'


# # Scraper starts here - last run on Monday, 06 Sept 2021, 9.06 am.

# In[2]:


#Your second value in "range" will be one more than the number of pages that exist on the wesbite
pages = list(range(1,372))

#This loops through all the pages to get the URLs to individual records
page_string = 'https://www.judiciary.uk/subject/prevention-of-future-deaths/page/{}/'
record_urls = []
for page in tqdm(pages):
    soup = get_url(page_string.format(str(page)))
    h5s = soup.find_all('h5', {'class': 'entry-title'})
    for h5 in h5s:
        record_urls.append(h5.a.get('href'))


# Here we check how many records (i.e. cases) were pulled from the urls & the first and last case

# In[3]:


len(record_urls)


# In[4]:


record_urls[0]


# In[5]:


record_urls[-1]


# Here is my second loop. This will go through the lists of URLs I just created above to visit each individual record and pull out and store the text data (info on the decreased/case) and the PDF URL I will use later

# In[6]:


def error_details(e_dict, record_count, record_url, details):
    e_dict['index'] = record_count
    e_dict['url'] = record_url
    e_dict['reason'] = details
    return e_dict


# In[8]:


reg_exp = re.compile(r"’s\s|s\s|'s\s")
text_cats = ['Date of report', 'Ref', 'Deceased name', 'Coroner name', 'Coroner Area', 'Category', "This report is being sent to"]
#First, I create two lists, one for the PDFs and one for the text data
record_text = []
pdf_urls = []
ref_list = []
#I want to loop through each URL & pull out the death information and pdf link for downloading
error_catching = []

record_count = 0
for record_url in tqdm(record_urls):
    try:
        error_dict = {}
        #Calling the retries function
        soup = retries(record_url, tries=5)
        
        if soup == 'Con error':
            print(f"{record_url} could not connect")
            error_catching.append(error_details(error_dict, record_count, record_url, 'Connection Error'))
            record_count +=1
            continue

        #This gets all the text fields from the website to work with
        death_info = soup.find('div', {'class':'entry-content'}).find_all('p')
        
        if not death_info:
            print(f"{record_url} produced no data")
            error_catching.append(error_details(error_dict, record_count, record_url, 'No Text Loaded'))
            record_count +=1
            continue
            
        #Our dictionary that will hold all of the text information that we will eventually append to "record_text"
        blankdict = {}
        
        #This is to handle 1 annoying record with messed up html tags
        if record_url == 'https://www.judiciary.uk/publications/roadsafety/':
            strong = death_info[0].find_all('strong')
            heads = ['date_of_report', 'ref', 'deceased_name', 'coroner_name', 'coroner_area', 'category']
            for st, h in zip(strong,heads):
                blankdict[h] = st.next_sibling.replace(':','').replace('Ref','').strip()
        #And another record with wonky html
        elif record_url == 'https://www.judiciary.uk/publications/helen-sheath/':
            brs = death_info[0].text.split('\n')
            vals = []
            for b in brs:
                vals.append(b.split(':'))
            for v in vals:
                if v[0] == "Coroners name":
                    alt = "coroner_name"
                    blankdict[alt] = v[1].strip().replace('\n','')
                elif v[0] == "Coroners Area":
                    alt = "coroner_area"
                    blankdict[alt] = v[1].strip().replace('\n','')
                else:
                    blankdict[v[0].strip().replace(' ','_').lower()] = v[1].strip().replace('\n','')
        else:        
            #looping through all of the text categories for handling
            for p in death_info:
                #This checks for blank fields and if there is nothing, it skips it
                if p.text.strip() == '':
                    pass
                #This checks for our "Normal" case in which a colon exists and the category is one of the ones we 
                #pre-specified above in the "text_cats" list
                #We also need to account here for one strange record for "Rebecca Evans" which has a weird text error
                #That we manually correct for
                elif ':' in p.text and p.text.split(':')[0] in text_cats and not 'Rebecca-EvansR.pdf' in p.text:
                    #Simply assigning the key and value from strings on either side of the colon, making everything 
                    #lower case and replacing spaces with underscores and also removing any stray semi-colons
                    text_list = p.text.split(':')
                    blankdict[text_list[0].strip().replace(' ','_').lower()] = text_list[1].strip().replace('\n','').replace('\xa0','')

                elif 'Rebecca-EvansR.pdf' in p.text:
                    #This deals with that singular odd record that currently exists as of 8 Nov 2019
                    blankdict['category'] = p.text.split(':')[1].strip().replace('\n','')
                    
                elif ':' not in p.text:
                    #If the string doesn't have a colon, we can't split on it so have to get it into dictionary format
                    #Using an alternate method that counts the length of the thing
                    if any(x in p.text for x in text_cats):
                        t = [x for x in text_cats if x in p.text][0]
                        l = len(t)
                        blankdict[t.replace(' ','_').lower()] = p.text[l+1:].replace('\n','').replace('\xa0','')
                    elif 'Coroners Area' in p.text:
                        blankdict['coroner_area'] = p.text[13:].strip().replace('\n','').replace('\xa0','')
                    else:
                        print("Something we haven't accounted for has happened")

                elif p.text.strip().count(":") == 2:
                    #This corrects for one odd record in which there are 2 colons but should generalize to fix it for
                    #any time this could happen, so long as it happens in the same way
                    text_list = p.text.split(':')
                    new_string = text_list[0] + text_list[1]
                    new_name = re.sub(reg_exp, ' ', new_string).strip()
                    blankdict[new_name.replace(' ','_').lower()] = text_list[2].strip().replace('\n','').replace('\xa0','')

                elif ':' in p.text and p.text.split(':')[0] not in text_cats:
                    #Some field names are in the form of "name_of_decesased" or "name_of_coroner" or are plural/
                    #possessive so this smashes those into our preferred naming formats
                    if 'Name of' in p.text:
                        all_text = p.text.split(':')
                        key_name = all_text[0].split(' ')
                        blankdict[key_name[2].strip() + '_name'] = all_text[-1].strip()
                    else:    
                        new_name = re.sub(reg_exp, ' ', p.text)
                        text_list = new_name.split(':')
                        blankdict[text_list[0].strip().replace(' ','_').lower()] = text_list[1].strip().replace('\n','').replace('\xa0','')
        blankdict['url'] = record_url
        
        #A small little check for duplicated ref names
        try:
            if not blankdict['ref']:
                pass
            elif blankdict['ref'] in ref_list:
                blankdict['ref'] = blankdict['ref'] + 'A'
            ref_list.append(blankdict['ref'])
        except KeyError:
            blankdict['ref'] = ''
            
        #This appends the final dict to the list
        record_text.append(blankdict)
        
        #this is a seperate process to get the PDF URLs (no matter how many there are) and adds them to their own list   
        urls = soup.find_all('li', {'class':'pdf'})
        pdf_list = []
        for url in urls:
            pdf_list.append(url.findNext('a').get('href'))
        pdf_urls.append(pdf_list)
        
        record_count += 1
        
    except Exception as e:
        import sys
        error_desc = f"{str(e)} occurred for {record_url} when trying to work with {p}"
        print(error_desc)
        error_catching.append(error_details(error_dict, record_count, record_url, error_desc))
        
        #Saving this in case we don't like the error catching.
        #import sys
        #raise type(e)(str(e) + '\n' + 'Error for Record: {}, Field: {}'.format(record_url, p)).with_traceback(sys.exc_info()[2])


# Here is the third loop to save the PDFs using the deceased Ref as the file name

# In[9]:


#Any errors should print out above, but you can also check the error_catching dict
#Here we just turn it into a dataframe quickly to easily view

error_df = pd.DataFrame(error_catching)
error_df


# In[10]:


def save_file(path_string, name_string):
    with open(path_string.format(name_string), 'wb') as d:
        d.write(myfile.content)

save_path = '/Users/georgiarichards/Desktop/Python/PFDs opioids/All_PDFs8/{}.pdf'

potential_names = ['ref', 'deceased_name', 'date_of_report']

record_count = 0
#This is the final scrape to actually get the URLs and change the name (when possible) to the refs
for r_t, p_u in zip(tqdm(record_text), pdf_urls):
    if not p_u:
        #If there is no pdf at all, we skip it.
        continue
    else:
        #All this does is gets the PDF and downloads it and names it after the reg
        #It looks scary and complicated but all it is doing is varying the name in the case of multiple PDFs
        #Or naming it for the deceased person if there is no Ref value
        #If there is a pdf but no ref or deceased name, this will throw an error and we can adjust.
        try:
            counter = 0
            if len(p_u) > 1:
                for p in p_u:
                    if counter == 0:
                        myfile = get(p)
                        named = False
                        for x in potential_names:
                            try:
                                if r_t[x]:
                                    save_file(save_path, r_t[x])
                                    counter +=1
                                    named = True
                                    break
                                else:
                                    continue
                            except KeyError:
                                continue
                        if not named:       
                            save_file(save_path, 'check_record_{}'.format(record_count))
                            counter +=1

                    else:
                        myfile = get(p)
                        named = False
                        for x in potential_names:
                            try:
                                if r_t[x]:
                                    save_file(save_path, r_t[x] + '_{}'.format(counter))
                                    counter +=1
                                    named = True
                                    break
                                else:
                                    continue
                            except KeyError:
                                continue
                        if not named:
                            save_file(save_path, 'check_record_{}_{}'.format(record_count, counter))
                            counter +=1
                                    
            else:
                myfile = get(p_u[0])
                named = False
                for x in potential_names:
                    try:
                        if r_t[x]:
                            save_file(save_path, r_t[x])
                            named = True
                            break
                        else:
                            continue
                    except KeyError:
                        continue
                if not named:       
                    save_file(save_path, 'check_record_{}'.format(record_count))
            
            record_count += 1
        
        except Exception as e:
            import sys
            if r_t['ref']:
                raise type(e)(str(e) + '\n' + 'Error for Record: {}'.format(r_t['ref'])).with_traceback(sys.exc_info()[2])
            else:
                raise type(e)(str(e) + '\n' + 'Error for Record Number: {}'.format(record_count)).with_traceback(sys.exc_info()[2])


# This is my final step that puts the text data (info on the deceased/case) into a csv file & adds the date it was pulled

# In[11]:


from datetime import date

headers = ['date_of_report', 'date_of_reports', 'ref', 'deceased_name', 'deceased_names', 'coroner_name', 'coroner_area', 'category', 'this_report_is_being_sent_to', 'these_report_are_being_sent_to', 'url']

with open('death_info_{}.csv'.format(date.today()), 'w', newline='', encoding='utf-8') as deaths_csv:
    writer = csv.DictWriter(deaths_csv, fieldnames=headers)
    writer.writeheader()
    for record in record_text:
        if record == {}:
            pass
        else:
            writer.writerow(record)


# This is an addition few steps to check what differences there are from the June 2021 records 

# In[12]:


import os

pdfs7 = os.listdir('All_PDFs7')
pdfs8 = os.listdir('All_PDFs8')

new_not_old = set(pdfs8).difference(pdfs7)

new_not_old_list = list(new_not_old)
new_not_old_list.sort()
new_not_old_list


# In[13]:


len(new_not_old_list)


# In[14]:


sep21 = pd.read_csv('death_info_2021-09-07.csv')
jun21 = pd.read_csv('death_info_2021-06-28.csv')


# In[15]:


cols = list(jun21.columns)
merged = sep21.merge(jun21, on=cols, how='left', indicator=True)


# In[16]:


l_only = merged[merged['_merge'] == 'left_only']


# In[17]:


len(l_only)


# In[18]:


l_only


# In[19]:


l_only.to_csv(r'death_info_newsep21.csv')


# Data processing for website 

# In[20]:


sep_names = pd.read_csv('death_info_2021-09-07.csv')
sep_names.head()


# In[21]:


sep_names['deceased_name'] = sep_names['deceased_name'].fillna("")


# In[22]:


sep_names['deceased_name'] = sep_names['deceased_name'].apply(lambda x: ''.join(i[0] for i in x.split()))


# In[23]:


sep_names['deceased_name'] = sep_names['deceased_name'].str.replace('\W', '')


# In[24]:


sep_names.head()


# In[25]:


sep_names.to_csv('death_info_2021-09-07_processed.csv')


# In[ ]:




