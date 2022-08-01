import requests
import pandas as pd
from bs4 import BeautifulSoup 
from urllib.request import urlopen
import regex as re
import mysql.connector
# import requests
# import pandas as pd
# import requests
# from bs4 import BeautifulSoup 
# from urllib.request import urlopen
# import regex as re
# import mysql.connector

#mysql.connector==2.2.9
db_sludinajums = pd.DataFrame()
db_ipasibas = pd.DataFrame()
db = pd.DataFrame()
punctuation = r"""!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~"""
##iegut visus sludinajumus
#################################################################
def iegutsludinajumus():
  global db
  HEADERS = {
      'Accept':'image/webp,image/png,image/svg+xml,image/*;q=0.8,video/*;q=0.8,*/*;q=0.5',
      'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15'
  }
  url ="https://www.ss.lv/lv/real-estate/flats/today-2/page1.html"
  db = pd.DataFrame()
  i = 0

  while True:
    i += 1
    url = f"https://www.ss.lv/lv/real-estate/flats/today-2/page{i}.html"
    response = requests.get(url)
    #if response.url!='https://www.ss.lv/lv/real-estate/flats/today-2/':
    if i <4:
      page = urlopen(url)
      html = page.read().decode("utf-8")
      soup = BeautifulSoup(html, "html.parser")
      texts = soup.find_all('td',class_='msga2')
      for text in texts:
        kuku = text.find('a')
        if kuku !=None :
          new_row= {
                  'url':'https://www.ss.lv'+kuku['href'],
                  'id':kuku['id']
              }
          db = db.append(new_row, ignore_index=True)
          


      
    else:
      break
##############################################################

#####meklet tekstu###########################################
def find_between( s, first, last ):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return "kļūda"
#############################################################


def ierakstit(links,id):
  global db
  global db_sludinajums
  global db_ipasibas
  url = links
  db_id=id
  page = urlopen(url)
  html = page.read().decode("utf-8")
  soup = BeautifulSoup(html, "html.parser")

  texts = soup.find_all('h2')
  for text in texts:
    veids=text.get_text()
    x = veids.split("/")
    db_sludinajumaveids = x[-1].strip() #iegūstam sludinājuma veidu!

  texts2 = soup.find_all("div",{"id":"msg_div_msg"})

  sludinajumatext = find_between(str(texts2),"</div>","<table")
  words = re.split(r'\W+', sludinajumatext)
  table = str.maketrans('', '', punctuation)
  stripped = [w.translate(table) for w in words]
  stripped.remove("")
  stripped.remove("br")
  db_sludinajumatext=stripped

  texts3 = soup.find("table",class_="options_list")
  new_row= {
                'url':url,
                'id':db_id,
                'adstype':db_sludinajumaveids,
                'adstext':db_sludinajumatext
            }
  db_sludinajums = db_sludinajums.append(new_row, ignore_index=True)
  for rinda in texts3.children:
    resultats=rinda.find_all("td",class_="ads_opt")
    for resul in resultats:
      #id = BeautifulSoup(resul, 'html.parser')
      db_ipasiba=resul.get('id')
      db_vertiba=resul.get_text()
      new_row= {
                
                'id':db_id,
                'pk_ipasibas':db_ipasiba,
                'vertext':db_vertiba
            }
      db_ipasibas = db_ipasibas.append(new_row, ignore_index=True)
  texts4=""
  texts4=soup.find("td",class_="ads_price")
  
  if not isinstance(texts4, type(None)): 
    txt=texts4.get_text()

    txt2 = txt.replace(" ", "")
    numeric_const_pattern = '[-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'
    rx = re.compile(numeric_const_pattern, re.VERBOSE)
    rezu=rx.findall(txt2)
    mervs = txt.split(" ")
    for merv in mervs:
      if not merv.isnumeric():
        mers = merv
        break
    db_cena=rezu[0]
    db_merv = mers
    db_ipasiba=texts4.get('id') 
    new_row= {
                  
                  'id':db_id,
                  'pk_ipasibas':db_ipasiba,
                  'vertext':db_cena,
                  'mervien':db_merv
              }
    db_ipasibas = db_ipasibas.append(new_row, ignore_index=True)
#########################################################################################################
def izietcaurisludinajumiem():
  global db
  global db_sludinajums
  global db_ipasibas
  for index, row in db.iterrows():
    i=0
    kopa = len(db)
    ierakstit(row['url'],row['id'])

###########################################################################################################

def izmainas():
  global db_sludinajums
  global db_ipasibas
  db_sludinajums['adstext_string'] = [' '.join(map(str, l)) for l in db_sludinajums['adstext']]
  db_sludinajums = db_sludinajums.drop('adstext', axis=1)
  db_ipasibas['mervien'] = db_ipasibas['mervien'].fillna("N/a")
  db_ipasibas['vertext']= db_ipasibas['vertext'].astype(str)
  db_ipasibas['mervien']= db_ipasibas['mervien'].astype(str)
############################################################################################################

def inserttodb():
  global db
  global db_ipasibas
  global db_sludinajums
  config = {
  'user': 'u648500775_admin',
  'password': '5V$0zx#31Ky',
  'host': 'sql506.main-hosting.eu',
  'database': 'u648500775_stock',
  'raise_on_warnings': True
}

  cnx = mysql.connector.connect(**config)


  cursor = cnx.cursor()
  sqlurlid= "INSERT INTO `urlid` (`url`, `id`) VALUES (%s, %s)" ##db
  sqlurlmain = "INSERT INTO `urlmain` (`url`, `adsid`, `adstype`,`adstext`) VALUES (%s,%s,%s,%s)" ##db_sludinajums
  sqlipasibas = "INSERT INTO `adsoption` (`adsid`, `pk_ipasiba`, `vertext`,`mervieniba`) VALUES (%s,%s,%s,%s)" ##db_sludinajums
  ### ierakstam db urlid
  for i,row in db.iterrows():
    cursor.execute(sqlurlid,tuple(row))
    cnx.commit()
  for i,row in db_sludinajums.iterrows():
    cursor.execute(sqlurlmain,tuple(row))
    cnx.commit()
  for i,row in db_ipasibas.iterrows():
    cursor.execute(sqlipasibas,tuple(row))
    cnx.commit()

  cursor.close()
  cnx.close()
############################################################################################################


def main():
  iegutsludinajumus()
  izietcaurisludinajumiem()
  izmainas()
  inserttodb()

##############################################################################################################

main()
