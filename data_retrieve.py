import pymongo
from pymongo import MongoClient
from bs4 import BeautifulSoup
import urllib
import json
import sys
import datetime
import re

#create database

client=MongoClient()


#data file in /var/lib/mongodb
db=client.doctissimo
categories=db.categories
topics=db.topics
messages=db.messages

now_tag=datetime.datetime.now() #test for new message, compare with database last_date

#retrieve data from forum doctissimo

#-------------------------------------------------CATEGORY LIST-------------------------------------------------------------------------------
category_page=urllib.request.urlopen("http://forum.doctissimo.fr/psychologie/liste_categorie.htm").read()
soup=BeautifulSoup(category_page,"lxml")

link_list=[]

for category in soup.findAll("tr",{"class":"cat"}):

	title=category.find("a",{"class":"cCatTopic"})
	msg_volume=category.find("td",{"class":"catCase2"})
	link=title["href"]
	link_list.append(link)



	#write to mongodb
	post={"title":title.text,"msg_volume":msg_volume.text,"link":link}
	categories.insert_one(post)

#remove first link (forum faqs)
link_list.pop(0)
print("Categories have been retrieved")

#-------------------------------------------------TOPIC LIST----------------------------------------------------------------------------------
	
for link in link_list:

	data=urllib.request.urlopen(link).read()
	soup=BeautifulSoup(data,"lxml")
	pages=soup.findAll("a",{"class":"cHeader"})
	page_list=[link]
	#check the last 10 pages // better to find last page
	try:
		for page in range(10):
			page_list.append(pages[page]['href'])
	except:
		print("less than 10 pages")

	#remove the search link
	page_list.pop(1)

	#empty topic list
	topic_list=[]

	for page in page_list:
		data=urllib.request.urlopen(page)
		soup=BeautifulSoup(data,"lxml")

		for topic in soup.findAll("tr",{"class":"sujet"}):
			#test the volume of answers, we only keep topic with 3 to 60 answers
			nb_answer=topic.find("td",{"class":"sujetCase7"}).text
			qty_answer=re.sub(r"\s+","",nb_answer)

			if (3<int(qty_answer)<50):

				topic_title=topic.find("td",{"class":"sujetCase3"}).text
				
				try:
					topic_url=topic.find("a")['href']
					topic_list.append(topic_url)
				except:
					topic_url="No url found"

				
				

				if topic.find("time"):
					last_date=topic.find("time")['datetime']
				else:
					last_date=datetime.datetime.now()

				post={"category_link":link,"topic_title":topic_title,"nb_answer":nb_answer,"last_date":last_date,"topic_url":topic_url}
				topics.insert_one(post)

			
#------------------------------------------------MESSAGE LIST--------------------------------------------------------------------------------------

		#messages to mongodb

			for topic in topic_list:
				data=urllib.request.urlopen(topic).read()
				soup=BeautifulSoup(data,"lxml")

				#find is there are several page for the topic

				for message in soup.findAll("div",{"itemprop":"comment"}):
					author=message.find("b",{"itemprop":"author"})
					date=message.find("span",{"class":"topic_posted"})

					#filter the corpus, removed forwarded messages and keep only new data
					corpus=message.find("div",{"itemprop":"text"})

					post={"topic":topic_title,"author":author.text,"date":date.text,"corpus":corpus.text}
					messages.insert_one(post)


				
