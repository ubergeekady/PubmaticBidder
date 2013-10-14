import sys
import random
import time  
import hashlib
import re
import json
import datetime
import base64
import os
import thread
import socket
import csv
import operator
import tornado.ioloop
import tornado.web
import tornado.httpclient
import tornado.options

from pyDes import *
from urlparse import urlparse
from tornado.web import asynchronous
from collections import defaultdict
from tornado.options import define, options

#Address of the forecasting server
UDP_IP = "46.137.241.79"
UDP_PORT = 5006

class MainHandler(tornado.web.RequestHandler):
    def get(self):
	global campaignData
	global bidCountIndex      
        start = time.time()
        postContent = self.request.body
        bidRequest = ssrtb_pb2.BidRequest()

        bid = True
        bidMicros=0
        code=""
        try:
            bidRequest.ParseFromString(postContent)
            ad = bidRequest.matching_ad_ids
            ad=ad[0]
            domain = re.sub('www.',r'',str(urlparse(bidRequest.url).netloc))
            country = bidRequest.user_geo_country.lower()

            try:
	      ronCampaigns = campaignData['display:roe']
	    except KeyError:
	      ronCampaigns = list()
	    try:
	      black = campaignData['display:roe:black:'+domain]
	    except KeyError:
	      black = list()

	    ronCampaigns = list(set(ronCampaigns) - set(black))			

	    try:
	      whiteCampaigns = campaignData['display:white:'+domain]
	    except KeyError:
	      whiteCampaigns = list()

	    campaigns = list(set(ronCampaigns+whiteCampaigns))

	    try:
	      geoCampaigns = campaignData['display:geo:'+country]
	    except KeyError:
	      geoCampaigns = list()
	      
	    campaigns = list(set(geoCampaigns) & set(campaigns))

	    size=str(bidRequest.ad_width)+"x"+str(bidRequest.ad_height)
	    try:
	      sizeCampaigns = campaignData['display:size:'+size]
	    except KeyError:
	      sizeCampaigns = list()
	      
	    campaigns = list(set(sizeCampaigns) & set(campaigns))

	    if(len(campaigns)>0):
	      camplist=[]
	      for camp in campaigns:
		  l = [camp, campaignData["display:campaign:"+str(camp)+":bid"],campaignData["display:campaign:"+str(camp)+":pacing"]]
		  camplist.append(l)
		  
	      camplist.sort(key=operator.itemgetter(1), reverse=True) # sorts the list in place decending by bids
	      
	      finalCampaign=0
	      for camp in camplist:
		  r=random.randrange(1,100)
		  if r<camp[2]:
		      finalCampaign=camp[0]
		      finalBid=camp[1]
		      break
		      
	      if finalCampaign>0:
		  banners = campaignData['display:campaign:'+str(finalCampaign)+':'+str(bidRequest.ad_width)+'x'+str(bidRequest.ad_height)]
		  randomBannerId = random.choice(banners)
		  bidMicros = finalBid * 1000000
		  info = base64.b64encode(json.dumps({'e':'openx','d':domain,'bid':randomBannerId,'cid':finalCampaign, 'b':finalBid, "s":bidRequest.user_geo_state,"country":country}))
		  info = info.replace("+","-").replace("/","_").replace("=","")
		  code='<iframe src="http://rtbidder.impulse01.com/serve?info='+info+'&p={WINNING_PRICE}&r={RANDOM}&red=" width="'+str(bidRequest.ad_width)+'" height="'+str(bidRequest.ad_height)+'" frameborder=0 marginwidth=0 marginheight=0 scrolling=NO></iframe>'
		  
        except:
            bid = False
            error = True
            print  sys.exc_info()

        if bid == False :
            bidMicros = 0
            code = ""
            
        print bidMicros
        print code
        print domain
        
        response = ssrtb_pb2.BidResponse()
        response.api_version = bidRequest.api_version
        response.auction_id = bidRequest.auction_id
        responseBid = response.bids.add()
        responseBid.matching_ad_id.campaign_id = int(ad.campaign_id)
        responseBid.matching_ad_id.placement_id = int(ad.placement_id)
        responseBid.matching_ad_id.creative_id = int(ad.creative_id)
        responseBid.cpm_bid_micros = int(bidMicros)
        responseBid.ad_code = code

        responseString = response.SerializeToString()

        self.write(responseString)
        timeTaken = time.time() - start

def autovivify(levels=1, final=dict):
    return (defaultdict(final) if levels < 2 else defaultdict(lambda: autovivify(levels - 1, final)))

	
#---------------------Refresh Campaign Index------------------------------------------------
def refreshCache():
    global campaignData
    http_client = tornado.httpclient.HTTPClient()
    try:
	response = http_client.fetch("http://terminal.impulse01.com:5003/index?channel=1")
	invertedIndex=json.loads(response.body)
    except:
        invertedIndex=dict()
    campaignData=invertedIndex
    print options.name+" Refreshed campaign inverted index from http://terminal.impulse01.com:5003/index?channel=1"
#-----------------------------------------------------------------------------------------------



#----------------------Initialize the Tornado Server --------------------------------
define("port", default=8888, help="run on the given port", type=int)
define("name", default="noname", help="name of the server")
define("refreshCache", default=10000, help="millisecond interval between cache refresh", type=int)
#sredisClient = tornadoredis.Client('cookie-tokyo.impulse01.com')
#redisClient.connect()
application = tornado.web.Application([(r".*", MainHandler),])
#-----------------------------------------------------------------------------------------------


#---------------------Construct Campaign Index------------------------------------------------
campaignData=dict()
http_client = tornado.httpclient.HTTPClient()
try:
    response = http_client.fetch("http://terminal.impulse01.com:5003/index?channel=1")
    invertedIndex=json.loads(response.body)
except:
    invertedIndex=dict()
campaignData=invertedIndex
print options.name+" Loaded campaign inverted index from http://terminal.impulse01.com:5003/index?channel=1"

#-----------------------------------------------------------------------------------------------

bidCountIndex = autovivify(6, int)

if __name__ == "__main__":
    tornado.options.parse_command_line()
    application.listen(options.port)
    tornado.ioloop.PeriodicCallback(refreshCache, options.refreshCache).start()
    tornado.ioloop.IOLoop.instance().start()     