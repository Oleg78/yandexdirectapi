import time
import json
import requests
import math
import datetime
import logging
import asyncio
import aiohttp


class DirectAPIException(Exception):
    pass


class DirectAPIError(DirectAPIException):
    pass


class DirectAPIConnectionError(DirectAPIException):
    pass


class DirectAPI:
    """
    Main class
    """
    login = ''
    token = ''
    max_clients = 10
    res_name = {
        'bids': 'Bids',
        'ads': 'Ads',
        'adgroups': 'AdGroups',
        'campaigns': 'Campaigns',
        'keywords': 'Keywords'
    }

    def __init__(self, login, token):
        self.login = login
        self.token = token


class DirectAPI4(DirectAPI):
    """
    Yandex Direct API v4
    """
    url = 'https://api-sandbox.direct.yandex.ru/v4/json/'

    def run(self, data):
        """
        Query the Yandex API v4
        :param data: json data
        :return: json result
        """
        response = requests.post(self.url, data=json.dumps(data, ensure_ascii=False).encode('utf8'))
        if response.status_code != 200:
            raise DirectAPIConnectionError(response)
        res = response.json()
        if res.get('error_code', False):
            raise DirectAPIError(res)
        return res

    def get_balance(self):
        """
        Get the client balance
        :return: balance (json)
        """
        data = {
            'method': 'GetClientInfo',
            'token': self.token,
            'locale': 'ru',
            'param': [self.login]
        }
        return self.run(data)

    def create_report(self, phrase):
        """
        Create the wordstat report
        :param phrase: phrase for the report
        :return: report id
        """
        data = {
            "method": "CreateNewWordstatReport",
            'token': self.token,
            'locale': 'ru',
            "param": {
                "Phrases": [phrase],
                "GeoID": [225]  # Россия
            }
        }
        res = self.run(data)
        try:
            return res["data"]
        except KeyError:
            raise Exception(res)

    def get_report(self, report_id):
        """
        Get the processed report
        :param report_id: report id
        :return: SearchedWith result (list of dicts {'Phrase': str, 'Shows': int})
        """
        data = {
            "method": "GetWordstatReport",
            'token': self.token,
            'locale': 'ru',
            "param": report_id
        }

        while True:
            res = self.run(data)
            try:
                return res["data"][0]["SearchedWith"]
            except KeyError:
                time.sleep(1)

    def delete_report(self, report_id):
        """
        Delete existing report
        :param report_id: report id
        :return: None
        """
        data = {
            "method": "DeleteWordstatReport",
            'token': self.token,
            'locale': 'ru',
            "param": report_id
        }
        self.run(data)

    def delete_all_reports(self):
        """
        Delete all the existing reports
        :return: None
        """
        data = {
            "method": "GetWordstatReportList",
            'token': self.token,
        }
        res = self.run(data)
        try:
            for report in res["data"]:
                self.delete_report(report['ReportID'])
        except KeyError:
            pass


class DirectAPI5(DirectAPI):
    """
    Yandex Direct API v5 Class
    """
    # url = 'https://api-sandbox.direct.yandex.ru/json/v5/'
    url = 'https://api.direct.yandex.com/json/v5/'
    units = ''

    def run(self, address, data):
        """
        Query the Yandex API v5
        :param address: address (method)
        :param data: json data
        :return: json result
        """
        # Just to avoid repetitions
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        res = loop.run_until_complete(self.async_run(address, data))
        return res

    async def async_run(self, address, data, limited_by=0):
        """
        Query the Yandex API v5 (asynchronous version)
        :param address: address (method)
        :param data: json data
        :param limited_by: offset for the next page (recursive call)
        :return: json result
        """
        headers = {'Authorization': 'Bearer ' + self.token,
                   'Client-Login': self.login,
                   'Accept-Language': 'ru'}
        res = {}
        if limited_by:
            logging.info('Recursive get with {} offset'.format(limited_by))
            try:
                data['params'].update({"Page": {"Offset": limited_by}})
            except KeyError:
                raise DirectAPIError('Got limited_by={} without "Params" in data'.format(limited_by))

        async with aiohttp.ClientSession() as session:
            async with session.post(self.url + address,
                                    headers=headers,
                                    data=json.dumps(data, ensure_ascii=False).encode('utf8')) as response:
                if response.status != 200:
                    raise DirectAPIConnectionError(response)
                res = await response.json()
                if res.get('error', False):
                    raise DirectAPIError(res)
                curr_limited_by = res['result'].get('LimitedBy', 0)
                if curr_limited_by:
                    next_page = await self.async_run(address, data, curr_limited_by)
                    res['result'][self.res_name[address]] += next_page['result'][self.res_name[address]]
                self.units = response.headers.get('Units')
                logging.info('Request: {}/{} Units: {}'.format(address, data.get('method', 'report'), self.units))
        return res

    def get_campaigns(self, campaigns):
        """
        Get the campaigns
        :param campaigns: To get all the campaigns set the 'campaigns' parameter to None
               Otherwise pass the list of the campaign Ids
        :return: dict of the campaigns {CampaignId: Campaign}
        """
        if campaigns:
            selection_criteria = {
                    "Ids": campaigns
            }
        else:
            selection_criteria = {}
        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": selection_criteria,
                "FieldNames": [
                    "Id",
                    "Name",
                    "State",
                    "DailyBudget",
                    "Funds",
                    "Statistics",
                    "Type"
                ],
                "TextCampaignFieldNames": [
                    "RelevantKeywords",
                    "Settings",
                    "BiddingStrategy"
                ]
            }
        }
        res = self.run('campaigns', data)
        return {campaign['Id']: campaign for campaign in res['result']['Campaigns']}

    def get_campaign_groups(self, campaign_id):
        """
        Get the campaign's groups
        :param campaign_id: campaign id
        :return: dict of the campaign groups {GroupId: AdGroup}
        """
        loop = asyncio.get_event_loop()
        res = loop.run_until_complete(self.async_get_campaign_groups(campaign_id))
        return res

    async def async_get_campaign_groups(self, campaign_id):
        """
        Get the campaign's groups
        :param campaign_id: campaign_id
        :return: dict of the campaign groups {GroupId: AdGroup}
        """
        selection_criteria = {
            "CampaignIds": [campaign_id],
            "Statuses": ["ACCEPTED"]
        }
        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": selection_criteria,
                "FieldNames": [
                    "CampaignId",
                    "Id",
                    "Name",
                    "Status",
                    "Type"
                ]
            }
        }
        res = await self.async_run('adgroups', data)
        logging.info('Got {} groups for {} campaign'.format(len(res['result']['AdGroups']), campaign_id))
        return {ad_group['Id']: ad_group for ad_group in res['result']['AdGroups']}

    async def async_get_campaign_active_groups(self, campaign_id):
        """
        Get active ad groups for a campaign
        :param campaign_id: Campaign Id
        :return: set of the campaign active group ids
        """
        ads = await self.async_get_campaigns_active_ads([campaign_id])
        group_ids = set()
        for ad in ads.values():
            group_ids.add(ad["AdGroupId"])
        return group_ids

    async def async_get_groups_active_ads(self, group_ids):
        """
        Get the group's ads
        :param group_ids: List of group ids
        :return: dict of the group's ads {AdId: Ad}
        """
        selection_criteria = {
            "AdGroupIds": group_ids,
            "States": ["ON"],
            "Statuses": ["ACCEPTED"]
        }
        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": selection_criteria,
                "FieldNames": [
                    "Id",
                    "AdGroupId",
                    "CampaignId"
                ]
            }
        }
        res = await self.async_run('ads', data)
        logging.info('Got {} ads for {} group'.format(len(res['result']['AdGroups']), len(group_ids)))
        return {ad['Id']: ad for ad in res['result']['Ads']}

    async def async_get_campaigns_active_ads(self, campaign_ids):
        """
        Get the campaign's groups
        :param campaign_ids: List of campaign ids
        :return: dict of the campaign's ads {AdId: Ad}
        """
        selection_criteria = {
            "CampaignIds": campaign_ids,
            "States": ["ON"],
            "Statuses": ["ACCEPTED"]
        }
        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": selection_criteria,
                "FieldNames": [
                    "Id",
                    "AdGroupId",
                    "CampaignId"
                ]
            }
        }
        res = await self.async_run('ads', data)
        ret = {}
        try:
            logging.info('Got {} ads for {} campaigns'.format(len(res['result']['Ads']), len(campaign_ids)))
            ret = {ad['Id']: ad for ad in res['result']['Ads']}
        except KeyError:
            logging.info('Got 0 ads for {} campaigns ({})'.format(len(campaign_ids), campaign_ids))
        return ret

    def get_groups_bids(self, group_ids):
        """
        Get bids for a list of groups
        :param group_ids: group ids
        :return: dict of group bids {KeywordId: Bid}
        """
        max_groups = 1000 * self.max_clients  # 10000 groups per request * clients number
        ret = {}
        for g in range(int(math.ceil(len(group_ids) / max_groups))):
            curr_group_ids = group_ids[g * max_groups:g * max_groups + max_groups]
            futures = []
            groups_per_client = int(math.ceil(len(curr_group_ids) / self.max_clients))
            for i in range(self.max_clients):
                logging.info('{}. Get bids for {}-{} groups'.format(i + 1,
                                                                    i * groups_per_client,
                                                                    i * groups_per_client + groups_per_client))
                req_group_ids = curr_group_ids[i * groups_per_client:i * groups_per_client + groups_per_client]
                data = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {
                            "AdGroupIds": req_group_ids
                        },
                        "FieldNames": [
                            "CampaignId",
                            "KeywordId",
                            "Bid",
                            "ContextBid",
                            "CompetitorsBids",
                            "SearchPrices",
                            "MinSearchPrice",
                            "CurrentSearchPrice",
                        ]
                    }
                }
                futures.append(self.async_run('bids', data))
            loop = asyncio.get_event_loop()
            res = loop.run_until_complete(asyncio.gather(*futures))
            for r in res:
                if r:
                    try:
                        ret.update({bid['KeywordId']: bid for bid in r['result']['Bids']})
                        logging.info('Got {} bids'.format(len(r['result']['Bids'])))
                    except KeyError:
                        logging.error('Something strange in get_group_bids result {}'.format(res))
        logging.info('Finally got {} bids for {} groups'.format(len(ret), len(group_ids)))
        return ret

    async def async_get_groups_bids(self, group_ids):
        """
        Get bids for a list of groups (asynchronous)
        :param group_ids: group ids
        :return: dict of group bids {KeywordId: Bid}
        """
        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "AdGroupIds": group_ids
                },
                "FieldNames": [
                    "CampaignId",
                    "KeywordId",
                    "Bid",
                    "ContextBid",
                    "CompetitorsBids",
                    "SearchPrices",
                    "MinSearchPrice",
                    "CurrentSearchPrice",
                ]
            }
        }
        res = await self.async_run('bids', data)
        try:
            ret = {bid['KeywordId']: bid for bid in res['result']['Bids']}
            logging.info('Got {} bids for {} groups'.format(len(ret), len(group_ids)))
        except KeyError:
            ret = {}
            logging.error('Something strange in get_campaign_group_bids result {}'.format(res))
        return ret

    def get_campaign_bids(self, campaign_id):
        """
        Get the campaign's bids
        :param campaign_id: campaign id
        :return: dict of campaign bids {KeywordId: Bid}
        """
        loop = asyncio.get_event_loop()
        res = loop.run_until_complete(self.async_get_campaign_bids(campaign_id))
        return res

    async def async_get_campaign_bids(self, campaign_id):
        """
        Get the campaign's bids (asynchronous)
        :param campaign_id: campaign id
        :return: dict of campaign bids {KeywordId: Bid}
        """
        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "CampaignIds": [campaign_id]
                },
                "FieldNames": [
                    "CampaignId",
                    "KeywordId",
                    "Bid",
                    "ContextBid",
                    "CompetitorsBids",
                    "SearchPrices",
                    "MinSearchPrice",
                    "CurrentSearchPrice",
                ]
            }
        }
        res = await self.async_run('bids', data)
        ret = None
        try:
            ret = {bid['KeywordId']: bid for bid in res['result']['Bids']}
        except KeyError:
            logging.error('Something strange in get_campaign_bids result {}'.format(res))
        return ret

    def get_campaign_active_bids(self, campaign_id):
        """
        Get the campaign's bids for active groups
        :param campaign_id: campaign id
        :return: dict of campaign bids {KeywordId: Bid}
        """
        loop = asyncio.get_event_loop()
        res = loop.run_until_complete(self.async_get_campaign_active_bids(campaign_id))
        return res

    async def async_get_campaign_active_bids(self, campaign_id):
        """
        Get the campaign's bids for active groups (asynchronous)
        :param campaign_id: campaign id
        :return: dict of campaign bids {KeywordId: Bid}
        """
        groups = await self.async_get_campaign_active_groups(campaign_id)
        if groups:
            res = await self.async_get_groups_bids(list(groups))
        else:
            res = {}
        return res

    def get_campaigns_bids(self, campaign_ids):
        """
        Get bids for each campaign (async request per one campaign)
        :param campaign_ids: list of the campaign ids
        :return: number of all the bids
        """
        start = datetime.datetime.now()
        logging.info('Start getting bids on '.format(start))
        ret = {}
        for i in range(int(math.ceil(len(campaign_ids) / self.max_clients))):
            logging.info('{}. Get {}-{} campaigns'.format(i + 1,
                                                          i * self.max_clients,
                                                          i * self.max_clients + self.max_clients))
            req_campaign_ids = campaign_ids[i * self.max_clients:i * self.max_clients + self.max_clients]
            futures = []
            for campaign_id in req_campaign_ids:
                logging.info('Create an async task for {} campaign'.format(campaign_id))
                futures.append(self.async_get_campaign_active_bids(campaign_id))
            loop = asyncio.get_event_loop()
            res = loop.run_until_complete(asyncio.gather(*futures))
            for r in res:
                if r:
                    ret.update(r)

        logging.info('End getting bids on {} / Total {}'.format(datetime.datetime.now(),
                                                                datetime.datetime.now() - start))
        return ret

    def set_bids(self, bids):
        """
        Set new bids
        :param bids: list of dicts: [{KeywordId: bid}]
        :return: number of the populated bids
        """
        # max number of bids per request
        max_bids = 10000
        ret = 0
        logging.info('Total new bids: {}'.format(len(bids)))
        for i in range(int(math.ceil(len(bids) / max_bids))):
            req_bids = bids[i * max_bids:i * max_bids + max_bids]
            logging.info('{} iteration: set {} bids'.format(i, len(req_bids)))
            data = {
                "method": "set",
                "params": {
                    "Bids": req_bids
                }
            }
            r = self.run('bids', data)
            ret += len(r['result']['SetResults'])
        return ret
