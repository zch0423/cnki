# coding=utf-8
'''
    FileName      :data2mongo.py
    Author        :@zch0423
    Date          :Sep 7, 2022
    Description   :
    把jsonDone中导入到mongo cnki/info中
    字段包括journal, title, ID, discipline, fund, 
            issue, year, authors, institutions,
            keywords, abstract
'''
from multiprocessing.sharedctypes import Value
import os
import json
from unittest import result
from pymongo import MongoClient
from urllib import parse
from configparser import ConfigParser


def connectMongo(config_path):
    '''
    @Description
    connect to mongo through config
    ------------
    @Params
    config_path, str
    ------------
    @Returns
    Mongo client
    '''
    config = ConfigParser()
    config.read("config.ini", encoding="utf-8")
    config = config["mongo"]
    user = parse.quote_plus(config["user"])
    pwd = parse.quote_plus(config["pwd"])
    db = config["db"]
    client = MongoClient(
        "mongodb://{0}:{1}@localhost:27017/{2}".format(user, pwd, db))
    return client


def parseItem(item, journal):
    '''
    @Description
    解析每篇文章的info
    ------------
    @Params
    item, dict
    journal, str, 杂志名
    ------------
    @Returns
    dict
    '''
    temp = {}
    temp["journal"] = journal
    saved_keys = ["title", "paperID", "discipline", "fund"]
    for key in saved_keys:
        temp[key] = item[key]
    issue = item["yearMonth"]
    temp["issue"] = issue
    try:
        temp["year"] = int(issue[:4])
    except ValueError:
        temp["year"] = 0
    authors = []
    for id, author in item["authors"].items():
        authors.append({"authorID": id, "name": author})
    institutions = []
    for id, institution in item["institutions"].items():
        institutions.append({"instID": id, "institution": institution})
    temp["authors"] = authors
    temp["institutions"] = institutions
    temp["keywords"] = item["keywords"]
    temp["abstract"] = item["abstract"]
    return temp


def parseJson(path, journal):
    '''
    @Description
    parse one json file, for one journal
    ------------
    @Params
    path, str, file path
    journal, str, journal name
    ------------
    @Returns
    list, [{},{}]
    '''
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    results = []
    for key, item in data.items():
        results.append(parseItem(item, journal))
    return results


def main():
    '''
    @Description
    ------------
    @Params
    ------------
    @Returns
    '''
    config_path = "config.ini"
    client = connectMongo(config_path)
    db = client["cnki"]
    collection = db["info"]
    try:
        with os.scandir("data/jsonDone") as it:
            for entry in it:
                if not entry.name.endswith("json"):
                    continue
                results = parseJson(entry.path, entry.name[:-5])
                if results:
                    collection.insert_many(results)
    except Exception as e:
        print(e)
    client.close()
    
if __name__ == "__main__":
    main()
    