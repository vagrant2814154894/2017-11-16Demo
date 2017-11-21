﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using MongoDB.Driver;
using MySql.Data.MySqlClient;
using 微信数据入库.Etity;
using FlowDataModel;
using MongoDB.Bson;
using Newtonsoft.Json;
using MongoDB.Bson.IO;

namespace 微信数据入库
{
    class Program
    {

        static void Main(string[] args)
        {
            // 日志客户端
            AliLog.Logger log = new AliLog.Logger();
            var noCno = 0;//没有卡号的卡
            long totalData = 0;//总数据量
            try
            {

                //链接字符串
                //string conn = "mongodb://127.0.0.1:27017";
                string conn = "mongodb://root:Fxft2017@dds-bp104401edaca7e41.mongodb.rds.aliyuncs.com:3717,dds-bp104401edaca7e42.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-3015103";
                //指定的数据库
                string dbName = "CardInfo";
                //string dbName = "Test";
                // Mongo客户端
                MongoClient client;
                //当前操作数据库
                IMongoDatabase database;

                string conUser = "Server=fxftdatabase.mysql.rds.aliyuncs.com;Port=3306;initial catalog=base_user;uid=ckb_admin; pwd =ckbadmin;Allow User Variables=True;Convert Zero Datetime=True";
                string conTerminal = "Server=fxftdatabase.mysql.rds.aliyuncs.com;Port=3306;initial catalog=base_app;uid=ckb_admin; pwd =ckbadmin;Allow User Variables=True;Convert Zero Datetime=True";

                //string conUser = "Server=localhost;Port=3306;initial catalog=base_user;uid=sa; pwd =123456;Allow User Variables=True;Convert Zero Datetime=True";
                //string conTerminal = "Server=localhost;Port=3306;initial catalog=base_app;uid=sa; pwd =123456;Allow User Variables=True;Convert Zero Datetime=True";

                #region 测试使用
                //userIds[0].Id = 1000000000020041;
                #endregion

                var beginTime = DateTime.Now;
                log.Debug($"微信数据入库开始时间：{beginTime}");
                client = new MongoClient(conn);
                database = client.GetDatabase(dbName);
                var cardCollection = database.GetCollection<Card>("tblCard");//表
                var wechatCollection = database.GetCollection<WxInfo>("tblWX");//表


                ConcurrentDictionary<long, List<string>> numbers = new ConcurrentDictionary<long, List<string>>();
                ConcurrentDictionary<long, List<WxItem>> wxInfos = new ConcurrentDictionary<long, List<WxItem>>();
                List<Cno> cNo = null;

                using (IDbConnection connection = new MySqlConnection(conTerminal))
                {
                    cNo = connection.Query<Cno>("SELECT t.Number,t.UserId FROM user_terminal t ").ToList();
                    log.Debug($"从user_terminal表获取到的需要入库的卡共：{cNo.Count}张");
                    foreach (var numItem in cNo)
                    {

                        if (numbers.ContainsKey(numItem.UserId))
                        {
                            var existNums = numbers[numItem.UserId];
                            existNums.Add(numItem.Number);
                        }
                        else
                        {
                            List<string> nums = new List<string>();
                            nums.Add(numItem.Number);
                            numbers.TryAdd(numItem.UserId, nums);

                        }
                    }

                    var appId =
                        connection.Query<Wechat>("SELECT  t.OpenId,t.WxPublicNo,t.UserId FROM user_wechat t ")
                            .ToList();

                    foreach (var appItem in appId)
                    {
                        if (wxInfos.ContainsKey(appItem.UserId))
                        {
                            var existWxInfos = wxInfos[appItem.UserId];
                            foreach (var wxInfo in existWxInfos)
                            {
                                if (appItem.WxPublicNo == wxInfo.appId)//如果存在appId
                                {
                                    wxInfo.openId.Add(appItem.OpenId);
                                }
                                else
                                {
                                    WxItem wxItem = new WxItem();
                                    wxItem.appId = appItem.WxPublicNo;
                                    wxItem.openId = new List<string>();
                                    wxItem.openId.Add(appItem.OpenId);
                                    existWxInfos.Add(wxItem);
                                    break;
                                }
                            }

                        }
                        else
                        {
                            var wxItems = new List<WxItem>();
                            WxItem wxItem = new WxItem();
                            wxItem.appId = appItem.WxPublicNo;
                            wxItem.openId = new List<string>();
                            wxItem.openId.Add(appItem.OpenId);
                            wxItems.Add(wxItem);
                            wxInfos.TryAdd(appItem.UserId, wxItems);

                        }



                    }
                }

                long suctotalData = 0;
                long noData = 0;//在user_wechat表里无数据
                var pageSize = 10000;
                var total = cNo.Count;
                var pages = total / pageSize + (total % pageSize > 0 ? 1 : 0);

                for (int i = 0; i < pages; i++)
                {
                    var subDatas = cNo.Skip(i * pageSize).Take(pageSize);
                    List<WxInfo> wxInfoList = new List<WxInfo>();

                    foreach (var user in subDatas)
                    {

                        if (wxInfos.ContainsKey(user.UserId))
                        {
                            //从mong中找sim
                            BsonArray querys = new BsonArray();
                            querys.Add(new BsonDocument("cNo", user.Number));
                            querys.Add(new BsonDocument("iccid", user.Number));
                            querys.Add(new BsonDocument("imsi", user.Number));
                            BsonDocument query = new BsonDocument("$or", querys);
                            var card = cardCollection.Find(query).FirstOrDefault();
                            if (card != null)
                            {
                                WxInfo wxInfo = new WxInfo();
                                wxInfo.cNo = card.cNo;
                                wxInfo.wxInfo = wxInfos[user.UserId];
                                wxInfoList.Add(wxInfo);
                            }
                            else
                            {
                                log.Debug($"{user.Number}在库里没有对应的卡号,目前没有卡号的共{++noCno}个");
                            }
                        }

                        else
                        {
                            log.Debug($"{user.UserId}在user_wechat表里无数据,目前这种卡共{++noData}张");
                        }
                    }

                    //mongdb处理
                    var list = wxInfoList.Select(item => new InsertOneModel<WxInfo>(item)).Cast<WriteModel<WxInfo>>().ToList();

                    var bulkResult = wechatCollection.BulkWriteAsync(list).Result;
                    //log.Debug($"微信数据入库第{i + 1}页：pasize：{subDatas.ToList().Count}，成功数：{bulkResult.InsertedCount}，失败数：{subDatas.ToList().Count - bulkResult.InsertedCount}");
                    suctotalData += bulkResult.InsertedCount;
                    //failCount += subDatas.ToList().Count - bulkResult.InsertedCount;
                }


                var endTime = DateTime.Now;
                log.Debug($"微信数据入库结束时间：{endTime}");
                log.Debug($"微信数据入库：总数据量:{cNo.Count}张，总成功数：{suctotalData}，由于mongo数据库卡号不存在失败的卡：{noCno},在user_wechat表里无数据的卡共：{noData}张");
            }
            catch (Exception ex)
            {

                log.Error($"微信数据入库异常信息：{ex.Message}");
            }


        }


    }
}
