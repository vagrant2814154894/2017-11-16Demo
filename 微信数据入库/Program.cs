using System;
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
            try
            {

                //链接字符串
                string conn = "mongodb://127.0.0.1:27017";
                //string conn = "mongodb://root:Fxft2017@dds-bp104401edaca7e41.mongodb.rds.aliyuncs.com:3717,dds-bp104401edaca7e42.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-3015103";
                //指定的数据库
                //string dbName = "CardInfo";
                string dbName = "Test";
                // Mongo客户端
                MongoClient client;
                //当前操作数据库
                IMongoDatabase database;

                //string conUser = "Server=fxftdatabase.mysql.rds.aliyuncs.com;Port=3306;initial catalog=base_user;uid=ckb_admin; pwd =ckbadmin;Allow User Variables=True;Convert Zero Datetime=True";
                //string conTerminal = "Server=fxftdatabase.mysql.rds.aliyuncs.com;Port=3306;initial catalog=base_app;uid=ckb_admin; pwd =ckbadmin;Allow User Variables=True;Convert Zero Datetime=True";

                string conUser = "Server=localhost;Port=3306;initial catalog=base_user;uid=sa; pwd =123456;Allow User Variables=True;Convert Zero Datetime=True";
                string conTerminal = "Server=localhost;Port=3306;initial catalog=base_app;uid=sa; pwd =123456;Allow User Variables=True;Convert Zero Datetime=True";

                List<User> userIds;
                using (IDbConnection connection = new MySqlConnection(conUser))
                {
                    userIds = connection.Query<User>("select t.Id from users t").ToList();
                }

                var noCno = 0;
                #region 测试使用

                //userIds[0].Id = 1000000000020041;
                #endregion

                var beginTime = DateTime.Now;
                log.Debug($"微信数据入库开始时间：{beginTime}");
                client = new MongoClient(conn);
                database = client.GetDatabase(dbName);
                var cardCollection = database.GetCollection<Card>("tblCard");//表
                List<WxInfo> wxInfoList = new List<WxInfo>();
                ConcurrentDictionary<long, string> numbers = new ConcurrentDictionary<long, string>();
                ConcurrentDictionary<long, Wechat> wxInfos = new ConcurrentDictionary<long, Wechat>();

                using (IDbConnection connection = new MySqlConnection(conTerminal))
                {
                    var number = connection.Query<Cno>("SELECT t.Number,t.UserId FROM user_terminal t ").ToList();
                    //有的UserId没有Number
                    foreach (var numItem in number)
                    {
                        numbers.TryAdd(numItem.UserId, numItem.Number);
                    }
                    var appId =
                        connection.Query<Wechat>("SELECT  t.OpenId,t.WxPublicNo,t.UserId FROM user_wechat t ")
                            .ToList();
                    foreach (var appItem in appId)
                    {
                        wxInfos.TryAdd(appItem.UserId, appItem);
                        

                    }
                }

                //BsonDocument query1 = new BsonDocument("$or", querys);
                List<Card> cards = new List<Card>();
                var mongoBegTime = DateTime.Now;
                log.Debug($"mongo查询开始时间：{mongoBegTime}");
                cardCollection.Find(new BsonDocument()).ForEachAsync((doc) =>
                {
                    cards.Add(doc);
                }).Wait();
                var mongoEndTime = DateTime.Now;
                log.Debug($"mongo查询结束时间：{mongoEndTime}");
                ConcurrentDictionary<string, Card> iccids = new ConcurrentDictionary<string, Card>();
                ConcurrentDictionary<string, Card> imsis = new ConcurrentDictionary<string, Card>();
                var noValues = 0;
                foreach (var item in cards)
                {
                    if (item.iccid == null)
                    {

                        iccids.TryAdd("no" + noValues, item);
                        noValues++;
                    }
                    else
                    {
                        iccids.TryAdd(item.iccid, item);

                    }
                    if (item.imsi == null)
                    {
                        imsis.TryAdd("no" + noValues, item);
                        noValues++;
                    }
                    else
                    {
                        imsis.TryAdd(item.imsi, item);

                    }
                }


                //每次10000
                foreach (var user in userIds)
                {
                    if (numbers.ContainsKey(user.Id) && wxInfos.ContainsKey(user.Id))
                    {
                        var number = numbers[user.Id];//从mong中找sim
                        string card = null;
                        if (iccids.ContainsKey(number))
                        {
                            card = iccids[number].cNo;
                        }
                        else if (imsis.ContainsKey(number))
                        {
                            card = imsis[number].cNo;
                        }
                        //BsonArray querys = new BsonArray();
                        //querys.Add(new BsonDocument("iccid", number));
                        //querys.Add(new BsonDocument("imsi", number));
                        //BsonDocument query = new BsonDocument("$or", querys);
                        //var card = cardCollection.Find(query).FirstOrDefault();

                        if (card != null)
                        {
                            WxInfo wxInfo = new WxInfo();
                            wxInfo.cNo = card;

                            wxInfo.wxInfo = new List<WxItem>();
                            WxItem wxItem = new WxItem();
                            wxItem.appId = wxInfos[user.Id].WxPublicNo;

                            wxItem.openId = new List<string>();
                            var openId = wxInfos[user.Id].OpenId;
                            wxItem.openId.Add(openId);

                            wxInfo.wxInfo.Add(wxItem);

                            wxInfoList.Add(wxInfo);
                        }
                        else
                        {
                            log.Debug($"{number}在库里没有对应的卡号,目前没有卡号的共{++noCno}个");
                        }
                    }
                }

                //mongdb处理
                var wechatCollection = database.GetCollection<WxInfo>("tblWX");//表

                var list = wxInfoList.Select(item => new InsertOneModel<WxInfo>(item)).Cast<WriteModel<WxInfo>>().ToList();

                var bulkResult = wechatCollection.BulkWriteAsync(list).Result;

                log.Debug($"微信数据入库成功数：{bulkResult.InsertedCount}，失败数：{wxInfoList.Count - bulkResult.InsertedCount}");
                var endTime = DateTime.Now;
                log.Debug($"微信数据入库结束时间：{endTime}");
            }
            catch (Exception ex)
            {

                log.Error($"微信数据入库异常信息：{ex.Message}");
            }


        }


    }
}
