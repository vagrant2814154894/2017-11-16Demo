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
using ServiceStack.Redis;

namespace 微信数据入库
{
    class Program
    {
      

        // 日志客户端
        static AliLog.Logger log = new AliLog.Logger();
        //链接字符串
        static string conn = "mongodb://127.0.0.1:27017";
        //string conn = "mongodb://root:Fxft2017@dds-bp104401edaca7e41.mongodb.rds.aliyuncs.com:3717,dds-bp104401edaca7e42.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-3015103";
        //指定的数据库
        //string dbName = "CardInfo";
        static string dbName = "Test";
        // Mongo客户端
        static MongoClient client;
        //当前操作数据库
        static IMongoDatabase database;
        /// <summary>
        /// 
        /// </summary>
        static RedisClient redisClient;
        //string conTerminal = "Server=fxftdatabase.mysql.rds.aliyuncs.com;Port=3306;initial catalog=base_app;uid=ckb_admin; pwd =ckbadmin;Allow User Variables=True;Convert Zero Datetime=True";

        static string conTerminal = "Server=localhost;Port=3306;initial catalog=base_app;uid=sa; pwd =123456;Allow User Variables=True;Convert Zero Datetime=True";

        static void Main(string[] args)
        {
            redisClient = new RedisClient("127.0.0.1", 6379);

            MongoClient mongoClient = new MongoClient(conn);
            IMongoDatabase updatabase = mongoClient.GetDatabase(dbName);
            var cardCollection = updatabase.GetCollection<Card>("tblCard");//表
            var wechatCollection = updatabase.GetCollection<WxInfo>("tblWX");//表
            FirstAddToWheactTable();
            //var terminalData = MemCachedManager.cache.Get("terminalData");//Id,number也要缓存，微信表的updateTime也要缓存
            var terminalData = redisClient.Get<List<Cno>>("terminalData");//61708
            //var wechatData = MemCachedManager.cache.Get("wechatData");
            var wechatData = redisClient.Get<List<Wechat>>("wechatData");//75194

            ConcurrentDictionary<long, List<WxItem>> wxInfos = new ConcurrentDictionary<long, List<WxItem>>();


            List<Cno> card = null;
            using (IDbConnection connection = new MySqlConnection(conTerminal))
            {

                card = connection.Query<Cno>("SELECT t.Id,t.Number,t.UserId FROM user_terminal t  ORDER BY t.Id desc").ToList();//61801
                redisClient.Set<List<Cno>>("terminalData", card, DateTime.Now.AddMonths(1));

                SortedDictionary<long, Cno> terminalIds = new SortedDictionary<long, Cno>();


                foreach (var cardItem in card)
                {
                    terminalIds.Add(cardItem.Id, cardItem);

                }
                if (terminalData != null)
                {
                    //mongo同步已删除数据
                    var i = 0;
                    foreach (var terminal in terminalData)
                    {
                        if (!terminalIds.ContainsKey(terminal.Id))
                        {

                            //拿到number号去mongo里面查卡号，
                            //从mongo中找sim
                            BsonArray querys = new BsonArray();
                            querys.Add(new BsonDocument("cNo", terminal.Number));
                            querys.Add(new BsonDocument("iccid", terminal.Number));
                            querys.Add(new BsonDocument("imsi", terminal.Number));
                            BsonDocument query = new BsonDocument("$or", querys);
                            var cNo = cardCollection.Find(query).FirstOrDefault();
                            if (cNo != null)
                            {

                                BsonDocument queryDelete = new BsonDocument("cNo", cNo.cNo);
                                //根据卡号删除数据
                                //wechatCollection.DeleteOne(queryDelete);//从业务角度来看用这种就够了
                                wechatCollection.DeleteMany(queryDelete);//用这种比较保险

                            }
                            i++;
                        }
                    }

                    //mongo同步新增数据
                    ConcurrentDictionary<long, List<string>> numbers = new ConcurrentDictionary<long, List<string>>();
                    var maxId = terminalData[0].Id;
                    Cno cardItem = new Cno();
                    cardItem.Id = maxId;
                    var newIds = terminalIds.ToList();
                    var newId = newIds[newIds.Count - 1].Key;
                    if (newId > maxId)
                    {
                        var newCards = connection.Query<Cno>("SELECT t.Id,t.Number,t.UserId FROM user_terminal t where t.Id>@Id ORDER BY t.Id desc", cardItem).ToList();
                        log.Debug($"从user_terminal表获取到的需要入库的卡共：{newCards.Count}张");
                        foreach (var numItem in newCards)
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

                        //mongo新增

                        #region wechat表数据整理

                        var newWechat =
                     connection.Query<Wechat>("SELECT t.UpdateTime, t.OpenId,t.WxPublicNo,t.UserId FROM user_wechat t ORDER BY t.UpdateTime desc").ToList();
                        redisClient.Set<List<Wechat>>("wechatData", newWechat, DateTime.Now.AddMonths(1));
                        foreach (var appItem in newWechat)
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


                        #endregion

                        #region 新增

                        List<WxInfo> wxInfoList = new List<WxInfo>();
                        long noData = 0;//在user_wechat表里无数据
                        var noCno = 0;//在mongo tbCard表里没有卡号的卡
                        foreach (var user in newCards)
                        {

                            if (wxInfos.ContainsKey(user.UserId))
                            {
                                //从mongo中找sim
                                BsonArray querys = new BsonArray();
                                querys.Add(new BsonDocument("cNo", user.Number));
                                querys.Add(new BsonDocument("iccid", user.Number));
                                querys.Add(new BsonDocument("imsi", user.Number));
                                BsonDocument query = new BsonDocument("$or", querys);
                                var addCno = cardCollection.Find(query).FirstOrDefault();
                                if (addCno != null)
                                {
                                    WxInfo wxInfo = new WxInfo();
                                    wxInfo.cNo = addCno.cNo;
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

                        log.Debug($"微信数据入库：总数据量:{newWechat.Count}张，总成功数：{bulkResult.InsertedCount}，由于mongo数据库卡号不存在失败的卡：{noCno},在user_wechat表里无数据的卡共：{noData}张");

                        #endregion


                    }

                    //mongo同步修改的数据
                    if (wechatData != null)
                    {
                        var lastUpdateTime = wechatData[0].UpdateTime;
                        Wechat wechat = new Wechat();
                        wechat.UpdateTime = lastUpdateTime;
                        var upWechats =
                       connection.Query<Wechat>("SELECT t.UpdateTime, t.OpenId,t.WxPublicNo,t.UserId FROM user_wechat t  where t.UpdateTime>@UpdateTime  ORDER BY t.UpdateTime desc", wechat).ToList();
                        //mongo修改
                        //upWechats存在的UserId进行更新
                        var j = 0;
                        foreach (var upWechat in upWechats)
                        {

                            if (terminalIds.ContainsKey(upWechat.UserId))
                            {
                                //从mongo中找sim
                                var num = terminalIds[upWechat.UserId].Number;
                                BsonArray querys = new BsonArray();
                                querys.Add(new BsonDocument("cNo", num));
                                querys.Add(new BsonDocument("iccid", num));
                                querys.Add(new BsonDocument("imsi", num));
                                BsonDocument query = new BsonDocument("$or", querys);
                                var upCno = cardCollection.Find(query).FirstOrDefault();
                                BsonDocument queryUp = new BsonDocument("cNo", upCno.cNo);
                                                                        

                                WxInfo wxInfo = new WxInfo();
                                wxInfo.cNo = upCno.cNo;
                                wxInfo.wxInfo = wxInfos[upWechat.UserId];
                                wechatCollection.DeleteMany(queryUp);//用这种比较保险
                                wechatCollection.InsertOne(wxInfo);

                                j++;
                                //var update = new BsonDocument() { { "$set", new BsonDocument("wxInfo", "111") } };
                                //wechatCollection.UpdateOneAsync(queryUp, update, new UpdateOptions() { IsUpsert = true });





                            }

                        }

                        var test = 0;
                    }
                 

                }






            }






        }

        static void FirstAddToWheactTable()
        {
            try
            {
                #region 测试使用
                //userIds[0].Id = 1000000000020041;
                #endregion

                var beginTime = DateTime.Now;
                log.Debug($"微信数据入库开始时间：{beginTime}");
                client = new MongoClient(conn);
                database = client.GetDatabase(dbName);
                var cardCollection = database.GetCollection<Card>("tblCard");//表
                var wechatCollection = database.GetCollection<WxInfo>("tblWX");//表


                #region 将user_terminal表和user_wechat表的数据缓存到本地

                ConcurrentDictionary<long, List<string>> numbers = new ConcurrentDictionary<long, List<string>>();
                ConcurrentDictionary<long, List<WxItem>> wxInfos = new ConcurrentDictionary<long, List<WxItem>>();
                //List<Cno> terminalCache = new List<Cno>();
                List<Cno> cNo = null;
                using (IDbConnection connection = new MySqlConnection(conTerminal))
                {


                    cNo = connection.Query<Cno>("SELECT t.Id,t.Number,t.UserId FROM user_terminal t  ORDER BY t.Id desc").ToList();
                    log.Debug($"从user_terminal表获取到的需要入库的卡共：{cNo.Count}张");
                    foreach (var numItem in cNo)
                    {

                        //terminalCache.Add(numItem);

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

                    //存储
                    redisClient.Set<List<Cno>>("terminalData", cNo, DateTime.Now.AddDays(3));
                    ////获取 
                    //var q = redisClient.Get<List<Cno>>("terminalData");
                    //用于更新使用的缓存
                    // MemCachedManager.cache.Set("terminalData", cNo, DateTime.Now.AddDays(3));
                    //var test= redisClient.Get("terminalDataqq").AsList();
                    //var terminalData1 = MemCachedManager.cache.Get("terminalData");
                    var appId =
                         connection.Query<Wechat>("SELECT t.UpdateTime, t.OpenId,t.WxPublicNo,t.UserId FROM user_wechat t ORDER BY t.UpdateTime desc").ToList();

                    ////用于更新使用的缓存
                    //MemCachedManager.cache.Set("wechatData", appId, DateTime.Now.AddDays(3));
                    //var wechatData1 = MemCachedManager.cache.Get("wechatData");
                    //存储
                    redisClient.Set<List<Wechat>>("wechatData", appId,DateTime.Now.AddMonths(1));
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

                #endregion

                #region mongo 微信表 数据新增
                long suctotalData = 0;
                long noData = 0;//在user_wechat表里无数据
                var noCno = 0;//在mongo tbCard表里没有卡号的卡
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
                            //从mongo中找sim
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
                #endregion

            }
            catch (Exception ex)
            {

                log.Error($"微信数据入库异常信息：{ex.Message}");
            }
        }
    }
}
