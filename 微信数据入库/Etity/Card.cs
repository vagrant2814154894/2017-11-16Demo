using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;

namespace 微信数据入库.Etity
{
    [BsonIgnoreExtraElements]
    public  class Card
    {
        public string cNo { get; set; }
        public string iccid { get; set; }
        public string imsi { get; set; }
    }
}
