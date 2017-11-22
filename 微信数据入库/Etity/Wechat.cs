using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace 微信数据入库.Etity
{
    public class Wechat
    {
        public string OpenId { get; set; }
        public string WxPublicNo { get; set; }
        public long UserId { get; set; }
        public DateTime UpdateTime { get; set; }

    }
}
