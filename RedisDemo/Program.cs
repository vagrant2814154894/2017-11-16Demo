using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Redis;

namespace RedisDemo
{
    //nuget中搜索：ServiceStack.Redis
    class Program
    {
        static void Main(string[] args)
        {
            //1.0开启Redis的客户端,引入 using ServiceStack.Redis;命名空间  (连接服务器)  
            using (RedisClient client = new RedisClient("127.0.0.1", 6379))
            {
                //client.Password = "123456"; //如果连接Redis的redis.windows.conf文件中的配置了访问密码，则在这设置将Password设为它的访问密码  

                //1.0----------------------------------  
                //将数据存储到了Redis服务器中  
                client.Set<string>("name", "zhangshang");

                //获取数据  
                string name = client.Get<string>("name");


                //删除key为蜀国的数据  
                client.Remove("蜀国");
                //client.RemoveAllFromList("蜀国");或者移除key为蜀国的集合数据。  



                //2.0---------------------------------  
                //将数据存入到同一个key中，以list集合的方式存储。（看它如何在同一个key中添加不同的数据）  
                client.AddItemToList("蜀国", "刘备");
                client.AddItemToList("蜀国", "关羽");
                client.AddItemToList("蜀国", "张飞");
                client.AddItemToList("蜀国", "张飞");

               
                //第一种获取数据的方式 （这里是获取key=“蜀国”的数据； 注意：这个key有两个值为“张飞”的数据）  
                List<string> list = client.GetAllItemsFromList("蜀国");
                list.ForEach(r => Console.WriteLine(r)); //输出：刘备 关羽 张飞 张飞  

                //第二种获取数据的方式  
                int listCount = (int)client.GetListCount("蜀国"); //获取key=“蜀国”的数据总条数 4  
                for (int i = 0; i < listCount; i++)
                {
                    Console.WriteLine(client.GetItemFromList("蜀国", i));
                }


                //3.0----------------------------------Set(消重)    
                //用消重的方式，将数据存储到Redis服务器中  
                client.AddItemToSet("魏国", "曹操");
                client.AddItemToSet("魏国", "曹操");
                client.AddItemToSet("魏国", "曹植");

                //获取数据  
                HashSet<string> ha = client.GetAllItemsFromSet("魏国"); //它返回的是一个HashSet的集合  
                List<string> list1 = ha.ToList(); //转成List  
                list1.ForEach(r => Console.WriteLine(r)); //输出：曹操 曹植  （注意：因为我们写入了两个曹操，但是这里使用了Set去重，数以只输出了一个曹操）  


                //4.0----------------------------------队列（队列的特点：先进先出）  

                client.EnqueueItemOnList("吴国", "孙坚");
                client.EnqueueItemOnList("吴国", "孙策");
                client.EnqueueItemOnList("吴国", "周瑜");
                int clistCount = (int)client.GetListCount("吴国"); //获取key=“吴国”的数据总条数 3  
                for (int i = 0; i < clistCount; i++)
                {
                    //出队列（出了对列的数据项，都会被删除，所以如果一个数据项出了对列后，那么Redis里面就会被删除）  
                    Console.WriteLine(client.DequeueItemFromList("吴国"));
                }

                //为了测试出队列的数据项是否被删除，我们来做一个检测  
                if (client.GetAllItemsFromList("吴国").Any() == false)
                {
                    Console.WriteLine("已经全部出队了，没有数据了");
                }


                List<UserInfo> u = new List<UserInfo>() {
                new UserInfo(){Name="1",Age=1},
                new UserInfo(){Name="2",Age=11},
                new UserInfo(){Name="3",Age=12},
            };

                //存储
                client.Set<List<UserInfo>>("test", u);
                //获取
                var q = client.Get<List<UserInfo>>("test");

                foreach (var item in q)
                {
                    Console.WriteLine(item.Name);
                }


                Console.ReadLine();


       
        Console.ReadKey();

            }
        }
    }
}
 public class UserInfo
{
    public string Name { get; set; }
    public int Age { get; set; }
}