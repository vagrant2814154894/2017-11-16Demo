using Memcached.ClientLibrary;

namespace 微信数据入库
{
    /// <summary>
    /// 分布式缓存的MemCached管理器
    /// </summary>
    public class MemCachedManager
    {
        public static MemcachedClient cache;

        static MemCachedManager()
        {
            //string[] servers = { "172.20.61.195:11211" };
            //string[] servers = { "172.16.12.157:11211" };
            string[] servers = { "127.0.0.1:11211" };

            //初始化池
            SockIOPool pool = SockIOPool.GetInstance();
            //设置服务器列表
            pool.SetServers(servers);
            //设置各服务之间负责均衡的权重
            pool.SetWeights(new int[] { 1 });
            //初始化时创建连接数
            pool.InitConnections = 3;
            //最小连接数
            pool.MinConnections = 3;
            //最大连接数
            pool.MaxConnections = 5;
            //连接的最大空闲时间，下面设置为6个小时（单位为ms），超过这个设置时间，连接会被释放
            pool.MaxIdle = 1000 * 60 * 60 * 6;
            //socket连接的超时时间，为0表示永不超时，即一直保持连接状态
            pool.SocketConnectTimeout = 0;
            //通讯的超时时间，下面设置为3秒，.net版本没有实现
            pool.SocketTimeout = 1000 * 3;
            //维护线程的间隔激活时间，下面设置为30秒（单位s），设置为0时表示不启用维护线程
            pool.MaintenanceSleep = 30;
            //设置SocketIO池的故障标志
            pool.Failover = true;
            //是否对TCP/IP通讯使用nalgle算法，.net版本没有实现
            pool.Nagle = false;
            //socket单次任务的最大时间（单位ms），超过这个时间socket会被强行中断，当前任务失败
            pool.MaxBusy = 1000 * 10;
            pool.Initialize();
            cache = new MemcachedClient();
            //是否启用压缩数据：如果启用了压缩，数据压缩长于门槛的数据将被储存在压缩的形式
            cache.EnableCompression = false;
        }
    }
}
