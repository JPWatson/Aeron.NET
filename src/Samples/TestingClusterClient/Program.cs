using System;
using System.Text;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;

namespace TestingClusterClient
{
    class Program
    {
        public static void Main(string[] args)
        {
            var context = new AeronCluster.Context();
            using (var client = AeronCluster.Connect(context))
            {
                Console.WriteLine("SessionId: " + client.ClusterSessionId());

                if (client.SendKeepAlive())
                {
                    Console.WriteLine("Keep alive");
                }

                var bytes = Encoding.UTF8.GetBytes("Hello World!");
                var buffer = new UnsafeBuffer(bytes);

                var decarator = new SessionDecorator(client.ClusterSessionId());

                var res = decarator
                    .Offer(client.IngressPublication(), 1, buffer, 0, buffer.Capacity);

                Console.WriteLine("Send message: " + res);

                Console.ReadLine();
            }
        }
    }
}