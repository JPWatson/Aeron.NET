using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Codecs;
using Adaptive.Cluster.Service;

namespace TestingCluster
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var context = new ClusteredServiceContainer.Context()
                .ClusteredService(new Service())
                .ErrorHandler(Console.WriteLine);

            using (var sc = ClusteredServiceContainer.Launch(context))
            {   
                Console.ReadLine();
            }
        }
    }

    public class Service : IClusteredService
    {
        public void OnStart(ICluster cluster)
        {
            Console.WriteLine("Starting...");
        }

        public void OnSessionOpen(ClientSession session, long timestampMs)
        {
            Console.WriteLine("OnSessionOpen...");
        }

        public void OnSessionClose(ClientSession session, long timestampMs, CloseReason closeReason)
        {
            Console.WriteLine("OnSessionClose...");
        }

        public void OnSessionMessage(long clusterSessionId, long correlationId, long timestampMs, IDirectBuffer buffer,
            int offset,
            int length, Header header)
        {
            Console.WriteLine("OnMessage..." + buffer.GetStringWithoutLengthUtf8(offset, length));
        }

        public void OnTimerEvent(long correlationId, long timestampMs)
        {
            Console.WriteLine("OnTimerEvent...");
        }

        public void OnTakeSnapshot(Publication snapshotPublication)
        {
            Console.WriteLine("OnTakeSnapshot...");
        }

        public void OnLoadSnapshot(Image snapshotImage)
        {
            Console.WriteLine("OnLoadSnapshot...");
        }

        public void OnReplayBegin()
        {
            Console.WriteLine("OnReplayBegin...");
        }

        public void OnReplayEnd()
        {
            Console.WriteLine("OnReplayEnd...");
        }

        public void OnRoleChange(ClusterRole newRole)
        {
            Console.WriteLine($"OnRoleChange to {newRole}");
        }
    }
}