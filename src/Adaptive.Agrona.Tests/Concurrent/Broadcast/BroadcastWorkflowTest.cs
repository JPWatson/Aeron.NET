using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Concurrent.Broadcast
{
    public class BroadcastWorkflowTest
    {
        [Test]
        public void BasicUseCase()
        {
            var msgTypeId = 7;
            var length = 256;
            var capacity = 256 * 8 + BroadcastBufferDescriptor.TrailerLength;
            var buffer = new UnsafeBuffer(new byte[capacity]);
            var receiver1 = new BroadcastReceiver(buffer);
            var transmitter = new BroadcastTransmitter(buffer);
            
            var srcBuffer = new UnsafeBuffer(new byte[length]);

            Assert.IsFalse(receiver1.ReceiveNext());
            
            transmitter.Transmit(msgTypeId, srcBuffer, 0, length);

            Assert.IsTrue(receiver1.ReceiveNext());
            Assert.AreEqual(msgTypeId, receiver1.TypeId);
            Assert.AreEqual(length, receiver1.Length);
            Assert.AreEqual(RecordDescriptor.HeaderLength, receiver1.Offset);

            Assert.IsFalse(receiver1.ReceiveNext());
        }
    }
}