using System;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Broadcast;
using FakeItEasy;
using NUnit.Framework;

namespace Adaptive.Agrona.Tests.Concurrent.Broadcast
{
    public class BroadcastTransmitterTest
    {
        private static readonly int MSG_TYPE_ID = 7;
        private static readonly int CAPACITY = 1024;
        private static readonly int TOTAL_BUFFER_LENGTH = CAPACITY + BroadcastBufferDescriptor.TrailerLength;
        private static readonly int TAIL_INTENT_COUNTER_OFFSET = CAPACITY + BroadcastBufferDescriptor.TailIntentCounterOffset;
        private static readonly int TAIL_COUNTER_INDEX = CAPACITY + BroadcastBufferDescriptor.TailCounterOffset;
        private static readonly int LATEST_COUNTER_INDEX = CAPACITY + BroadcastBufferDescriptor.LatestCounterOffset;

        private UnsafeBuffer _buffer;
        private BroadcastTransmitter _broadcastTransmitter;

        [SetUp]
        public void SetUp()
        {
            _buffer = A.Fake<UnsafeBuffer>();
            A.CallTo(() => _buffer.Capacity).Returns(TOTAL_BUFFER_LENGTH);

            _broadcastTransmitter = new BroadcastTransmitter(_buffer);
        }

        [Test]
        public void ShouldCalculateCapacityForBuffer()
        {
            Assert.AreEqual(CAPACITY, _broadcastTransmitter.Capacity);
        }

        [Test]
        public void ShouldThrowExceptionForCapacityThatIsNotPowerOfTwo()
        {
            int capacity = 777;
            int totalBufferLength = capacity + BroadcastBufferDescriptor.TrailerLength;

            A.CallTo(() => _buffer.Capacity).Returns(totalBufferLength);

            Assert.Throws<InvalidOperationException>(() => new BroadcastTransmitter(_buffer));
        }

        [Test]
        public void ShouldThrowExceptionWhenMaxMessageLengthExceeded()
        {
            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
            Assert.Throws<ArgumentException>(() =>
                _broadcastTransmitter.Transmit(MSG_TYPE_ID, srcBuffer, 0, _broadcastTransmitter.MaxMsgLength+ 1)
            );
        }

        [Test]
        public void ShouldThrowExceptionWhenMessageTypeIdInvalid()
        {
            int invalidMsgId = -1;
            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);

            Assert.Throws<ArgumentException>(() =>
                _broadcastTransmitter.Transmit(invalidMsgId, srcBuffer, 0, 32)
            );
        }

        [Test]
        public void ShouldTransmitIntoEmptyBuffer()
        {
            long tail = 0L;
            int recordOffset = (int) tail;
            int length = 8;
            int recordLength = length + RecordDescriptor.HeaderLength;
            int recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);

            A.CallTo(() => _buffer.GetLong(TAIL_COUNTER_INDEX)).Returns(tail);

            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
            int srcIndex = 0;

            _broadcastTransmitter.Transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);

            A.CallTo(() => _buffer.GetLong(TAIL_COUNTER_INDEX)).MustHaveHappened()
                .Then(A.CallTo(() => _buffer.PutLongOrdered(TAIL_INTENT_COUNTER_OFFSET, tail + recordLengthAligned)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetLengthOffset(recordOffset), recordLength)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetTypeOffset(recordOffset), MSG_TYPE_ID)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutBytes(RecordDescriptor.GetMsgOffset(recordOffset), srcBuffer, srcIndex, length)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutLong(LATEST_COUNTER_INDEX, tail)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutLongOrdered(TAIL_COUNTER_INDEX, tail + recordLengthAligned)).MustHaveHappened());
        }

        [Test]
        public void ShouldTransmitIntoUsedBuffer()
        {
            long tail = RecordDescriptor.RecordAlignment * 3;
            int recordOffset = (int)tail;
            int length = 8;
            int recordLength = length + RecordDescriptor.HeaderLength;
            int recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);

            A.CallTo(() => _buffer.GetLong(TAIL_COUNTER_INDEX)).Returns(tail);

            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
            int srcIndex = 0;

            _broadcastTransmitter.Transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);

            A.CallTo(() => _buffer.GetLong(TAIL_COUNTER_INDEX)).MustHaveHappened()
                .Then(A.CallTo(() => _buffer.PutLongOrdered(TAIL_INTENT_COUNTER_OFFSET, tail + recordLengthAligned)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetLengthOffset(recordOffset), recordLength)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetTypeOffset(recordOffset), MSG_TYPE_ID)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutBytes(RecordDescriptor.GetMsgOffset(recordOffset), srcBuffer, srcIndex, length)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutLong(LATEST_COUNTER_INDEX, tail)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutLongOrdered(TAIL_COUNTER_INDEX, tail + recordLengthAligned)).MustHaveHappened());
        }

        [Test]
        public void ShouldTransmitIntoEndOfBuffer()
        {
            int length = 8;
            int recordLength = length + RecordDescriptor.HeaderLength;
            int recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);
            long tail = CAPACITY - recordLengthAligned;
            int recordOffset = (int) tail;

            A.CallTo(() => _buffer.GetLong(TAIL_COUNTER_INDEX)).Returns(tail);

            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
            int srcIndex = 0;

            _broadcastTransmitter.Transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);

            A.CallTo(() => _buffer.GetLong(TAIL_COUNTER_INDEX)).MustHaveHappened()
                .Then(A.CallTo(() => _buffer.PutLongOrdered(TAIL_INTENT_COUNTER_OFFSET, tail + recordLengthAligned)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetLengthOffset(recordOffset), recordLength)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetTypeOffset(recordOffset), MSG_TYPE_ID)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutBytes(RecordDescriptor.GetMsgOffset(recordOffset), srcBuffer, srcIndex, length)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutLong(LATEST_COUNTER_INDEX, tail)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutLongOrdered(TAIL_COUNTER_INDEX, tail + recordLengthAligned)).MustHaveHappened());
        }

        [Test]
        public void ShouldApplyPaddingWhenInsufficientSpaceAtEndOfBuffer()
        {
            long tail = CAPACITY - RecordDescriptor.RecordAlignment;
            int recordOffset = (int) tail;
            int length = RecordDescriptor.RecordAlignment + 8;
            int recordLength = length + RecordDescriptor.HeaderLength;
            int recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);
            int toEndOfBuffer = CAPACITY - recordOffset;

            A.CallTo(() => _buffer.GetLong(TAIL_COUNTER_INDEX)).Returns(tail);

            UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
            int srcIndex = 0;
            
            _broadcastTransmitter.Transmit(MSG_TYPE_ID, srcBuffer, srcIndex, length);


            A.CallTo(() => _buffer.GetLong(TAIL_COUNTER_INDEX)).MustHaveHappened()
                .Then(A.CallTo(() => _buffer.PutLongOrdered(TAIL_INTENT_COUNTER_OFFSET, tail + recordLengthAligned + toEndOfBuffer)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetLengthOffset(recordOffset), toEndOfBuffer)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetTypeOffset(recordOffset), RecordDescriptor.PaddingMsgTypeID)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetLengthOffset(0), recordLength)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutInt(RecordDescriptor.GetTypeOffset(0), MSG_TYPE_ID)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutBytes(RecordDescriptor.GetMsgOffset(0), srcBuffer, srcIndex, length)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutLong(LATEST_COUNTER_INDEX, tail + toEndOfBuffer)).MustHaveHappened())
                .Then(A.CallTo(() => _buffer.PutLongOrdered(TAIL_COUNTER_INDEX, tail + recordLengthAligned + toEndOfBuffer)).MustHaveHappened());
        }
    }
}