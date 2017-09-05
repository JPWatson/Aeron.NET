using System;
using System.Threading;

namespace Adaptive.Agrona.Concurrent.Broadcast
{
    /// <summary>
    /// Transmit messages via an underlying broadcast buffer to zero or more <seealso cref="BroadcastReceiver"/>s.
    /// <para>
    /// <b>Note:</b> This class is not threadsafe. Only one transmitter is allowed per broadcast buffer.
    /// </para>
    /// </summary>
    public class BroadcastTransmitter
    {
        private readonly IAtomicBuffer _buffer;
        private readonly int _tailIntentCountIndex;
        private readonly int _tailCounterIndex;
        private readonly int _latestCounterIndex;

        /// <summary>
        /// Construct a new broadcast transmitter based on an underlying <seealso cref="UnsafeBuffer"/>.
        /// The underlying buffer must a power of 2 in size plus sufficient space
        /// for the <seealso cref="BroadcastBufferDescriptor.TrailerLength"/>.
        /// </summary>
        /// <param name="buffer"> via which messages will be exchanged. </param>
        /// <exception cref="InvalidOperationException"> if the buffer capacity is not a power of 2
        /// plus <seealso cref="BroadcastBufferDescriptor.TrailerLength"/> in capacity. </exception>
        public BroadcastTransmitter(IAtomicBuffer buffer)
        {
            _buffer = buffer;
            Capacity = buffer.Capacity - BroadcastBufferDescriptor.TrailerLength;

            BroadcastBufferDescriptor.CheckCapacity(Capacity);
            buffer.VerifyAlignment();

            MaxMsgLength = RecordDescriptor.CalculateMaxMessageLength(Capacity);
            _tailIntentCountIndex = Capacity + BroadcastBufferDescriptor.TailIntentCounterOffset;
            _tailCounterIndex = Capacity + BroadcastBufferDescriptor.TailCounterOffset;
            _latestCounterIndex = Capacity + BroadcastBufferDescriptor.LatestCounterOffset;
        }

        /// <summary>
        /// Get the capacity of the underlying broadcast buffer.
        /// </summary>
        /// <returns> the capacity of the underlying broadcast buffer. </returns>
        public int Capacity { get; }

        /// <summary>
        /// Get the maximum message length that can be transmitted for a buffer.
        /// </summary>
        /// <returns> the maximum message length that can be transmitted for a buffer. </returns>
        public int MaxMsgLength { get; }

        /// <summary>
        /// Transmit a message to <seealso cref="BroadcastReceiver"/>s via the broadcast buffer.
        /// </summary>
        /// <param name="msgTypeId"> type of the message to be transmitted. </param>
        /// <param name="srcBuffer"> containing the encoded message to be transmitted. </param>
        /// <param name="srcIndex">  srcIndex in the source buffer at which the encoded message begins. </param>
        /// <param name="length">    in bytes of the encoded message. </param>
        /// <exception cref="ArgumentException"> of the msgTypeId is not valid,
        ///                                  or if the message length is greater than <seealso cref="#maxMsgLength()"/>. </exception>
        public void Transmit(int msgTypeId, IDirectBuffer srcBuffer, int srcIndex, int length)
        {
            RecordDescriptor.CheckTypeId(msgTypeId);
            CheckMessageLength(length);

            IAtomicBuffer buffer = _buffer;
            long currentTail = buffer.GetLong(_tailCounterIndex);
            int recordOffset = (int) currentTail & (Capacity - 1);
            int recordLength = RecordDescriptor.HeaderLength + length;
            int recordLengthAligned = BitUtil.Align(recordLength, RecordDescriptor.RecordAlignment);
            long newTail = currentTail + recordLengthAligned;
            
            int toEndOfBuffer = Capacity - recordOffset;
            if (toEndOfBuffer < recordLengthAligned)
            {
                SignalTailIntent(buffer, newTail + toEndOfBuffer);
                InsertPaddingRecord(buffer, recordOffset, toEndOfBuffer);

                currentTail += toEndOfBuffer;
                recordOffset = 0;
            }
            else
            {
                SignalTailIntent(buffer, newTail);
            }

            buffer.PutInt(RecordDescriptor.GetLengthOffset(recordOffset), recordLength);
            buffer.PutInt(RecordDescriptor.GetTypeOffset(recordOffset), msgTypeId);

            buffer.PutBytes(RecordDescriptor.GetMsgOffset(recordOffset), srcBuffer, srcIndex, length);

            buffer.PutLong(_latestCounterIndex, currentTail);
            buffer.PutLongOrdered(_tailCounterIndex, currentTail + recordLengthAligned);
        }

        private void SignalTailIntent(IAtomicBuffer buffer, long newTail)
        {
            buffer.PutLongOrdered(_tailIntentCountIndex, newTail);
            Thread.MemoryBarrier();
        }

        private static void InsertPaddingRecord(IAtomicBuffer buffer, int recordOffset, int length)
        {
            buffer.PutInt(RecordDescriptor.GetLengthOffset(recordOffset), length);
            buffer.PutInt(RecordDescriptor.GetTypeOffset(recordOffset), RecordDescriptor.PaddingMsgTypeID);
        }

        private void CheckMessageLength(int length)
        {
            if (length > MaxMsgLength)
            {
                throw new ArgumentException("Encoded message exceeds maxMsgLength of " + MaxMsgLength + ", length=" + length);
            }
        }
    }
}