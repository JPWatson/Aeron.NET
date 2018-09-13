using System;
using Adaptive.Agrona.Collections;

namespace Adaptive.Agrona
{
    /// <summary>
    /// Expandable <seealso cref="IMutableDirectBuffer"/> that is backed by an array. When values are put into the buffer beyond its
    /// current length, then it will be expanded to accommodate the resulting position for the value.
    /// <para>
    /// Put operations will expand the capacity as necessary up to <seealso cref="MAX_ARRAY_LENGTH"/>. Get operations will throw
    /// a <seealso cref="IndexOutOfRangeException"/> if past current capacity.
    /// </para>
    /// <para>
    /// Note: this class has a natural ordering that is inconsistent with equals.
    /// Types my be different but equal on buffer contents.
    /// </para>
    /// </summary>
    public unsafe class ExpandableArrayBuffer : IMutableDirectBuffer
    {
        /// <summary>
        /// Maximum length to which the underlying buffer can grow. Some JVMs set bits in the last few bytes.
        /// </summary>
        public static readonly int MAX_ARRAY_LENGTH = int.MaxValue - 8;

        /// <summary>
        /// Initial capacity of the buffer from which it will expand as necessary.
        /// </summary>
        public const int INITIAL_CAPACITY = 128;

        private byte[] _byteArray;

        /// <summary>
        /// Create an <seealso cref="ExpandableArrayBuffer"/> with an initial length of <seealso cref="#INITIAL_CAPACITY"/>.
        /// </summary>
        public ExpandableArrayBuffer() : this(INITIAL_CAPACITY)
        {
        }

        /// <summary>
        /// Create an <seealso cref="ExpandableArrayBuffer"/> with a provided initial length.
        /// </summary>
        /// <param name="initialCapacity"> of the buffer. </param>
        public ExpandableArrayBuffer(int initialCapacity)
        {
            _byteArray = new byte[initialCapacity];
        }

        public void Wrap(byte[] buffer)
        {
            throw new NotSupportedException();
        }

        public void Wrap(byte[] buffer, int offset, int length)
        {
            throw new NotSupportedException();
        }

        public void Wrap(IDirectBuffer buffer)
        {
            throw new NotSupportedException();
        }

        public void Wrap(IDirectBuffer buffer, int offset, int length)
        {
            throw new NotSupportedException();
        }

        public void Wrap(IntPtr pointer, int length)
        {
            throw new NotSupportedException();
        }

        public void Wrap(IntPtr pointer, int offset, int length)
        {
            throw new NotSupportedException();
        }

        public IntPtr BufferPointer => IntPtr.Zero;
        public byte[] ByteArray => _byteArray;
        public ByteBuffer ByteBuffer => null;
        public int Capacity => _byteArray.Length;

        public void CheckLimit(int limit)
        {
            throw new NotImplementedException();
        }

        public long GetLong(int index, ByteOrder byteOrder)
        {
            fixed (byte* b = _byteArray)
            {
                
            }
        }

        public long GetLong(int index)
        {
            throw new NotImplementedException();
        }

        public int GetInt(int index, ByteOrder byteOrder)
        {
            throw new NotImplementedException();
        }

        public int GetInt(int index)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int index)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int index)
        {
            throw new NotImplementedException();
        }

        public short GetShort(int index, ByteOrder byteOrder)
        {
            throw new NotImplementedException();
        }

        public short GetShort(int index)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int index)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int index)
        {
            throw new NotImplementedException();
        }

        public void GetBytes(int index, byte[] dst)
        {
            throw new NotImplementedException();
        }

        public void GetBytes(int index, byte[] dst, int offset, int length)
        {
            throw new NotImplementedException();
        }

        public void GetBytes(int index, IMutableDirectBuffer dstBuffer, int dstIndex, int length)
        {
            throw new NotImplementedException();
        }

        public string GetStringUtf8(int index)
        {
            throw new NotImplementedException();
        }

        public string GetStringAscii(int index)
        {
            throw new NotImplementedException();
        }

        public string GetStringUtf8(int index, int length)
        {
            throw new NotImplementedException();
        }

        public string GetStringAscii(int index, int length)
        {
            throw new NotImplementedException();
        }

        public string GetStringWithoutLengthUtf8(int index, int length)
        {
            throw new NotImplementedException();
        }

        public void BoundsCheck(int index, int length)
        {
            BoundsCheck0(index, length);
        }

        public bool IsExpandable => true;

        public void SetMemory(int index, int length, byte value)
        {
            throw new NotImplementedException();
        }

        public void PutLong(int index, long value, ByteOrder byteOrder)
        {
            throw new NotImplementedException();
        }

        public void PutLong(int index, long value)
        {
            throw new NotImplementedException();
        }

        public void PutInt(int index, int value, ByteOrder byteOrder)
        {
            throw new NotImplementedException();
        }

        public void PutInt(int index, int value)
        {
            throw new NotImplementedException();
        }

        public int PutIntAscii(int index, int value)
        {
            throw new NotImplementedException();
        }

        public int PutLongAscii(int index, long value)
        {
            throw new NotImplementedException();
        }

        public void PutDouble(int index, double value)
        {
            throw new NotImplementedException();
        }

        public void PutFloat(int index, float value)
        {
            throw new NotImplementedException();
        }

        public void PutShort(int index, short value, ByteOrder byteOrder)
        {
            throw new NotImplementedException();
        }

        public void PutShort(int index, short value)
        {
            throw new NotImplementedException();
        }

        public void PutChar(int index, char value)
        {
            throw new NotImplementedException();
        }

        public void PutByte(int index, byte value)
        {
            throw new NotImplementedException();
        }

        public void PutBytes(int index, byte[] src)
        {
            throw new NotImplementedException();
        }

        public void PutBytes(int index, byte[] src, int offset, int length)
        {
            throw new NotImplementedException();
        }

        public void PutBytes(int index, IDirectBuffer srcBuffer, int srcIndex, int length)
        {
            throw new NotImplementedException();
        }

        public int PutStringUtf8(int index, string value)
        {
            throw new NotImplementedException();
        }

        public int PutStringAscii(int index, string value)
        {
            throw new NotImplementedException();
        }

        public int PutStringWithoutLengthAscii(int index, string value)
        {
            throw new NotImplementedException();
        }

        public int PutStringWithoutLengthAscii(int index, string value, int valueOffset, int length)
        {
            throw new NotImplementedException();
        }

        public int PutStringUtf8(int index, string value, int maxEncodedSize)
        {
            throw new NotImplementedException();
        }

        public int PutStringWithoutLengthUtf8(int index, string value)
        {
            throw new NotImplementedException();
        }

        private void EnsureCapacity(int index, int length)
        {
            if (index < 0)
            {
                throw new IndexOutOfRangeException("index cannot be negative: index=" + index);
            }

            var resultingPosition = index + (long) length;
            var currentArrayLength = _byteArray.Length;

            if (resultingPosition > currentArrayLength)
            {
                if (currentArrayLength >= MAX_ARRAY_LENGTH)
                {
                    throw new IndexOutOfRangeException($"index={index} length={length} maxCapacity={MAX_ARRAY_LENGTH}");
                }

                _byteArray = ArrayUtil.CopyOf(_byteArray, CalculateExpansion(currentArrayLength, (int) resultingPosition));
            }
        }

        private static int CalculateExpansion(int currentLength, int requiredLength)
        {
            long value = currentLength;

            while (value < requiredLength)
            {
                value = value + (value >> 1);

                if (value > int.MaxValue)
                {
                    value = MAX_ARRAY_LENGTH;
                }
            }

            return (int) value;
        }
        
        private void BoundsCheck0(int index, int length)
        {
            int currentArrayLength = _byteArray.Length;
            long resultingPosition = index + (long)length;
            if (index < 0 || resultingPosition > currentArrayLength)
            {
                throw new IndexOutOfRangeException($"index={index} length={length} capacity={currentArrayLength}");
            }
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return base.ToString();
        }

        public int CompareTo(IDirectBuffer other)
        {
            throw new NotImplementedException();
        }
    }
}