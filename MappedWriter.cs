using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;

namespace FileStress
{
    internal class MappedWriter : MappedWriterBase
    {
        readonly Queue<Tuple<MemoryMappedFile, SafeMemoryMappedViewHandle, string>> myQueue = new();
        readonly bool myUnlock = false;
        byte[] myRandomData = null;
        const int ERROR_NOT_LOCKED = 0x9e;
        ulong myCount = 1;


        public MappedWriter(int fileSizeInMB, string drive, bool noUnmap, bool unlock, bool bWrite) :base(fileSizeInMB, drive, noUnmap, bWrite)
        {
            myUnlock = unlock;
            
        }

        unsafe void ModifyMap(MemoryMappedFile file)
        {
            Random rand = new();
            using var accessor = file.CreateViewAccessor();
            byte* ptr = null;
            var handle = accessor.SafeMemoryMappedViewHandle;
            handle.AcquirePointer(ref ptr);
            ulong* lPtr = (ulong*)ptr;
            for (int i = 0; i < myFileSizeInBytes; i += 512)
            {
                ulong value = ((ulong)rand.Next()) << 32 | (uint)rand.Next();
                *((ulong*)((byte*)lPtr + i)) = value;
            }
            handle.ReleasePointer();
        }

        protected override unsafe void CreateMemoryMap(ref int counter, Random rand)
        {
            if(myRandomData == null)
            {
                myRandomData = CreateRandomData(myFileSizeInBytes, rand);
            }
            string fileMappingName = $"File Mapping {rand.Next()}{rand.Next()}{rand.Next()}";
            MemoryMappedFile file = MemoryMappedFile.CreateNew(fileMappingName, myFileSizeInBytes);
            byte* ptr = null;
            var accessor = file.CreateViewAccessor();
            var handle = accessor.SafeMemoryMappedViewHandle;
            handle.AcquirePointer(ref ptr);
            ulong* lPtr = (ulong*)ptr;
            myCount++;

            fixed (byte* pbRandom = &myRandomData[0])
            {
                ulong* pRandom = (ulong*)pbRandom;
                for (int i = 0; i < myFileSizeInBytes / 8; i++)
                {
                    ulong value = *(pRandom + i) ^ *(pRandom+myCount);  // xor with other random location from random array to get ever changing random data
                                                                        // If we simply store 0s then memory compression will get much less work to do and we 
                                                                        // get only occasional page file writes. With non compressible not repeated random data
                                                                        // we should see page file writes with the same rate as we create images in the page file
                    *(lPtr + i) = value;
                }
            }
            lock (myQueue)
            {
                myQueue.Enqueue(Tuple.Create(file, handle, fileMappingName));
            }
            counter++;

            if (myUnmap)
            {
                handle.ReleasePointer();
            }
            if( myUnlock )
            {
                bool unlockRes = VirtualUnlock(new IntPtr(lPtr), myFileSizeInBytes);
                if( unlockRes == false)
                {
                    int lastError = Marshal.GetLastWin32Error();
                    if( lastError != ERROR_NOT_LOCKED)
                    {
                        Console.WriteLine("Unlock failed with {0}", lastError );
                    }
                    
                }
            }
        }

        static private byte[] CreateRandomData(uint size, Random rand)
        {
            byte[] buffer = new byte[size];
            rand.NextBytes(buffer);
            return buffer;
        }



        private unsafe void ModifyMaps()
        {
            lock (myQueue)
            {
                var arr = myQueue.ToArray();
                Console.WriteLine($"Modify {arr.Length} Maps]");
                for (int i = 0; i < arr.Length; i++)
                {
                    ModifyMap(arr[i].Item1);
                }
            }
        }


        protected override unsafe void WriteMemorMapToFile(string dir, Random rand)
        {
            var chunk = new List<Tuple<MemoryMappedFile, SafeMemoryMappedViewHandle, string>>();

            if (myQueue.Count > 100)
            {
                lock (myQueue)
                {
                    while (chunk.Count < 100 && myQueue.Count > 0)
                    {
                        chunk.Add(myQueue.Dequeue());
                    }
                }
            }

            if (chunk.Count > 0)
            {
                Console.WriteLine($"Got Chunk of {chunk.Count} files {myQueue.Count} pending");

                var sw = Stopwatch.StartNew();
                foreach (var fileMapView in chunk)
                {
                    string fileName = Path.Combine(dir, $"MemoryMap_{rand.Next()}{rand.Next()}{rand.Next()}.txt");
                    using var file = new FileStream(fileName, FileMode.Create, FileAccess.Write);

                    byte* ptr = null;

                    MemoryMappedViewAccessor acc = null;
                    if( myUnmap )
                    {
                        acc = fileMapView.Item1.CreateViewAccessor();
                        acc.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
                    }
                    else
                    {
                        fileMapView.Item2.AcquirePointer(ref ptr);
                    }
                    var iPtr = new IntPtr(ptr);
                    WriteFile(file.SafeFileHandle, iPtr, myFileSizeInBytes, out uint bytesWritten, null);

                    if( myUnmap )
                    {
                        acc.SafeMemoryMappedViewHandle.ReleasePointer();
                        acc.Dispose();
                    }
                    else
                    {
                        // If unmap was not done by creator we need to release now two times!
                        fileMapView.Item2.ReleasePointer();
                        fileMapView.Item2.ReleasePointer();
                    }
                   
                    fileMapView.Item1.Dispose();
                }

                sw.Stop();
                Console.WriteLine($"Did write {chunk.Count} files in {sw.Elapsed.TotalMilliseconds:F0} ms");
            }
            else
            {
                Thread.Sleep(1);
            }
        }
    }
}