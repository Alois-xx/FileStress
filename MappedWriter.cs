using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace FileStress
{
    internal class MappedWriter : MappedWriterBase
    {
        readonly Queue<Tuple<MemoryMappedFile, SafeMemoryMappedViewHandle, string>> myQueue = new();

        public MappedWriter(int fileSizeInMB, string drive, bool noUnmap):base(fileSizeInMB, drive, noUnmap)
        {
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
            string fileMappingName = $"File Mapping {rand.Next()}{rand.Next()}{rand.Next()}";
            MemoryMappedFile file = MemoryMappedFile.CreateNew(fileMappingName, myFileSizeInBytes);
            byte* ptr = null;
            var accessor = file.CreateViewAccessor();
            var handle = accessor.SafeMemoryMappedViewHandle;
            handle.AcquirePointer(ref ptr);
            ulong* lPtr = (ulong*)ptr;
            for (int i = 0; i < myFileSizeInBytes; i += 512)
            {
                ulong value = ((ulong)rand.Next()) << 32 | (uint)rand.Next();
                *((ulong*)((byte*)lPtr + i)) = value;
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