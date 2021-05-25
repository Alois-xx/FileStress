using Microsoft.Win32.SafeHandles;
using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace FileStress
{
    abstract class MappedWriterBase
    {
        protected readonly bool myUnmap;
        protected readonly string myOutputFolder;
        protected readonly uint myFileSizeInBytes;
        protected readonly bool mybWrite;

        /// <summary>
        /// Win32 API for native file writing
        /// </summary>
        /// <param name="handle">File handle (can be provided by a managed Stream).</param>
        /// <param name="buffer">A pointer to the buffer containing the data to be written to the file or device.</param>
        /// <param name="numBytesToRead">Number of bytes to be written.</param>
        /// <param name="numBytesRead">Number of bytes that was written.</param>
        /// <param name="overlapped">Can be null. Otherwise it shall point to a valid and unique OVERLAPPED structure, otherwise the function can incorrectly report that the write operation is complete.</param>
        /// <returns></returns>
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern unsafe bool WriteFile(SafeFileHandle handle, IntPtr buffer, uint numBytesToRead, out uint numBytesRead, NativeOverlapped* overlapped);

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern unsafe bool VirtualUnlock(IntPtr buffer, ulong size);

        public MappedWriterBase(int fileSizeInMB, string outputFolder, bool noUnmap, bool bWrite)
        {
            myUnmap = !noUnmap;
            myOutputFolder = outputFolder;
            myFileSizeInBytes = (uint)fileSizeInMB * 1024 * 1024;
            mybWrite = bWrite;
        }

        public virtual void StartGeneration(int nFilesPerSecond)
        {
            Task.Run(() =>
            {
               
                int counter = 0;
                Random rand = new();
                Console.WriteLine($"Creating {nFilesPerSecond} files/s");
                while (true)
                {
                    var sw = Stopwatch.StartNew();
                    counter = 0;

                    while (sw.Elapsed.TotalSeconds < 1.0d)
                    {
                        if (counter < nFilesPerSecond)
                        {
                            CreateMemoryMap(ref counter, rand);
                        }
                        else
                        {
                            Thread.Sleep(1);
                        }
                    }
                }
            });
        }

        public void Write()
        {
            Directory.CreateDirectory(myOutputFolder);
            Random rand = new();

            if (mybWrite)
            {
                while (true)
                {
                    WriteMemorMapToFile(myOutputFolder, rand);
                }
            }
            else
            {
                // when we skip writing we block indefinitely to simulate a growing memory leak
                Thread.Sleep(Timeout.Infinite);
            }
        }

        protected abstract unsafe void CreateMemoryMap(ref int counter, Random rand);
        protected abstract unsafe void WriteMemorMapToFile(string dir, Random rand);
    }
}
