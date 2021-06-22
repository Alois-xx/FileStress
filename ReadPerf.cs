using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileStress
{
    class ReadPerf
    {
        readonly List<FileStream> myFiles = new();
        readonly byte[] myBuffer;
        readonly int myReadBufferSizeInBytes;

        public ReadPerf(string directory, int bufferSizeKB)
        {
            myReadBufferSizeInBytes = bufferSizeKB * 1024;
            myBuffer = new byte[myReadBufferSizeInBytes];

            foreach(var file in Directory.GetFiles(directory))
            {
                var fileStream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, myReadBufferSizeInBytes, FileOptions.RandomAccess);
                myFiles.Add(fileStream);
            }
            
        }

        const int GiB = 1000 * 1000 * 1000;
        const int MB = 1024 * 1024;

        public void Run()
        {
            ulong totalBytesRead = 0;
            ulong gbRead = 0;
            ulong lastChunckTotalBytesRead = 0;


            long start = Stopwatch.GetTimestamp();
            long sliceStart = Stopwatch.GetTimestamp();
            foreach (var stream in myFiles)
            {
                int lastReadBytes;
                while ((lastReadBytes = stream.Read(myBuffer, 0, myReadBufferSizeInBytes)) != 0)
                {
                    totalBytesRead += (ulong)lastReadBytes;
                    ulong currentGBRead = totalBytesRead / GiB;
                    if (gbRead != currentGBRead)
                    {
                        gbRead = currentGBRead;
                        long current = Stopwatch.GetTimestamp();
                        double durationIns = GetTimeIns(sliceStart, current);
                        Console.WriteLine($"Current Read Perf since Last {(totalBytesRead - lastChunckTotalBytesRead) / MB} MB {((totalBytesRead - lastChunckTotalBytesRead) / MB) / durationIns:F0} MB/s Current File: {stream.Name}");
                        sliceStart = Stopwatch.GetTimestamp();
                        lastChunckTotalBytesRead = totalBytesRead;
                    }
                }
            }

            var duration = GetTimeIns(start, Stopwatch.GetTimestamp());
            Console.WriteLine($"Total Read Performance: {(totalBytesRead / MB) / duration:F0} MB/s while reading {totalBytesRead/MB:F0} MB");
        }

        double GetTimeIns(long start, long stop)
        {
            return new TimeSpan(stop - start).TotalSeconds;
        }

    }
}
