using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FileStress
{
    class ThroughputTest
    {
        class Measurement
        {
            public DateTime CompleteTime;
            public int TimeSinceStartIns;
            public double FileCreateMs;
            public double FileWriteMs;
            public double FileCloseMs;
            public double TotalTimeMs;
            public long TotalMBWritten;
            public string FileName;
            public long MegaBytesPerSeconds10s;
        }


        /// <summary>
        /// Folder in which the files are stored
        /// </summary>
        readonly string myFolder;

        /// <summary>
        /// How long the test should run
        /// </summary>
        readonly float myRuntimeMinutes;

        /// <summary>
        /// File Size 
        /// </summary>
        readonly float myFileSizeInMB;

        /// <summary>
        /// Number of writer threads
        /// </summary>
        readonly int myWriterThreadCount;

        /// <summary>
        /// Stop writing token
        /// </summary>
        CancellationToken myToken;

        readonly List<Measurement> myResults = new();
        long myTotalMBWritten = 0;
        long myPreviousMBWritten = 0;
        readonly Random myRandom = new();
        readonly byte[] myWriteBuffer;
        readonly bool myUseRandomData;

        long myMegaBytesPerSeconds10s;

        DateTime myStartWritingTime;

        internal const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";

        Action myFileDeletion;
        bool myFileDeletionCalled = false;



        public ThroughputTest(string outputFolder, float runTimeMinutes, float fileSizeInMB, int nThreads, bool randomData, Action fileDeletion)
        {
            myRuntimeMinutes = runTimeMinutes;
            myFileSizeInMB = fileSizeInMB;
            myWriterThreadCount = nThreads;
            myFolder = outputFolder;
            myUseRandomData = randomData;
            myWriteBuffer = new byte[(int)(myFileSizeInMB * 1024 * 1024)];
            myFileDeletion = fileDeletion;
        }

        public void Run()
        {
            Directory.CreateDirectory(myFolder);

            CancellationTokenSource source = new();
            myToken = source.Token;

            string rootFolder = Path.GetPathRoot(myFolder).ToLowerInvariant();

            var drive = DriveInfo.GetDrives().Where(x => x.Name.ToLowerInvariant() == rootFolder).FirstOrDefault();

            long estimatedMB = (long) (myRuntimeMinutes * 100 * 60);

            long diff = drive.AvailableFreeSpace - estimatedMB * 1024 * 1024;

            Console.WriteLine($"Drive {drive.Name} has {drive.AvailableFreeSpace / (1024L * 1024 * 1024)} GB free. Test will add ca. {estimatedMB / 1024} GB of data.");

            CtrlCHandler.Instance.Register(Save);

            Task runner = StartWriting();
            int sleepMs = (int)(myRuntimeMinutes * 60 * 1000);
            Console.WriteLine($"Sleep for {sleepMs}ms");
            Thread.Sleep(sleepMs);
            source.Cancel();
            try
            {
                runner.Wait();
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Got Exception during writing: {ex}");
            }

            Save();
           
        }

        void Save()
        {
            lock (myResults)
            {
                List<string> results = myResults.Select((measurement, i) =>
                {
                    return $"{i + 1};{measurement.CompleteTime.ToString(DateTimeFormat)};{measurement.TimeSinceStartIns};{measurement.FileCreateMs / 1000.0f};{measurement.FileWriteMs / 1000.0f};{measurement.FileCloseMs / 1000.0f};{measurement.TotalTimeMs / 1000.0f};{measurement.TotalMBWritten};{measurement.MegaBytesPerSeconds10s:F0};{Path.GetPathRoot(myFolder)};{myWriterThreadCount};{measurement.FileName}";
                }).ToList();
                results.Insert(0, $"FileNr;Time;Time since Start in s;Open in s;Write in s;Close in s;Total duration for {myFileSizeInMB} MB file in s;MB Written so far;MB/s Averaged over last 10s;Drive;Threads;FileName");

                string timeStamp = DateTime.Now.ToString("HH_mm_ss");

                string detailsFileName = Path.Combine(Path.GetPathRoot(myFolder), $"Througput_{Environment.MachineName}_{myFileSizeInMB:F0}MB_{myRuntimeMinutes}minutes_{timeStamp}.csv");
                Console.WriteLine($"Details can be found in file {detailsFileName}");
                File.WriteAllLines(detailsFileName, results);
            }
        }

        private Task StartWriting()
        {
            myStartWritingTime = DateTime.UtcNow;
            Task.Run(BufferUpdater); // modify to be written data constantly
            Task.Run(MBPerSecondWriter);
            return Task.Run(() =>
            {
                Console.WriteLine($"Execute test on {myWriterThreadCount} threads");
                Parallel.Invoke(Enumerable.Repeat(new Action(Writer), myWriterThreadCount).ToArray()); ;
            });
        }


        void MBPerSecondWriter()
        {
            long prevMB = 0;

            while(!myToken.IsCancellationRequested)
            {
                Thread.Sleep(10_000);
                if(myFileDeletionCalled)
                {
                    myFileDeletionCalled = false;
                    prevMB = Interlocked.Exchange(ref myPreviousMBWritten, myTotalMBWritten);
                    continue;
                }
                prevMB = Interlocked.Exchange(ref myPreviousMBWritten, myTotalMBWritten);
                myMegaBytesPerSeconds10s = (myTotalMBWritten - prevMB) / 10;
            }
        }

        void BufferUpdater()
        {
            if (myUseRandomData)
            {
                Random rand = new();
                while (!myToken.IsCancellationRequested)
                {
                    rand.NextBytes(myWriteBuffer);  // write random data to prevent the SSD storage controler to skip writing
                    Thread.Sleep(15);
                }
            }
            else
            {
                for(int i=0;i<myWriteBuffer.Length;i++)
                {
                    myWriteBuffer[i] = (byte)'A';
                }
            }
        }


        void Writer()
        {
            int exceptionCount = 0;
            while(!myToken.IsCancellationRequested && exceptionCount<5)
            {
                string fileName = null;
                lock (myRandom)
                {
                    fileName = Path.Combine(myFolder, $"ThroughputTests_{myRandom.Next()}{myRandom.Next()}{myRandom.Next()}.txt");
                }

                var sw = Stopwatch.StartNew();
                double fileCreateMs;
                double fileWriteMs;
                double totalMs;
                try
                {
                    {
                        using var file = new FileStream(fileName, FileMode.Create, FileAccess.Write);
                        fileCreateMs = sw.Elapsed.TotalMilliseconds;
                        file.Write(myWriteBuffer, 0, myWriteBuffer.Length);
                        fileWriteMs = sw.Elapsed.TotalMilliseconds;
                    }
                    sw.Stop();
                    totalMs = sw.Elapsed.TotalMilliseconds;
                    lock (myResults)
                    {
                        myResults.Add(new Measurement
                        {
                            CompleteTime = DateTime.Now,
                            TimeSinceStartIns = (int)(DateTime.UtcNow - myStartWritingTime).TotalSeconds,
                            FileCreateMs =  fileCreateMs,
                            FileWriteMs =   fileWriteMs - fileCreateMs,
                            FileCloseMs =  totalMs - fileWriteMs,
                            TotalTimeMs =  totalMs,
                            MegaBytesPerSeconds10s = myMegaBytesPerSeconds10s,
                            TotalMBWritten =  Interlocked.Add(ref myTotalMBWritten, (long)myFileSizeInMB),
                            FileName  = fileName
                        });
                    }
                    Console.WriteLine($"Open: {(int)fileCreateMs,3} ms, Close: {(int)(totalMs-fileWriteMs),3} ms, Write: {(int) (fileWriteMs-fileCreateMs),3} ms, Last 10s {myMegaBytesPerSeconds10s,-4} MB/s");
                }
                catch (IOException ex) when ((ex.HResult & 0xFFFF) == 0x27 || (ex.HResult & 0xFFFF) == 0x70)
                {
                    Console.WriteLine("Disk full. Deleting old data.");
                    myFileDeletionCalled = true;
                    myFileDeletion?.Invoke();

                }
                catch (IOException ex)
                {
                    exceptionCount++;
                    Console.WriteLine($"Got IO Exception: {ex}. We retry again.");
                }
            }

        }
    }
}
