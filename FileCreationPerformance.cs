using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace FileStress
{
    class FileCreationPerformance
    {
        internal const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";


        public void TestFileCreationPerformance(string outputFolder, int nFiles, int fileSizeBytes, bool readFileLength)
        {
            Directory.CreateDirectory(outputFolder);
            ConcurrentQueue<string> fileNames = new();
            Random rand = new();

            Console.WriteLine($"Create {nFiles} of size {fileSizeBytes} in directory {outputFolder} which contains {Directory.GetFiles(outputFolder).Length} files");
            for (int i = 0; i < nFiles; i++)
            {
                fileNames.Enqueue(Path.Combine(outputFolder, $"this is a very long file name which contains many characters to make it for the ntfs files system expensive to create many similar files with nearly the sames names at the end {rand.Next()}{rand.Next()}{rand.Next()}"));
            }

            string columName = "File Create/Close in ms";
            if ( fileSizeBytes > 0 )
            {
                columName = $"File Create/Write {(int)(fileSizeBytes / (1024 * 1024))} MB/Close in ms";
            }

            List<Tuple<string, string, string>> msCreateTimes = new()
            {
                Tuple.Create("FileNr", "Time", columName)
            };

            void create()
            {
                byte[] someData = new byte[fileSizeBytes];
                for (int i = 0; i < someData.Length; i++)
                {
                    someData[i] = (byte)'A';
                }

                while (fileNames.TryDequeue(out string file))
                {
                    var sw = Stopwatch.StartNew();

                    using (var tmp = new FileStream(file, FileMode.CreateNew, FileAccess.ReadWrite))
                    {
                        if (fileSizeBytes > 0)
                        {
                            tmp.Write(someData, 0, someData.Length);
                        }
                    }

                    if (readFileLength)
                    {
                        FileInfo info = new(file);
                        var tmpLen = info.Length;
                    }

                    sw.Stop();

                    lock (msCreateTimes)
                    {
                        msCreateTimes.Add(Tuple.Create((msCreateTimes.Count + 1).ToString(), DateTime.Now.ToString(DateTimeFormat), ((int)sw.Elapsed.TotalMilliseconds).ToString()));
                    }
                }
            }

            var sw = Stopwatch.StartNew();
            Parallel.Invoke(create, create);
            sw.Stop();

            string rootPath = Path.GetPathRoot(outputFolder);
            string detailsFileName = Path.Combine(rootPath,"FileCreation.csv");
            Console.WriteLine($"Did create {nFiles} in {sw.Elapsed.TotalSeconds:F1}s. Details can be found in file {detailsFileName}");
            File.WriteAllLines(detailsFileName, msCreateTimes.Select(x => $"{x.Item1}\t{x.Item2}\t{x.Item3}"));
        }
    }
}
