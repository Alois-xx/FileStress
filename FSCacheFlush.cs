using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FileStress
{
    class FSCacheFlush
    {
        readonly List<string> myFiles = new();

        int myCounter = 0;

        public FSCacheFlush(string fileOrDirectory) : this(fileOrDirectory, SearchOption.TopDirectoryOnly)
        { }

        public FSCacheFlush(string fileOrDirectory, SearchOption option)
        {
            if (Directory.Exists(fileOrDirectory))
            {
                switch(option)
                {
                    case SearchOption.AllDirectories:
                        myFiles.AddRange(GetFilesRecursiveIgnoringInaccessbileDirectories(fileOrDirectory, "*.*"));
                        break;
                    case SearchOption.TopDirectoryOnly:
                        myFiles.AddRange(Directory.GetFiles(fileOrDirectory, "*.*"));
                        break;
                }

                
            }
            else if (File.Exists(fileOrDirectory))
            {
                myFiles.Add(fileOrDirectory);
            }
            else
            {
                throw new FileNotFoundException($"File/Directory {fileOrDirectory} does not exist.");
            }

        }

        public void Flush()
        {
            Console.WriteLine();
            Parallel.ForEach(myFiles, 
                new ParallelOptions
                {
                     MaxDegreeOfParallelism = 5
                },
                file =>
                {
                    FlushFileSystemCacheForFile(file);
                });
            Console.WriteLine($"Flushed {myCounter}/{myFiles.Count} files");
        }

        void FlushFileSystemCacheForFile(string fileName)
        {
            const FileOptions FileFlagNoBuffering = (FileOptions)0x20000000;

            try
            {
                using var file = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileFlagNoBuffering);
                // Just open close the file to flush file system cache with FileFlagNoBuffering
                //Console.WriteLine($"Flushing {fileName}");
                int old = Interlocked.Increment(ref myCounter);
                if( old % 100 == 0 || old == myFiles.Count)
                {
                    Console.CursorLeft = 0;
                    Console.Write($"{100.0f*old/(myFiles.Count*1.0f):F0} % {old}/{myFiles.Count}");
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Failed to flush {fileName} {ex.GetType().Name}: {ex.Message}");
            }

        }

        public static IEnumerable<string> GetFilesRecursiveIgnoringInaccessbileDirectories(string root, string searchPattern)
        {
            Stack<string> pending = new Stack<string>();
            pending.Push(root);
            while (pending.Count != 0)
            {
                var path = pending.Pop();
                string[] next = null;
                try
                {
                    next = Directory.GetFiles(path, searchPattern);
                }
                catch 
                {
                    // ignore access errors for inaccesible diretories
                }

                if (next != null && next.Length != 0)
                {
                    foreach (var file in next)
                    {
                        yield return file;
                    }
                }
                try
                {
                    next = Directory.GetDirectories(path);
                    foreach (var subdir in next)
                    {
                        pending.Push(subdir);
                    }
                }
                catch 
                {
                    // ignore access errors for inaccesible diretories
                }
            }
        }
    }



}
