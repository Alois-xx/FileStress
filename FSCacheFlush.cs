using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileStress
{
    class FSCacheFlush
    {
        List<string> myFiles = new List<string>();

        public FSCacheFlush(string fileOrDirectory) : this(fileOrDirectory, SearchOption.TopDirectoryOnly)
        { }

        public FSCacheFlush(string fileOrDirectory, SearchOption option)
        {
            if (Directory.Exists(fileOrDirectory))
            {
                myFiles.AddRange(Directory.GetFiles(fileOrDirectory, "*.*", option));
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
            foreach(var file in myFiles)
            {
                FlushFileSystemCacheForFile(file);
            }
        }

        void FlushFileSystemCacheForFile(string fileName)
        {
            const FileOptions FileFlagNoBuffering = (FileOptions)0x20000000;

            using (var file = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileFlagNoBuffering))
            {
                // Just open close the file to flush file system cache with FileFlagNoBuffering
            }

        }
    }
}
