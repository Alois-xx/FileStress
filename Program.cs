using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace FileStress
{
    class Program
    {
        static readonly string HelpStr = $"FileStress {FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location).FileVersion}" + Environment.NewLine +
            " FileStress [-touchgb dd] [-committgb dd] [[ -map [dd] -nounmap ] [-filecreate] [-netmaps]] [-filecreate [-keepFiles]] [-throughput dd [-keepFiles] [-nthreads nn] [-filesizemb dd] [-norandom]]  [c:] [d:] [e:] [f:]..." + Environment.NewLine +
            " -touchgb   dd      Commit and touch dd GB of memory before other tests start" + Environment.NewLine + 
            " -committgb dd      Commit but do not touch memory before other tests start" + Environment.NewLine + 
            " -waitforenter      Wait for an enter press before exiting. That way you can create several GB sized allocations which can be released later interactivey from the shell." + Environment.NewLine +
           $" -map               Create with a rate of dd files/s memory maps and save the 4 MB files to \\{FolderName} folder. Default is the C drive" + Environment.NewLine +
            "     -nounmap       Do not unmap the data until it is written to keep the data in the current process working set" + Environment.NewLine +
           $"  -filecreate       File Creation Test which will create 20K files in the folder \\{FolderName} on the target drive" + Environment.NewLine +
           $"  -throughput dd    Test drive thoughput by writing 10 MB files from two threads to \\{FolderName}. dd is the runtime of the test in minutes as float." + Environment.NewLine +
            "     -nthreads n    Number of concurrent writes. Default is 2" + Environment.NewLine + 
            "     -filesizemb n  Size of file to be written. Default is 10" + Environment.NewLine + 
            "     -norandom      By default random data is written to the files. Otherwise a simple pattern with A is written to the files" + Environment.NewLine +
            " -keepFiles         Do not deleted created temporary files on exit" + Environment.NewLine +
            "Examples" + Environment.NewLine + 
            "Allocate 10 GB of memory to put system under stress" + Environment.NewLine +
            "  FileStress -touchgb 10 -waitforenter" + Environment.NewLine + 
            "Test Disk Throughput with threads writing random data in 10MB sized files for 30 minutes" + Environment.NewLine +
            "  FileStress -throughput 30 C:" + Environment.NewLine + 
            ""+
            "";

        enum Mode
        {
            Help = 0,
            Mapping,
            FileCreate,
            Throughput,
            Allocate,
        }

        const string FolderName = "TempFilePerformanceTest";

        unsafe static void Main(string[] args)
        {
            int nFilesPerSecond = 30;
            bool noUnmap = false;
            Mode mode = Mode.Help;
            string drive = "C:";
            int nThreads = 2;
            float fileSizeMB = 10;
            bool randomData = true;
            bool touchMemory = false;
            int GB = 0;
            bool waitForExit = false;
            float nMinuteRuntime = 1;
            bool deleteTempFilesOnExit = true;
            string outputFolder = null;

            void CleanFilesOnExit()
            {
                if( deleteTempFilesOnExit && Directory.Exists(outputFolder))
                {
                    string[] files = Directory.GetFiles(outputFolder);
                    if (files.Length > 0)
                    {
                        Console.WriteLine($"Cleaning {files.Length} temp files from {outputFolder}");
                        foreach (var file in files)
                        {
                            try
                            {
                                File.Delete(file);
                            }
                            catch (Exception)
                            { }
                        }
                    }

                }

            };


            Queue<string> qargs = new(args);

            while(qargs.Count>0)
            {
                string currentArg = qargs.Dequeue();
                switch (currentArg.ToLowerInvariant())
                {
                    case "-touchgb":
                        touchMemory = true;
                        GB = NextNumberOrDefault(qargs, 0);
                        break;
                    case "-committgb":
                        GB = NextNumberOrDefault(qargs, 0);
                        break;
                    case "-waitforenter":
                        waitForExit = true;
                        break;
                    case "-keepfiles":
                        deleteTempFilesOnExit = false;
                        break;
                    case "-nounmap":
                        noUnmap = true;
                        break;
                    case "-throughput":
                        mode = Mode.Throughput;
                        nMinuteRuntime = NextNumberOrDefault(qargs, 5.0f);
                        break;
                    case "-nthreads":
                        nThreads = NextNumberOrDefault(qargs, 2);
                        break;
                    case "-filesizemb":
                        fileSizeMB = NextNumberOrDefault(qargs, 10);
                        break;
                    case "-norandom":
                        randomData = false;
                        break;
                    case "c:":
                    case "d:":
                    case "e:":
                    case "f:":
                    case "g:":
                    case "h:":
                    case "i:":
                    case "j:":
                        drive = currentArg.ToLowerInvariant();
                        break;
                    case "-filecreate":
                        mode = Mode.FileCreate;
                        break;
                    case "-map":
                        mode = Mode.Mapping;
                        break;
                    default:
                        var parsed = ParseNumber(currentArg, 0.0f);
                        if( parsed.Value)
                        {
                            switch (mode)
                            {
                                case Mode.Mapping:
                                    nFilesPerSecond = (int) parsed.Key;
                                    break;
                                case Mode.Throughput:
                                    nMinuteRuntime = parsed.Key;
                                    break;
                                default:
                                    Console.WriteLine($"Error: The argument {currentArg} is not expected!");
                                    return;
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Error: The argument {currentArg} is not expected!");
                        }
                        break;
                }
            }


            try
            {
                if (GB > 0)
                {
                    if( mode == Mode.Help)
                    {
                        mode = Mode.Allocate;
                    }
                    Allocator allocator = new Allocator();
                    allocator.AllocateMemory(GB, touchMemory);
                }

                outputFolder = $"{drive}\\{FolderName}";


                switch (mode)
                {
                    case Mode.Allocate: // allocation is done before other commands
                        Console.WriteLine("Allocation test completed");
                        break;
                    case Mode.Help:
                        Help();
                        break;
                    case Mode.Mapping:
                        MappedWriterBase writer = new MappedWriter((int)fileSizeMB, outputFolder, noUnmap);
                        writer.StartGeneration(nFilesPerSecond);
                        writer.Write();
                        break;
                    case Mode.FileCreate:
                        var filePerf = new FileCreationPerformance();
                        filePerf.TestFileCreationPerformance(outputFolder, 20_000, 0, false);
                        break;
                    case Mode.Throughput:
                        var test = new ThrougputTest(outputFolder, nMinuteRuntime, fileSizeMB, nThreads, randomData);
                        test.Run();
                        break;
                    default:
                        Help();
                        break;
                }

                if( waitForExit )
                {
                    Console.WriteLine("Press enter to exit");
                    Console.ReadLine();
                }
            }
            catch(Exception ex)
            {
                Help();
                Console.WriteLine($"Got Exception: {ex}");
            }

            CleanFilesOnExit();
        }

        private static void Help()
        {
            Console.WriteLine(HelpStr);
        }

        static T NextNumberOrDefault<T>(Queue<string> args, T defaultNr)
        {
            T lret = defaultNr;
            if (args.Count > 0)
            {
                string tryArg = args.Peek();
                var parsed = ParseNumber(tryArg, defaultNr);
                lret = parsed.Key;
                if (parsed.Value == true)
                {
                    args.Dequeue();
                }
            }
            return lret;
        }

        static KeyValuePair<T, bool> ParseNumber<T>(string str, T defaultNr)
        {
            T lret;
            bool success = true;
            if (typeof(T) == typeof(float))
            {
                if (float.TryParse(str, out float parsedNr))
                {
                    lret = (T)(object)parsedNr;
                }
                else
                {
                    success = false;
                    lret = defaultNr;
                }
            }
            else
            {
                if (int.TryParse(str, out int parsedNr))
                {
                    lret = (T)(object)parsedNr;
                }
                else
                {
                    success = false;
                    lret = defaultNr;
                }
            }

            return new KeyValuePair<T, bool>(lret, success);
        }
    }
}
