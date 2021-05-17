using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FileStress
{
    class Allocator
    {
        [DllImport("kernel32.dll", SetLastError = true, ExactSpelling = true)]
        static extern IntPtr VirtualAllocEx(IntPtr hProcess, IntPtr lpAddress, UIntPtr dwSize, AllocationType flAllocationType, MemoryProtection flProtect);

        [Flags]
        public enum AllocationType
        {
            Commit = 0x1000,
            Reserve = 0x2000,
            Decommit = 0x4000,
            Release = 0x8000,
            Reset = 0x80000,
            Physical = 0x400000,
            TopDown = 0x100000,
            WriteWatch = 0x200000,
            LargePages = 0x20000000
        }

        [Flags]
        public enum MemoryProtection
        {
            Execute = 0x10,
            ExecuteRead = 0x20,
            ExecuteReadWrite = 0x40,
            ExecuteWriteCopy = 0x80,
            NoAccess = 0x01,
            ReadOnly = 0x02,
            ReadWrite = 0x04,
            WriteCopy = 0x08,
            GuardModifierflag = 0x100,
            NoCacheModifierflag = 0x200,
            WriteCombineModifierflag = 0x400
        }




        unsafe public void AllocateMemory(int GB, bool touchMemory)
        {
            ulong dwBytes = (ulong)GB * 1024uL * 1024 * 1024;
            Console.WriteLine($"Commit {GB} GB Memory = {dwBytes} bytes");
            IntPtr ptr = VirtualAllocEx(Process.GetCurrentProcess().Handle, IntPtr.Zero, new UIntPtr(dwBytes), AllocationType.Commit, MemoryProtection.ReadWrite);
            if (ptr == IntPtr.Zero)
            {
                Console.WriteLine($"Alloc did fail!");
                return;
            }

            if (touchMemory)
            {
                Console.WriteLine("Touching memory");
                byte* p = (byte*)ptr.ToPointer();
                byte* start = (byte*)ptr.ToPointer();
                for (; p < start + (dwBytes); p += 4096)
                {
                    *p = 1;
                }
                Console.WriteLine("Touching memory done");
            }
        }
    }
}
