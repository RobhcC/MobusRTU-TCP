using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ModbusRTU_TCP
{
    internal static class Program
    {
        /// <summary>
        /// 应用程序的主入口点。
        /// </summary>
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);

            // UI线程未捕获异常改为拦截处理：弹窗提示后程序继续运行（不闪退）
            // 程序活着，采集定时器和刷库定时器就还活着，缓冲数据就有机会正常落库
            Application.SetUnhandledExceptionMode(UnhandledExceptionMode.CatchException);
            Application.ThreadException += (s, e) =>
            {
                MessageBox.Show(
                    "发生未处理异常，程序已拦截并继续运行：\r\n\r\n" + e.Exception.Message,
                    "错误提示", MessageBoxButtons.OK, MessageBoxIcon.Error);
            };

            Application.Run(new Form1());
        }
    }
}
