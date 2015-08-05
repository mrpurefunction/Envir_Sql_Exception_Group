using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;

using System.Threading;
using System.Data.SqlClient;

//web
using System.Web;
using System.Net;
using System.Net.Http;
//file
using System.IO;
//excel access
using System.Data.OleDb;

namespace Envir_Sql_Svc
{
    public partial class Service1 : ServiceBase
    {
        private object omutex = new object();

        private bool exit = true;

        public Service1()
        {
            InitializeComponent();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="args"></param>
        protected override void OnStart(string[] args)
        {
            try
            {
                if (!EventLog.SourceExists("Env_Sql_Svc"))
                {
                    //An event log source should not be created and immediately used.
                    //There is a latency time to enable the source, it should be created
                    //prior to executing the application that uses the source.
                    //Execute this sample a second time to use the new source.
                    EventLog.CreateEventSource("Env_Sql_Svc", "Env_Sql_Svc");

                    //Console.WriteLine("CreatedEventSource");
                    //Console.WriteLine("Exiting, execute the application a second time to use the source.");
                    // The source is created.  Exit the application to allow it to be registered.
                    return;
                }
            }
            catch (Exception ex)
            {
            }
            //set the connection limit
            System.Net.ServicePointManager.DefaultConnectionLimit = 500;
            lock (omutex)
            {
                exit = false;
            }
            ThreadStart ts = new ThreadStart(ThreadFunc);
            Thread t = new Thread(ts);
            t.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        protected override void OnStop()
        {
            lock (omutex)
            {
                exit = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void WorkerFunc()
        {
            try
            {
                System.Configuration.AppSettingsReader asr = new System.Configuration.AppSettingsReader();
                //log on with credential
                #region logon
                CookieContainer cc = new CookieContainer();
                HttpWebRequest hwr1 = (HttpWebRequest)HttpWebRequest.Create("http://111.1.15.83:8080/zjzxjk/login.do");
                hwr1.ContentType = "application/x-www-form-urlencoded";
                //user name and password
                string credential = "fw=login&UserName=浙能乐清&PassWord=123456";
                //verb
                hwr1.Method = "POST";
                hwr1.KeepAlive = false;
                //write credential into request stream in encoding of gb2312
                using (StreamWriter writer = new StreamWriter(hwr1.GetRequestStream(), System.Text.Encoding.GetEncoding("gb2312")))
                {
                    writer.Write(credential);
                    writer.Flush();
                }
                //keep the cookie for future use
                hwr1.CookieContainer = cc;
                WebResponse wr = (HttpWebResponse)hwr1.GetResponse();
                wr.Close();
                #endregion

                #region Get Data & download .xls
                //determine whether log-on suceeds
                string sessionstr = null;
                if (hwr1.Address.OriginalString.Split(';').Length > 1)
                {
                    sessionstr = hwr1.Address.OriginalString.Split(';')[1];
                }
                if ((sessionstr != null) && (sessionstr.Contains("jsessionid")))
                {
                    #region
                    //System.IO.Stream s = wr.GetResponseStream();
                    //System.IO.StreamReader reader = new System.IO.StreamReader(s, System.Text.Encoding.GetEncoding("gb2312"));
                    //string returnvalue = reader.ReadToEnd();
                    #endregion
                    //Query data             
                    string condition = HttpUtility.UrlEncode("method", Encoding.ASCII) + "=" + HttpUtility.UrlEncode("historydata", Encoding.ASCII);
                    condition += "&" + HttpUtility.UrlEncode("query", Encoding.ASCII) + "=" + HttpUtility.UrlEncode("0", Encoding.ASCII);
                    condition += "&" + HttpUtility.UrlEncode("lb", Encoding.ASCII) + "=" + HttpUtility.UrlEncode("fq", Encoding.ASCII);
                    //set the time sting format
                    //string begintime = dateTimePicker_start.Value.ToString("yyyy-MM-dd HH:mm:ss").Replace(' ', ',');
                    string endtime = DateTime.Now.ToString("yyyy-MM-dd HH:00:00").Replace(' ', ',');
                    
                    //modified 20150707
                    string begintime = DateTime.Now.AddDays(-int.Parse((string)asr.GetValue("daysrollback", typeof(string)))).ToString("yyyy-MM-dd HH:00:00").Replace(' ', ',');

                    condition += "&" + HttpUtility.UrlEncode("begintime", Encoding.ASCII) + "=" + HttpUtility.UrlEncode(begintime, Encoding.ASCII);
                    condition += "&" + HttpUtility.UrlEncode("endtime", Encoding.ASCII) + "=" + HttpUtility.UrlEncode(endtime, Encoding.ASCII);
                    HttpWebRequest hwr2 = (HttpWebRequest)HttpWebRequest.Create("http://111.1.15.83:8080/zjzxjk/history_action.do?" + condition);
                    //set cookie for hwr2
                    hwr2.CookieContainer = cc;
                    //verb
                    hwr2.Method = "GET";
                    hwr2.ContentType = "text/html, application/xhtml+xml, */*";
                    hwr2.KeepAlive = false;
                    //hwr2.UserAgent = "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.1) Web-Sniffer/1.0.24 ";
                    //hwr2.Credentials = CredentialCache.DefaultCredentials;
                    //hwr2.Headers.Add("Accept-Encoding", "gzip, deflate");
                    //read the response
                    HttpWebResponse wr2 = (HttpWebResponse)hwr2.GetResponse();
                    using (System.IO.Stream s2 = wr2.GetResponseStream())
                    {
                        //using (System.IO.StreamReader reader2 = new System.IO.StreamReader(s2, System.Text.Encoding.GetEncoding("gb2312")))
                        using (System.IO.StreamReader reader2 = new System.IO.StreamReader(s2, System.Text.Encoding.Default))
                        {
                            //string returnvalue2 = reader2.ReadToEnd();
                            reader2.ReadToEnd();
                        }
                    }
                    wr2.Close();
                    //Download excel file
                    HttpWebRequest hwr3 = (HttpWebRequest)HttpWebRequest.Create("http://111.1.15.83:8080/zjzxjk/createHisDataFile.do?method=hisFactorYQExcel&date=new Date().getTime()");
                    //set cookie for hwr3
                    hwr3.CookieContainer = cc;
                    //verb
                    hwr3.Method = "GET";
                    hwr3.KeepAlive = false;
                    WebResponse wr3 = (HttpWebResponse)hwr3.GetResponse();
                    using (System.IO.Stream s3 = wr3.GetResponseStream())
                    {
                        using (Stream localStream = File.Create((string)asr.GetValue("filepath", typeof(string))))
                        {
                            // Allocate a 1k buffer
                            byte[] buffer = new byte[1024];
                            int bytesRead;
                            // Simple do/while loop to read from stream until
                            // no bytes are returned
                            do
                            {
                                // Read data (up to 1k) from the stream
                                bytesRead = s3.Read(buffer, 0, buffer.Length);
                                // Write the data to the local file
                                localStream.Write(buffer, 0, bytesRead);
                            } while (bytesRead > 0);
                        }
                    }
                    wr3.Close();
                }
                #endregion

                #region logout
                HttpWebRequest hwr4 = (HttpWebRequest)HttpWebRequest.Create("http://111.1.15.83:8080/zjzxjk/login.do");
                hwr4.ContentType = "application/x-www-form-urlencoded";
                //user name and password
                string paramout = "fw=loginOut";
                //verb
                hwr4.Method = "POST";
                //write credential into request stream in encoding of gb2312
                using (StreamWriter writer = new StreamWriter(hwr4.GetRequestStream(), System.Text.Encoding.Default))
                {
                    writer.Write(paramout);
                    writer.Flush();
                }
                hwr4.CookieContainer = cc;
                hwr4.KeepAlive = false;
                WebResponse wr4 = (HttpWebResponse)hwr4.GetResponse();
                wr4.Close();
                #endregion

                #region special treatment on .xls file
                Microsoft.Office.Interop.Excel.Application app = new Microsoft.Office.Interop.Excel.Application();
                Microsoft.Office.Interop.Excel.Workbook book1 = app.Workbooks.Open((string)asr.GetValue("filepath", typeof(string)),
                    Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing,
                    Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing, Type.Missing);
                book1.Save();
                book1.Close(Type.Missing, (string)asr.GetValue("filepath", typeof(string)), Type.Missing);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(book1);
                app.Quit();
                System.Runtime.InteropServices.Marshal.ReleaseComObject(app);
                #endregion

                //transfer data in .xls into SQL DB
                #region transfer data into SQL
                string excelfile = (string)asr.GetValue("filepath", typeof(string));
                string strcon = "Provider=Microsoft.Jet.OLEDB.4.0;" + "Data Source=" + excelfile + ";Extended Properties=\"Excel 8.0;HDR=YES\";";
                //string strcon = "Provider=Microsoft.ACE.OLEDB.12.0;" + "Data Source=" + excelfile + ";Extended Properties=\"Excel 12.0 Xml;HDR=YES\";";
                DataSet ds;
                using (OleDbConnection odc = new OleDbConnection(strcon))
                {
                    //odc.Open();
                    string sheetname = (string)asr.GetValue("sheetname", typeof(string));
                    string strquery = "select * from [" + sheetname + "$]";
                    using (OleDbDataAdapter odba = new OleDbDataAdapter(strquery, odc))
                    {
                        ds = new DataSet();
                        odba.Fill(ds);
                    }
                }
                if (ds != null)
                {
                    string enterprise;
                    string pointname;
                    DateTime stamp;
                    int t = 0;
                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        if (t == 0)
                        {
                            t++;
                            continue;
                        }
                        try
                        {
                            enterprise = r[1].ToString();
                            pointname = r[2].ToString();
                            stamp = DateTime.Parse(r[3].ToString());
                            StringBuilder sb = new StringBuilder();
                            //sb.Append("select count(*) from EnvirMonitorData t where t.enterprise = '" + enterprise + "' and t.pointname = '" + pointname + "' and t.timestamps = '" + stamp.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                            sb.Append("select count(*) from EnvirMonitorData t where t.pointname = '" + pointname + "' and t.timestamps = '" + stamp.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                            #region not used
                            //Database db = DatabaseFactory.CreateDatabase("dbconn");
                            ////db.CreateConnection();
                            //System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                            #endregion
                            int num = 0;
                            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection((string)asr.GetValue("sql", typeof(string))))
                            {
                                conn.Open();
                                SqlCommand sc = new SqlCommand(sb.ToString(), conn);
                                num = (int)sc.ExecuteScalar();
                                if (num == 0)
                                {
                                    StringBuilder sb2 = new StringBuilder();
                                    int startcol = (int)asr.GetValue("startcol", typeof(int));
                                    int endcol = (int)asr.GetValue("endcol", typeof(int));
                                    for (int i = startcol; i < endcol; i++)
                                    {
                                        sb2.Clear();
                                        sb2.Append("insert into EnvirMonitorData(enterprise,pointname,timestamps,indicatorid,indicatorvalue) values('");
                                        sb2.Append(enterprise + "','");
                                        sb2.Append(pointname + "','");
                                        sb2.Append(stamp.ToString("yyyy-MM-dd HH:mm:ss") + "',");
                                        if ((i - 3) == 1)
                                        {
                                            sb2.Append((i - 3 + 1).ToString() + ",");
                                        }
                                        else if ((i - 3) == 2)
                                        {
                                            sb2.Append((i - 3 - 1).ToString() + ",");
                                        }
                                        else if ((i - 3) == 3)
                                        {
                                            sb2.Append((i - 3 + 1).ToString() + ",");
                                        }
                                        else if ((i - 3) == 4)
                                        {
                                            sb2.Append((i - 3 - 1).ToString() + ",");
                                        }
                                        else
                                        {
                                            sb2.Append((i - 3).ToString() + ",");
                                        }
                                        sb2.Append(double.Parse(r[i].ToString()).ToString() + ")");
                                        SqlCommand sc2 = new SqlCommand(sb2.ToString(), conn);
                                        sc2.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            WriteEventLog(ex.Message, System.Diagnostics.EventLogEntryType.Error, "Env_Sql_Svc");
                        }

                        try
                        {
                            enterprise = r[1].ToString();
                            pointname = r[2].ToString();
                            stamp = DateTime.Parse(r[3].ToString());
                            StringBuilder sb = new StringBuilder();
                            //sb.Append("select count(*) from EnvirMonitorData t where t.enterprise = '" + enterprise + "' and t.pointname = '" + pointname + "' and t.timestamps = '" + stamp.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                            sb.Append("select count(*) from WebMonitorData t where t.pointname = '" + pointname + "' and t.timestamps = '" + stamp.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                            #region not used
                            //Database db = DatabaseFactory.CreateDatabase("dbconn");
                            ////db.CreateConnection();
                            //System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                            #endregion
                            int num = 0;
                            using (System.Data.SqlClient.SqlConnection conn = new System.Data.SqlClient.SqlConnection((string)asr.GetValue("sql", typeof(string))))
                            {
                                conn.Open();
                                SqlCommand sc = new SqlCommand(sb.ToString(), conn);
                                num = (int)sc.ExecuteScalar();
                                if (num == 0)
                                {
                                    StringBuilder sb2 = new StringBuilder();
                                    int startcol = (int)asr.GetValue("startcol", typeof(int));
                                    int endcol = (int)asr.GetValue("endcol", typeof(int));
                                    for (int i = startcol; i < endcol; i++)
                                    {
                                        sb2.Clear();
                                        sb2.Append("insert into WebMonitorData(enterprise,pointname,timestamps,indicatorid,indicatorvalue) values('");
                                        sb2.Append(enterprise + "','");
                                        sb2.Append(pointname + "','");
                                        sb2.Append(stamp.ToString("yyyy-MM-dd HH:mm:ss") + "',");
                                        if ((i - 3) == 1)
                                        {
                                            sb2.Append((i - 3 + 1).ToString() + ",");
                                        }
                                        else if ((i - 3) == 2)
                                        {
                                            sb2.Append((i - 3 - 1).ToString() + ",");
                                        }
                                        else if ((i - 3) == 3)
                                        {
                                            sb2.Append((i - 3 + 1).ToString() + ",");
                                        }
                                        else if ((i - 3) == 4)
                                        {
                                            sb2.Append((i - 3 - 1).ToString() + ",");
                                        }
                                        else
                                        {
                                            sb2.Append((i - 3).ToString() + ",");
                                        }
                                        sb2.Append(double.Parse(r[i].ToString()).ToString() + ")");
                                        SqlCommand sc2 = new SqlCommand(sb2.ToString(), conn);
                                        sc2.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            WriteEventLog(ex.Message, System.Diagnostics.EventLogEntryType.Error, "Env_Sql_Svc");
                        }
                    }
                }
                #endregion
                System.GC.Collect();
            }
            catch (Exception ex)
            {
                WriteEventLog(ex.Message, System.Diagnostics.EventLogEntryType.Error, "Env_Sql_Svc");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void ThreadFunc()
        {
            DateTime lastexecution = new DateTime(1900, 1, 1);
            bool temp;
            while (1 == 1)
            {
                lock (omutex)
                {
                    temp = exit;
                }
                if (temp)
                {
                    break;
                }
                else
                {
                    if (DateTime.Now > lastexecution.AddMinutes(5.0))
                    {
                        WorkerFunc();
                        lastexecution = DateTime.Now;
                    }
                }
                Thread.Sleep(1000);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="eventmessage"></param>
        /// <param name="elet"></param>
        /// <param name="evtsource"></param>
        private void WriteEventLog(string eventmessage, EventLogEntryType elet, string evtsource)
        {
            try
            {
                EventLog el = new EventLog("Env_Sql_Svc");
                el.Source = evtsource;
                el.WriteEntry(eventmessage, elet);
            }
            catch (Exception ex)
            {
            }
        }
    }
}
