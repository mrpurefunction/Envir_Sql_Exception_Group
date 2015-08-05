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

//excel access
using System.Data.OleDb;

namespace CsvSupply
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {

        }

        protected override void OnStop()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        public void WorkFunc(string excelname, string sheetname)
        {
            bool isnormal = true;
            try
            {
                System.Configuration.AppSettingsReader asr = new System.Configuration.AppSettingsReader();

                #region transfer data into SQL
                string excelfile = (string)asr.GetValue("filepath", typeof(string));
                string strcon = "Provider=Microsoft.Jet.OLEDB.4.0;" + "Data Source=" + excelfile + ";Extended Properties=\"Text;HDR=No;FMT=Delimited\";";
                //string strcon = "Provider=Microsoft.ACE.OLEDB.12.0;" + "Data Source=" + excelfile + ";Extended Properties=\"Excel 12.0 Xml;HDR=YES\";";
                DataSet ds;
                using (OleDbConnection odc = new OleDbConnection(strcon))
                {
                    //odc.Open();
                    //string sheetname = (string)asr.GetValue("sheetname", typeof(string));
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
                            sb.Append("select count(*) from EnvirMonitorData t where t.enterprise = '" + enterprise + "' and t.pointname = '" + pointname + "' and t.timestamps = '" + stamp.ToString("yyyy-MM-dd HH:mm:ss") + "'");
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
                            //WriteEventLog(ex.Message, System.Diagnostics.EventLogEntryType.Error, "Env_Sql_Svc");
                            isnormal = false;
                        }
                    }
                }
                #endregion
                System.GC.Collect();
            }
            catch (Exception ex)
            {
                isnormal = false;
            }
            finally
            {
                if (!isnormal)
                {

                }
            }
        }
    }
}
