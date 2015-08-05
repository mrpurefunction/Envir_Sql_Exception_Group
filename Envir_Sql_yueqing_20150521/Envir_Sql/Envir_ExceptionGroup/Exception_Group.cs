using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using Microsoft.Practices.EnterpriseLibrary;
using Microsoft.Practices.EnterpriseLibrary.Common;
using Microsoft.Practices.EnterpriseLibrary.Data;

namespace Envir_ExceptionGroup
{
    public class Exception_Group
    {
        /// <summary>
        /// SCR
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <returns></returns>
        public DataSet GetScrRelatedDs(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                DataSet ds = null;
                DataSet ds2 = null;
                #region
                //StringBuilder sb = new StringBuilder();
                //sb.Append("select * from V_RuleResult_SCR t where t.machineid = '" + machineid.ToString() + "' and ");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                //Database db = DatabaseFactory.CreateDatabase("dbconn");
                //System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                //return db.ExecuteDataSet(dbc);
                #endregion
                StringBuilder sb = new StringBuilder();
                StringBuilder sb2 = new StringBuilder();
                sb.Append("select top 1 s.* from v_ruleresult_scr s where s.TimeLog < '");
                sb.Append(ts.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_scr t where t.TimeLog < '");
                sb.Append(ts.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = 'SCR撤出' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc");
                sb2.Append("select top 1 s.* from v_ruleresult_scr s where s.TimeLog < '");
                sb2.Append(ts.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss") + "' and s.Timelog>= '" + ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_scr t where t.TimeLog < '");
                sb2.Append(ts.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss") + "' and t.Timelog>= '" + ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = 'SCR投运' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                System.Data.Common.DbCommand dbc2 = db.GetSqlStringCommand(sb2.ToString());
                ds = db.ExecuteDataSet(dbc);
                ds2 = db.ExecuteDataSet(dbc2);

                if (ds.Tables[0].Rows.Count > 0)
                {
                    return ds;
                }
                if (ds2.Tables[0].Rows.Count > 0)
                {
                    return ds2;
                }
                return null;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// SCR-calib
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <returns></returns>
        public DataSet GetCalibRelatedDs(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                System.Configuration.AppSettingsReader asr = new System.Configuration.AppSettingsReader();
                string[] pts = ((string)asr.GetValue("calib_nox", typeof(string))).Split(';');
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from V_RuleResult_Calib t where t.rulename in (");
                foreach (string p in pts)
                {
                    sb.Append("'" + p + "'");
                    if (p != pts.Last())
                    {
                        sb.Append(",");
                    }
                }
                sb.Append(") and t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// SCR-special condition
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <returns></returns>
        public DataSet GetSconRelatedDs(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from V_RuleResult_SpecialCon t where t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }


        /// <summary>
        /// SCR-start stop
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <returns></returns>
        public DataSet GetStartStopDs(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select top 1 s.* from v_ruleresult_startstop s where s.TimeLog < '" );
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_startstop t where t.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = '机组停机' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }


        /// <summary>
        /// SCR
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public int? GetScrRelatedCount(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                DataSet ds = null;
                DataSet ds2 = null;
                #region
                //StringBuilder sb = new StringBuilder();
                //sb.Append("select count(*) from V_RuleResult_SCR t where t.machineid = '" + machineid.ToString() + "' and ");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                //Database db = DatabaseFactory.CreateDatabase("dbconn");
                //System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                //return (int)db.ExecuteScalar(dbc);
                #endregion
                StringBuilder sb = new StringBuilder();
                StringBuilder sb2 = new StringBuilder();
                sb.Append("select top 1 s.* from v_ruleresult_scr s where s.TimeLog < '");
                sb.Append(ts.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_scr t where t.TimeLog < '");
                sb.Append(ts.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = 'SCR撤出' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc");
                sb2.Append("select top 1 s.* from v_ruleresult_scr s where s.TimeLog < '");
                sb2.Append(ts.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss") + "' and s.Timelog>= '" + ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_scr t where t.TimeLog < '");
                sb2.Append(ts.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss") + "' and t.Timelog>= '" + ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = 'SCR投运' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                System.Data.Common.DbCommand dbc2 = db.GetSqlStringCommand(sb2.ToString());
                ds = db.ExecuteDataSet(dbc);
                ds2 = db.ExecuteDataSet(dbc2);

                if ((ds.Tables[0].Rows.Count == 0) && (ds2.Tables[0].Rows.Count == 0))
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// SCR-calib
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public int? GetCalibRelatedCount(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                System.Configuration.AppSettingsReader asr = new System.Configuration.AppSettingsReader();
                string[] pts = ((string)asr.GetValue("calib_nox", typeof(string))).Split(';');
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from V_RuleResult_Calib t where t.rulename in (");
                #region
                //sb.Append('10HTA50CQ104AB','20HTA50CQ104AB','30HTA50CQ104AB','40HTA50CQ104AB'
                #endregion
                foreach (string p in pts)
                {
                    sb.Append("'" + p + "'");
                    if (p != pts.Last())
                    {
                        sb.Append(",");
                    }
                }
                sb.Append(") and t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return (int)db.ExecuteScalar(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// Scr-special condition
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public int? GetSconRelatedCount(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from V_RuleResult_SpecialCon t where t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return (int)db.ExecuteScalar(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// Scr-start stop
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public int? GetStartStopCount(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                #region
                //StringBuilder sb = new StringBuilder();
                //sb.Append("select count(*) from V_RuleResult_SpecialCon t where t.machineid = '" + machineid.ToString() + "' and ");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                //Database db = DatabaseFactory.CreateDatabase("dbconn");
                //System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                //return (int)db.ExecuteScalar(dbc);
                #endregion
                StringBuilder sb = new StringBuilder();
                sb.Append("select top 1 s.* from v_ruleresult_startstop s where s.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_startstop t where t.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = '机组停机' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc ");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                DataSet ds = db.ExecuteDataSet(dbc);
                if (ds.Tables[0].Rows.Count == 0)
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="envirid"></param>
        /// <returns></returns>
        public bool? IsExceptionGroupDataExist(long envirid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from exceptiondata_group t where ");
                sb.Append("t.envir_id = " + envirid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                if ((int)db.ExecuteScalar(dbc) > 0)
                {
                    return true;
                }
                return false;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Envirid"></param>
        /// <param name="typeid"></param>
        /// <param name="typecontent"></param>
        /// <param name="mconfirm"></param>
        public void AddExceptionGroupData(long envirid, int typeid, string typecontent, int mconfirm)
        {
            try
            {
                if (IsExceptionGroupDataExist(envirid) == false)
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into exceptiondata_group(envir_id,typeid,typecontent,mconfirm) values(");
                    sb.Append(envirid.ToString() + ",");
                    sb.Append(typeid.ToString() + ",'");
                    if (typecontent == null)
                    {
                        sb.Append("',");
                    }
                    else
                    {
                        sb.Append(typecontent + "',");
                    }
                    sb.Append(mconfirm.ToString() + ")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="envirid"></param>
        public void DeleteExceptionGroupData(long envirid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                //delete records which is not locked
                sb.Append("delete exceptiondata_group where (locked=0 or locked is null) and envir_id=");
                sb.Append(envirid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                db.ExecuteNonQuery(dbc);
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// FGD
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <returns></returns>
        public DataSet GetFGDRelatedDs(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select top 1 s.* from v_ruleresult_fgd s where s.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_fgd t where t.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = 'FGD撤出' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// FGD-calib
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <returns></returns>
        public DataSet GetCalibRelatedDs_FGD(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                System.Configuration.AppSettingsReader asr = new System.Configuration.AppSettingsReader();
                string[] pts = ((string)asr.GetValue("calib_so2", typeof(string))).Split(';');
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from V_RuleResult_Calib_FGD t where t.rulename in (");
                foreach (string p in pts)
                {
                    sb.Append("'" + p + "'");
                    if (p != pts.Last())
                    {
                        sb.Append(",");
                    }
                }
                sb.Append(") and t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// FGD-special condition
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <returns></returns>
        public DataSet GetSconRelatedDs_FGD(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from V_RuleResult_SpecialCon_FGD t where t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// FGD-start stop
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <returns></returns>
        public DataSet GetStartStopDs_FGD(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select top 1 s.* from v_ruleresult_startstop_fgd s where s.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_startstop_fgd t where t.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = '机组停机' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc ");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// FGD
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public int? GetFGDRelatedCount(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                #region
                //StringBuilder sb = new StringBuilder();
                //sb.Append("select count(*) from V_RuleResult_SCR t where t.machineid = '" + machineid.ToString() + "' and ");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                //Database db = DatabaseFactory.CreateDatabase("dbconn");
                //System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                //return (int)db.ExecuteScalar(dbc);
                #endregion
                StringBuilder sb = new StringBuilder();
                sb.Append("select top 1 s.* from v_ruleresult_fgd s where s.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_fgd t where t.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = 'FGD撤出' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc ");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                DataSet ds = db.ExecuteDataSet(dbc);
                if (ds.Tables[0].Rows.Count == 0)
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// FGD-calib
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public int? GetCalibRelatedCount_FGD(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                System.Configuration.AppSettingsReader asr = new System.Configuration.AppSettingsReader();
                string[] pts = ((string)asr.GetValue("calib_so2", typeof(string))).Split(';');
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from V_RuleResult_Calib_FGD t where t.rulename in (");
                foreach (string p in pts)
                {
                    sb.Append("'" + p + "'");
                    if (p != pts.Last())
                    {
                        sb.Append(",");
                    }
                }
                sb.Append(") and t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return (int)db.ExecuteScalar(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// FGD-special condition
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public int? GetSconRelatedCount_FGD(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from V_RuleResult_SpecialCon_FGD t where t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return (int)db.ExecuteScalar(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// FGD-start stop
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public int? GetStartStopCount_FGD(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select top 1 s.* from v_ruleresult_startstop_fgd s where s.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and (select top 1 t.alarmlog from v_ruleresult_startstop_fgd t where t.TimeLog < '");
                sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.machineid = '" + machineid.ToString() + "' order by t.timelog desc) = '机组停机' and s.machineid = '" + machineid.ToString() + "' order by s.TimeLog desc");
                //sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                //sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                DataSet ds = db.ExecuteDataSet(dbc);
                if (ds.Tables[0].Rows.Count == 0)
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// DUST-calib
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public DataSet GetCalibRelatedDs_DUST(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                System.Configuration.AppSettingsReader asr = new System.Configuration.AppSettingsReader();
                string[] pts = ((string)asr.GetValue("calib_dust", typeof(string))).Split(';');
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from V_RuleResult_Calib_DUST t where t.rulename in (");
                foreach (string p in pts)
                {
                    sb.Append("'" + p + "'");
                    if (p != pts.Last())
                    {
                        sb.Append(",");
                    }
                }
                sb.Append(") and t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// DUST-calib
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="offsetahead"></param>
        /// <param name="offsetbehind"></param>
        /// <param name="machineid"></param>
        /// <returns></returns>
        public int? GetCalibRelatedCount_DUST(DateTime ts, int offsetahead, int offsetbehind, int machineid)
        {
            try
            {
                System.Configuration.AppSettingsReader asr = new System.Configuration.AppSettingsReader();
                string[] pts = ((string)asr.GetValue("calib_dust", typeof(string))).Split(';');
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from V_RuleResult_Calib_DUST t where t.rulename in (");
                foreach (string p in pts)
                {
                    sb.Append("'" + p + "'");
                    if (p != pts.Last())
                    {
                        sb.Append(",");
                    }
                }
                sb.Append(") and t.machineid = '" + machineid.ToString() + "' and ");
                sb.Append("t.timelog < '" + ts.AddHours(offsetbehind).ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog > '" + ts.AddHours(-offsetahead).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return (int)db.ExecuteScalar(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

    }
}
