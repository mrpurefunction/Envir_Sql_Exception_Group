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
    public class Exception_Rulelog
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="envirid"></param>
        /// <param name="rulelogid"></param>
        /// <param name="typeid"></param>
        /// <returns></returns>
        public bool? IsMatchDataExist(long envirid, long rulelogid, int typeid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from envirexception_rulelog_match t where ");
                sb.Append("t.envir_id = " + envirid.ToString() + " and ");
                sb.Append("t.rule_id = " + rulelogid.ToString() + " and ");
                sb.Append("t.typeid = " + typeid.ToString());
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
        /// <param name="envirid"></param>
        /// <param name="rulelogid"></param>
        /// <param name="typeid"></param>
        public void AddMatchData(long envirid, long rulelogid, int typeid)
        {
            try
            {
                if (IsMatchDataExist(envirid, rulelogid, typeid) == false)
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into envirexception_rulelog_match(envir_id,rule_id,typeid,isshowed) values(");
                    sb.Append(envirid.ToString() + ",");
                    sb.Append(rulelogid.ToString() + ",");
                    sb.Append(typeid.ToString() + ",");
                    sb.Append(1.ToString() + ")");
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
        public void DeleteMatchData(long envirid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("delete envirexception_rulelog_match where envir_id=");
                sb.Append(envirid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                db.ExecuteNonQuery(dbc);
            }
            catch (Exception ex)
            {
            }
        }
    }
}
