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
    public class EnvExceptions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetEnvExceptionsDs(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from V_EnvirIndicatorValue_abnormal t where ");
                sb.Append("t.timestamps <= '" + et.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timestamps >= '" + st.ToString("yyyy-MM-dd HH:mm:ss") + "'");
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
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public int? GetEnvExceptionsCount(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from V_EnvirIndicatorValue_abnormal t where ");
                sb.Append("t.timestamps <= '" + et.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timestamps >= '" + st.ToString("yyyy-MM-dd HH:mm:ss") + "'");
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
