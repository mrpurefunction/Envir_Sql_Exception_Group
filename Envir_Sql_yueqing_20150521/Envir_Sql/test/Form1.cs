using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Envir_ExceptionGroup;

namespace test
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            System.Data.DataSet ds_expt = (new EnvExceptions()).GetEnvExceptionsDs(DateTime.Now.AddMonths(-1), DateTime.Now);
            if(ds_expt !=null)
            {
                foreach (System.Data.DataRow dr_expt in ds_expt.Tables[0].Rows)
                {
                    //NOx
                    if (int.Parse(dr_expt["indicatorid"].ToString()) == 3)
                    {
                        long envid = long.Parse(dr_expt["id"].ToString());
                        //scr,typeid:1
                        System.Data.DataSet ds_scr = (new Exception_Group()).GetScrRelatedDs(DateTime.Parse(dr_expt["timestamps"].ToString()), 3/*offset hour*/, 3, int.Parse(dr_expt["pointid"].ToString()));
                        if (ds_scr != null)
                        {
                            foreach (System.Data.DataRow dr_scr in ds_scr.Tables[0].Rows)
                            {
                                (new Exception_Rulelog()).AddMatchData(envid, long.Parse(dr_scr["id"].ToString()), 1);
                            }
                        }
                        //calibration,typeid:3
                        System.Data.DataSet ds_calib = (new Exception_Group()).GetCalibRelatedDs(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        if (ds_calib != null)
                        {
                            foreach (System.Data.DataRow dr_calib in ds_calib.Tables[0].Rows)
                            {
                                (new Exception_Rulelog()).AddMatchData(envid, long.Parse(dr_calib["id"].ToString()), 3);
                            }
                        }
                        //special condition,typeid:2
                        System.Data.DataSet ds_scon = (new Exception_Group()).GetSconRelatedDs(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        if (ds_scon != null)
                        {
                            foreach (System.Data.DataRow dr_scon in ds_scon.Tables[0].Rows)
                            {
                                (new Exception_Rulelog()).AddMatchData(envid, long.Parse(dr_scon["id"].ToString()), 2);
                            }
                        }
                        //machine start and stop,typeid:15
                        System.Data.DataSet ds_ss = (new Exception_Group()).GetStartStopDs(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        if (ds_ss != null)
                        {
                            foreach (System.Data.DataRow dr_ss in ds_ss.Tables[0].Rows)
                            {
                                (new Exception_Rulelog()).AddMatchData(envid, long.Parse(dr_ss["id"].ToString()), 15);
                            }
                        }

                        //获取记录数
                        int? scrcount = (new Exception_Group()).GetScrRelatedCount(DateTime.Parse(dr_expt["timestamps"].ToString()), 3, 3, int.Parse(dr_expt["pointid"].ToString()));
                        int? sconcount = (new Exception_Group()).GetSconRelatedCount(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        int? calibcount = (new Exception_Group()).GetCalibRelatedCount(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        int? startstopcount = (new Exception_Group()).GetStartStopCount(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        if ((scrcount != null) && (sconcount != null) && (calibcount != null) && (startstopcount != null))
                        {
                            //如果有两类及以上关联数据
                            if (((scrcount > 0) && (sconcount > 0)) || ((scrcount > 0) && (calibcount > 0)) || ((scrcount > 0) && (startstopcount > 0)) || ((sconcount > 0) && (calibcount > 0)) || ((sconcount > 0) && (startstopcount > 0)) || ((startstopcount > 0) && (calibcount > 0)))
                            {
                                if (startstopcount > 0)
                                {
                                    (new Exception_Group()).AddExceptionGroupData(envid, 15, "", 1);
                                    continue;
                                }
                                if (scrcount > 0)
                                {
                                    (new Exception_Group()).AddExceptionGroupData(envid, 1, "", 1);
                                    continue;
                                }
                                if (sconcount > 0)
                                {
                                    (new Exception_Group()).AddExceptionGroupData(envid, 2, "", 1);
                                    continue;
                                }
                                if (calibcount > 0)
                                {
                                    (new Exception_Group()).AddExceptionGroupData(envid, 3, "", 1);
                                    continue;
                                }
                            }
                            //有一类关联数据
                            if (scrcount > 0)
                            {
                                (new Exception_Group()).AddExceptionGroupData(envid, 1, "", 0);
                                continue;
                            }
                            if (sconcount > 0)
                            {
                                (new Exception_Group()).AddExceptionGroupData(envid, 2, "", 0);
                                continue;
                            }
                            if (calibcount > 0)
                            {
                                (new Exception_Group()).AddExceptionGroupData(envid, 3, "", 0);
                                continue;
                            }
                            if (startstopcount > 0)
                            {
                                (new Exception_Group()).AddExceptionGroupData(envid, 15, "", 0);
                                continue;
                            }
                        }
                    }
                    //SO2
                    if (int.Parse(dr_expt["indicatorid"].ToString()) == 1)
                    {
                        #region
                        //DataTable dt = new DataTable();
                        //dt.Columns.Add("a");
                        //DataRow dr = dt.NewRow();
                        //dr["a"] = "xyz";
                        //dt.Rows.Add(dr);        
                        #endregion
                        long envid = long.Parse(dr_expt["id"].ToString());
                        //fgd,typeid:16
                        System.Data.DataSet ds_fgd = (new Exception_Group()).GetFGDRelatedDs(DateTime.Parse(dr_expt["timestamps"].ToString()), 3/*offset hour*/, 3, int.Parse(dr_expt["pointid"].ToString()));
                        if (ds_fgd != null)
                        {
                            foreach (System.Data.DataRow dr_fgd in ds_fgd.Tables[0].Rows)
                            {
                                (new Exception_Rulelog()).AddMatchData(envid, long.Parse(dr_fgd["id"].ToString()), 16);
                            }
                        }
                        //calibration,typeid:3
                        System.Data.DataSet ds_calib = (new Exception_Group()).GetCalibRelatedDs_FGD(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        if (ds_calib != null)
                        {
                            foreach (System.Data.DataRow dr_calib in ds_calib.Tables[0].Rows)
                            {
                                (new Exception_Rulelog()).AddMatchData(envid, long.Parse(dr_calib["id"].ToString()), 3);
                            }
                        }
                        //special condition,typeid:2
                        System.Data.DataSet ds_scon = (new Exception_Group()).GetSconRelatedDs_FGD(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        if (ds_scon != null)
                        {
                            foreach (System.Data.DataRow dr_scon in ds_scon.Tables[0].Rows)
                            {
                                (new Exception_Rulelog()).AddMatchData(envid, long.Parse(dr_scon["id"].ToString()), 2);
                            }
                        }
                        //machine start and stop,typeid:15
                        System.Data.DataSet ds_ss = (new Exception_Group()).GetStartStopDs_FGD(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        if (ds_ss != null)
                        {
                            foreach (System.Data.DataRow dr_ss in ds_ss.Tables[0].Rows)
                            {
                                (new Exception_Rulelog()).AddMatchData(envid, long.Parse(dr_ss["id"].ToString()), 15);
                            }
                        }

                        //获取记录数
                        int? fgdcount = (new Exception_Group()).GetFGDRelatedCount(DateTime.Parse(dr_expt["timestamps"].ToString()), 3, 3, int.Parse(dr_expt["pointid"].ToString()));
                        int? sconcount = (new Exception_Group()).GetSconRelatedCount_FGD(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        int? calibcount = (new Exception_Group()).GetCalibRelatedCount_FGD(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        int? startstopcount = (new Exception_Group()).GetStartStopCount_FGD(DateTime.Parse(dr_expt["timestamps"].ToString()), 2, 2, int.Parse(dr_expt["pointid"].ToString()));
                        if ((fgdcount != null) && (sconcount != null) && (calibcount != null) && (startstopcount != null))
                        {
                            //如果有两类及以上关联数据
                            if (((fgdcount > 0) && (sconcount > 0)) || ((fgdcount > 0) && (calibcount > 0)) || ((fgdcount > 0) && (startstopcount > 0)) || ((sconcount > 0) && (calibcount > 0)) || ((sconcount > 0) && (startstopcount > 0)) || ((startstopcount > 0) && (calibcount > 0)))
                            {
                                if (startstopcount > 0)
                                {
                                    (new Exception_Group()).AddExceptionGroupData(envid, 15, "", 1);
                                    continue;
                                }
                                if (fgdcount > 0)
                                {
                                    (new Exception_Group()).AddExceptionGroupData(envid, 16, "", 1);
                                    continue;
                                }
                                if (sconcount > 0)
                                {
                                    (new Exception_Group()).AddExceptionGroupData(envid, 2, "", 1);
                                    continue;
                                }
                                if (calibcount > 0)
                                {
                                    (new Exception_Group()).AddExceptionGroupData(envid, 3, "", 1);
                                    continue;
                                }
                            }
                            //有一类关联数据
                            if (fgdcount > 0)
                            {
                                (new Exception_Group()).AddExceptionGroupData(envid, 16, "", 0);
                                continue;
                            }
                            if (sconcount > 0)
                            {
                                (new Exception_Group()).AddExceptionGroupData(envid, 2, "", 0);
                                continue;
                            }
                            if (calibcount > 0)
                            {
                                (new Exception_Group()).AddExceptionGroupData(envid, 3, "", 0);
                                continue;
                            }
                            if (startstopcount > 0)
                            {
                                (new Exception_Group()).AddExceptionGroupData(envid, 15, "", 0);
                                continue;
                            }
                        }

                    }
                    //Dust
                    if (int.Parse(dr_expt["indicatorid"].ToString()) == 11)
                    {

                    }
                }
            }
        }
    }
}
