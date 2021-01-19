using AMS.Drives;
using AMS.Drives.GDW130_376;
using AMS.Monitoring;
using AMS.Monitoring.AgencySearchs;
using AMS.Monitoring.AMI.GDW130_376.AutoNetWork;
using AMS.Monitoring.Database;
using AMS.Monitoring.DeviceSearchs;
using AMS.Monitoring.Forms;
using AMS.Monitoring.GDW130_376;
using AMS.Monitoring.UnitSearchs;
using AMS.Monitoring.XlCloud.Hg212;
using AMS.Monitoring.XlCloud.Hg212.Channel;
using AMS.Monitoring.XlCloud.Hg212.EventHandle;
using AMS.Monitoring.XlCloud.NBIOT;
using AMS.Monitoring.XlCloud.QKL8154;
using AMS.Monitoring.XlCloud.QKL8154.AutoNetwork;
using AMS.Monitoring.XlCloud.QKL8154.Comm;
using AMS.Monitoring.XlCloud.QKL8154.Public;
using Newtonsoft.Json;
using NPOI.HSSF.UserModel;
using NPOI.SS.UserModel;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using XlSoft.Service;
using XlSoft.Service.NLogService;
using Function = AMS.Monitoring.Function;
using IdFilter = AMS.Monitoring.DeviceSearchs.IdFilter;


namespace amsRealtimePush
{
    internal class Program
    {
        private static RealTimePushApplication m_Application;
        private static HttpClient client;

        private static string processIdentity
        {
            get
            {
                return ConfigurationManager.AppSettings["ProcessIdentity"];
            }
        }
        private static string monitorUrl
        {
            get
            {
                var url = ConfigurationManager.AppSettings["SysMonitorApiUrl"];
                if (string.IsNullOrWhiteSpace(url))
                {
                    return "";
                }
                if (!url.StartsWith("http"))
                {
                    url = "http://" + url;
                }
                return url;
            }
        }

        private static void Main(string[] args)
        {
            try
            {
                ServiceUtil.SearchAssmbly();//搜索所有dll，获取所有继承IService并且配置了Option特性的类型。
                ServiceUtil.InitConfig();
                m_Application = new RealTimePushApplication();
                m_Application.WriteLog("服务器开始初始化。。。");
                m_Application.SearchAssembly();

                m_Application.DbConnectionCloseTimer = 300000000;
                Console.WriteLine(m_Application.SystemName);
                Console.WriteLine("当前服务器名:" + System.Net.Dns.GetHostName());
                Console.WriteLine("");
                try
                {
                    m_Application.StartSysMonitor();
                }
                catch (Exception ex)
                {
                    m_Application.WriteLog("进程监测：" + ex.Message);
                }
                //Console.WriteLine("      1、自动建档");
                //Console.WriteLine("      2、WebSocket监听");
                //Console.WriteLine("      3、安全WebSocket监听(电能云)");
                //Console.WriteLine("      4、安全WebSocket监听(环保云)");
                //Console.WriteLine("      5、用户订阅事件推送");
                //Console.WriteLine("      6、环保数据上报处理");
                //Console.WriteLine("      7、维护日志是否已阅读状态");
                //Console.WriteLine("      8、环保消息推送");
                //Console.WriteLine("      9、消息推送（新）");
                //Console.WriteLine("      10、redis事件接收");
                //Console.WriteLine("      11、新组网流程");
                //Console.WriteLine("      12、新组网流程2");
                //Console.WriteLine("      13、新乡企业电量监控");
                //Console.WriteLine("      14、NBIOT-UDP服务");
                //Console.WriteLine("      15、NBIOT-TCP服务");
                //Console.WriteLine("      16、环保数据上报处理新(redis处理)");
                //Console.WriteLine("      17、测试");

                //Console.WriteLine("      1024、档案对比");
                //Console.WriteLine(" ");
                //Console.WriteLine("请选择要执行的序号:");


                int intCommand = 1024;
                //int bussinessId = 0;
                //if (args != null && args.Length > 0)
                //{
                //    intCommand = Function.ToInt(args[0]);
                //    if (args.Length > 1) { bussinessId = Function.ToInt(args[1]); }
                //}
                //else
                //{
                //    string strCommand = "";
                //    while ((strCommand = Console.ReadLine()) != "")
                //    {
                //        intCommand = Function.ToInt(strCommand);
                //        break;
                //    }
                //}
                switch (intCommand)
                {
                    case 1:
                    case 6:
                        var ercServer = new ErcServer(m_Application);
                        ercServer.StartErcSave(intCommand == 1 ? 0 : 1);


                        break;
                    //case 2:
                    //    new MqTopicServer(m_Application, bussinessId).StartWebSocket(false);
                    //    break;
                    //case 3:
                    //    new MqTopicServer(m_Application, bussinessId).StartWebSocket(true, 0);
                    //    break;
                    //case 4:
                    //    new MqTopicServer(m_Application, bussinessId).StartWebSocket(true, 1);
                    //    break;
                    case 5:
                        new MqQueueServer(m_Application).StartPushMessage();
                        break;
                    case 7:
                        new LogReadServer(m_Application).Start();
                        break;
                    case 8:
                        new MsgPush_SQHB(m_Application).StartPushMessage();
                        break;
                    case 9:
                        new MsgPushServer(m_Application).Start();
                        break;
                    case 10:
                        var RedisAutoReport1 = new RedisAutoReport(m_Application);
                        RedisAutoReport.check(RedisAutoReport1, m_Application);
                        break;
                    case 11:
                        {
                            Version2Help.Start(m_Application);
                        }
                        break;
                    case 12:

                        CallOnlineServer server = new CallOnlineServer(m_Application);
                        server.StartErcSave();

                        var RedisAutoReport2 = new RedisAutoReport2(m_Application);
                        RedisAutoReport2.check(RedisAutoReport2, m_Application);
                        break;
                    case 13:
                        new XinXiangPushData(m_Application).Start();
                        break;
                    case 14:
                        new NBIOTServer(m_Application).Start();
                        break;
                    case 15:
                        new NBIOTServerTcp(m_Application).Start();
                        break;
                    case 16:
                        new RedisHandler(m_Application);
                        break;
                    case 17:
                        break;

                    case 212:
                        var bytes = Encoding.ASCII.GetBytes("##0633QN=20210104103302025;ST=80;CN=2011;PW=123456;MN=610119270925201000002682;Flag=5;CP=&&DataTime=20210104103000;100003-Ia=74.403,100003-Ib=77.913,100003-Ic=81.639,100003-Ua=207.3,100003-Ub=205.5,100003-Uc=208.9,100003-Pt=41.8556,100003-Pa=13.5675,100003-Pb=13.5485,100003-Pc=14.7397,100003-Pdem=42.3476,100003-Ept=80223.867,100003-Epa=26919.545,100003-Epb=26086.896,100003-Epc=27217.424,100003-Eg1=52196.934,100003-Eg2=0.000,100003-Eg3=0.000,100003-Eg4=0.000,100003-Pa_ave=13.9033,100003-Pb_ave=13.7766,100003-Pc_ave=14.7833,100003-Tpa=28.0,100003-Tpb=25.7,100003-Tpc=24.2,100003-Pga=28.4,100003-Pgb=32.2,100003-Pgc=30.2,100003-Flag=N&&6900");
                        Hg212ReceiveDataHandle handle = new Hg212ReceiveDataHandle(m_Application);

                        ReadInfo(handle);

                        handle.DoReceivedData(bytes);
                        break;
                    case 1024:

                        Console.WriteLine("请输入本平台的机构ID:");
                        long orgId = Function.ToLong(Console.ReadLine());

                        Console.WriteLine("请输入站点ID:");
                        int siteid = Function.ToInt(Console.ReadLine());

                        Console.WriteLine("正在导出报表中.....");

                        //ProgressBar progressBar = new ProgressBar(Console.CursorLeft, Console.CursorTop, 50, ProgressBarType.Multicolor);
                        //List<Task> tasks = new List<Task>();
                        //TaskFactory taskFactory = new TaskFactory();
                        //int k = 0;
                        //for (int i = 0; i < 10; i++)
                        //{


                        //    Thread.Sleep(1000);
                        //    k += 1;
                        //    progressBar.Dispaly(Function.ToInt(((k * 1.0) / 10) * 100));
                        //}
                        Console.WriteLine("                                                         ");
                        Console.WriteLine("*********************************************************");
                        Console.WriteLine("              以下关系很简单,请手动查询：");
                        Console.WriteLine("      产污治污分组信息: select * from ep_unitoutunitgroup");
                        Console.WriteLine("      产污治污分组下详细信息: select * from unitoutunit");

                        Console.WriteLine("               以下是作为验证报表的SQL表关系：");
                        Console.WriteLine("      治污分组信息: select * from ep_groupconfig");
                        Console.WriteLine("      治污分组下的设备 : select * from ep_unitindevicegroup");
                        Console.WriteLine("*********************************************************");

                        //ProgressBar progressBar = new ProgressBar(Console.CursorLeft, Console.CursorTop, 50, ProgressBarType.Character);
                        var companies = CompanyBase(orgId);


                        var mapArea = GetAreaMap();
                        foreach (var query in companies)
                        {
                            //终端MN 376从扩展表取值
                            var gdwTerminals = query.DataCollectors
                                .Where(p => p.DataCollectorModelCode != typeof(Hg212DeviceNew).FullName && p.DataCollectorModelCode != typeof(Hg212Device).FullName)
                                .ToList();

                            MapAreaVm areaVm = mapArea.ContainsKey(siteid) ? mapArea[siteid] : null;
                            string extenType = "HJ212Map";
                            if (areaVm != null)
                            {
                                if (areaVm.Area != 0)
                                {
                                    extenType += areaVm.Area;
                                }
                            }

                            foreach (var teminal in gdwTerminals)
                            {
                                if (areaVm != null)
                                {
                                    teminal.mn = GetExtendMn(teminal.DeviceId, extenType);
                                }
                                else
                                {
                                    teminal.mn = "";
                                }
                            }
                            var gdwMeters = query.MonitorEquips.Where(p => !p.MonitorModelCode.Equals(typeof(HGC20Device).FullName)).ToList();
                            foreach (var meter in gdwMeters)
                            {
                                if (areaVm != null)
                                {
                                    meter.MonitorEquipSiteCode = GetExtendFactor(meter.MonitorEquipCode, extenType);
                                }
                                else
                                {
                                    meter.MonitorEquipSiteCode = "";
                                    meter.Identifier = "";
                                }
                            }

                            //V型网关下的转为终端下设备
                            var vmodels = query.MonitorEquips.Where(p => p.MonitorModelCode.Equals(typeof(VModeDevice).FullName)).ToList();
                            foreach (var vm in vmodels)
                            {
                                query.MonitorEquips.Where(p => p.DataCollectorCode == vm.DeviceId)
                                  .ToList()
                                  .ForEach(p => p.DataCollectorCode = vm.DataCollectorCode);

                                query.TotalMeters.Where(p => p.DataCollectorCode == vm.DeviceId)
                                 .ToList()
                                 .ForEach(p => p.DataCollectorCode = vm.DataCollectorCode);

                                query.MonitorEquips.Remove(vm);
                            }
                        }


                        Console.WriteLine($"读取企业档案完成");

                        var path = AppDomain.CurrentDomain.BaseDirectory + "/file";
                        if (!Directory.Exists(path))
                        {
                            Directory.CreateDirectory(path);
                        }

                        WriteCompanys(companies);
                        WriteTotalMeters(companies);
                        WriteMeters(companies);
                        WriteUnits(companies);

                        WriteEPGroup(companies);
                        Console.WriteLine("导出完成,保存路径:" + path);
                        break;

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("启动失败:" + e.Message + "。");
                Console.WriteLine(e.StackTrace);
            }

            Console.ReadKey();
        }
        private static void PostSysMonitorInfo(HttpClient client, string url, object request)
        {
            try
            {
                StringContent sc = new StringContent(JsonConvert.SerializeObject(request));

                //RedisAndMqSystemConfigData config = m_Application.GetSystemConfigData(typeof(RedisAndMqSystemConfigData)) as RedisAndMqSystemConfigData;
                if (!string.IsNullOrWhiteSpace(monitorUrl))
                {
                    client.PostAsync(monitorUrl + "/" + url, sc);
                }
            }
            catch (Exception)
            {

                throw;
            }

        }

        private static void PeroidItemSendThread()
        {
            while (true)
            {
                try
                {
                    string[] itemIds = new string[] {
                        //数据库连接数
                        "9DD10508-2DEF-4EC1-927E-8374F8345D82",
                        //总连接池数量
                        "66117FA1-E19D-4663-9222-1541FB0527CA",
                        //UnitSmartTaskInfo
                        "18BC3080-E852-4286-BD4D-2CAFC4EDBCD0",
                        //DeviceSmartTaskInfo
                        "1F4E7D6E-C9E1-496B-9503-095BA504C5AC",
                        //增删改SQL执行数量
                        "142486E3-ADBD-4E85-B8B2-F6AA4066D286",
                        //查询SQL执行数量
                        "E452840C-9F15-4995-9797-1DD3F1B75561"
                    };

                    DateTime nextExecuteTime = SysMonitorUtil.SendSysMonitorItem(monitorUrl, processIdentity, (itemIdentity) =>
                    {
                        decimal? value = null;
                        switch (itemIdentity)
                        {
                            case "9DD10508-2DEF-4EC1-927E-8374F8345D82":
                                value = m_Application.DbConnectionPool.Count;
                                break;
                            case "66117FA1-E19D-4663-9222-1541FB0527CA":
                                value = m_Application.DbConnectionPool.Count + m_Application.ReadOnlyDbConnectionPool.Count + m_Application.DbFreePool.Count + m_Application.ReadOnlyDbFreePool.Count;
                                break;
                            case "18BC3080-E852-4286-BD4D-2CAFC4EDBCD0":
                                value = UnitSmartTaskInfo.SaveValueCount;
                                break;
                            case "1F4E7D6E-C9E1-496B-9503-095BA504C5AC":
                                value = DeviceSmartTaskInfo.SaveValueCount;
                                break;
                            case "142486E3-ADBD-4E85-B8B2-F6AA4066D286":
                                var lastExecuteNonQuery = DbConnection.CurrentExecuteNonQuerySqlCount - DbConnection.LastExecuteNonQuerySqlCount;
                                DbConnection.LastExecuteNonQuerySqlCount = DbConnection.CurrentExecuteNonQuerySqlCount;
                                value = lastExecuteNonQuery;
                                break;
                            case "E452840C-9F15-4995-9797-1DD3F1B75561":
                                var lastExecuteQuery = DbConnection.CurrentExecuteQuerySqlCount - DbConnection.LastExecuteQuerySqlCount;
                                DbConnection.LastExecuteQuerySqlCount = DbConnection.CurrentExecuteQuerySqlCount;
                                value = lastExecuteQuery;
                                break;
                            default:
                                break;
                        }
                        return value;
                    }, (itemIdentity) =>
                    {
                        return itemIds.Contains(itemIdentity);
                    });
                    if (nextExecuteTime > DateTime.Now)
                    {
                        Thread.Sleep(nextExecuteTime - DateTime.Now);
                    }

                }
                catch (Exception ex)
                {
                    m_Application.WriteLog("进程监测周期线程：" + ex.Message + "\r\n" + ex.StackTrace);
                    Thread.Sleep(5000);
                }
            }

        }


        private static bool ReadInfo(Hg212ReceiveDataHandle handle)
        {

            DbConnection dbConn = null;
            try
            {
                dbConn = m_Application.GetDbConnection();
                ConcurrentDictionary<string, ConcurrentList<kob>> dicInfo = new ConcurrentDictionary<string, ConcurrentList<kob>>();
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("SELECT a.Identifier,a.deviceid,a.ratio,b.deviceaddress,a.analogId FROM analog a,device b,device d WHERE a.deviceid=d.deviceid AND b.deviceid = d.parentid AND b.deviceType = 'AMS.Monitoring.XlCloud.Hg212.Hg212Device'");
                var dr = dbConn.ExecuteReader(sb.ToString());
                while (dr.Read())
                {
                    string address = Function.ToString(dr["deviceaddress"]);
                    if (!string.IsNullOrEmpty(address))
                    {
                        if (!dicInfo.ContainsKey(address))
                        {
                            dicInfo.TryAdd(address, new ConcurrentList<kob>());
                        }

                        kob temp = new kob();
                        temp.ShuJvnum = Function.ToString(dr["Identifier"]);
                        temp.deviceid = Function.ToInt(dr["deviceid"]);
                        temp.ratio = Function.ToDecimalNull(dr["ratio"]);
                        temp.type = 1;//旧Hg212
                        temp.AnalogId = Function.ToInt(dr["analogId"]);
                        if (!string.IsNullOrEmpty(temp.ShuJvnum))
                        {
                            if (!dicInfo[address].TryAdd(temp))
                            {
                                m_Application.WriteLog("Hg212ChannelDrive-->ReadInfo():dicInfo尝试插入失败！ShuJvnum为：" + temp.ShuJvnum + ",deviceId为：" + temp.deviceid + ",ratio为：" + temp.ratio + ",type为2，" + "AnalogId为：" + temp.AnalogId);
                            }
                        }
                    }
                }
                dr.Close();

                sb = new StringBuilder();
                sb.AppendFormat("SELECT a.Identifier,a.deviceid,a.ratio,b.deviceaddress,a.analogId FROM analog a,device b,device d WHERE a.deviceid=d.deviceid AND b.deviceid = d.parentid AND b.deviceType = 'AMS.Monitoring.XlCloud.Hg212.Hg212DeviceNew'");
                dr = dbConn.ExecuteReader(sb.ToString());
                while (dr.Read())
                {
                    string address = Function.ToString(dr["deviceaddress"]);
                    if (!string.IsNullOrEmpty(address))
                    {
                        if (!dicInfo.ContainsKey(address))
                        {
                            dicInfo.TryAdd(address, new ConcurrentList<kob>());
                        }

                        kob temp = new kob();
                        temp.ShuJvnum = Function.ToString(dr["Identifier"]);
                        temp.deviceid = Function.ToInt(dr["deviceid"]);
                        temp.ratio = Function.ToDecimalNull(dr["ratio"]);
                        temp.type = 2; //新版Hg212
                        temp.AnalogId = Function.ToInt(dr["analogId"]);

                        if (!string.IsNullOrEmpty(temp.ShuJvnum))
                        {
                            if (!dicInfo[address].TryAdd(temp))
                            {
                                m_Application.WriteLog("Hg212ChannelDrive-->ReadInfo():dicInfo尝试插入失败！ShuJvnum为：" + temp.ShuJvnum + ",deviceId为：" + temp.deviceid + ",ratio为：" + temp.ratio + ",type为2，" + "AnalogId为：" + temp.AnalogId);
                            }

                        }
                    }
                }
                dr.Close();

                //_dicInfo = dicInfo;
                handle.dicInfo = dicInfo;

                string sql = " select p.deviceaddress, p.deviceid as parentId, c.deviceid, c.Identifier as PoId  from  device c , device p where   p.deviceid = c.parentid ";
                dr = dbConn.ExecuteReader(sql);
                ConcurrentDictionary<string, ConcurrentList<MeterInfo>> dicModuleInfo = new ConcurrentDictionary<string, ConcurrentList<MeterInfo>>();
                while (dr.Read())
                {
                    string address = Function.ToString(dr["deviceaddress"]);
                    int terminalId = Function.ToInt(dr["parentId"]);
                    int meterId = Function.ToInt(dr["deviceid"]);
                    string poId = Function.ToString(dr["PoId"]);

                    if (string.IsNullOrEmpty(address))
                    {
                        continue;
                    }
                    if (!dicModuleInfo.ContainsKey(address))
                    {
                        dicModuleInfo.TryAdd(address, new ConcurrentList<MeterInfo>());
                    }

                    if (!string.IsNullOrEmpty(poId))
                    {
                        MeterInfo meter = new MeterInfo();
                        meter.DeviceId = meterId;
                        meter.PoId = poId;
                        meter.TerminalId = terminalId;

                        if (!dicModuleInfo[address].TryAdd(meter))
                        {
                            m_Application.WriteLog("Hg212ChannelDrive-->ReadInfo():MeterInfo尝试插入失败！address为：" + address + ",DeviceId为：" + meterId + ",PoId为：" + poId + "TerminalId为：" + terminalId);
                        }

                    }
                }
                dr.Close();

                handle.dicModuleInfo = dicModuleInfo;
                GC.Collect();

                return true;
            }
            catch (Exception ex)
            {
                m_Application.WriteLog("Hg212ChannelDrive-->ReadInfo()报错:" + ex.Message);
                return false;
            }
            finally
            {
                if (dbConn != null) { dbConn.Close(); }
            }

        }


        public void SaveSignalAnalogs(Analogs analog, ApplicationClass application, int newDeviceId, string deviceCode)
        {
            using (var conn = application.GetDbConnection())
            {
                int maxAnalogId = Function.ToInt(conn.ExecuteScalar(" select max(analogId) from  Analog")) + 1;
                Insert saveData = new Insert("Analog");
                saveData.AddColumn("Name", analog.MatterName);
                saveData.AddColumn("Ratio", analog.Ratio);
                saveData.AddColumn("StateOption", analog.StateOption);
                saveData.AddColumn("AnalogId", maxAnalogId);
                saveData.AddColumn("OrdinalNumber", analog.OrdinalNum);
                saveData.AddColumn("DeviceId", newDeviceId);
                saveData.AddColumn("DataUnit", analog.DataUnit);
                saveData.AddColumn("DataPeriod", analog.DataPeriod);
                saveData.AddColumn("Identifier", deviceCode + "-" + analog.MatterTypeCode);
                saveData.Execute(conn);

            }
        }

        public void CreateDefaultAnalogByPostType(Device newMeter, string deviceCode)
        {
            List<string> analogTypes = new List<string> { "Ia", "Ib", "Ic", "Ua", "Ub", "Uc", "Pt", "Pa", "Pb", "Pc", "Pga", "Pgb", "Pgc", "Ept", "Epa", "Epb", "Epc", "Eg1", "Eg2", "Eg3", "Eg4" };
            int order = 0;
            foreach (var anlog in analogTypes)
            {
                try
                {
                    order++;
                    var analogRequest = new Analogs
                    {
                        //MatterId = maxAnaId,
                        EquipCode = newMeter.DeviceId,
                        MatterName = deviceCode + "-" + anlog,
                        //MatterCode = Function.ToString(dr["Identifier"]),
                        OrdinalNum = order,
                        DataUnit = 0,
                        DataPeriod = 900,
                        Ratio = 1,
                        StateOption = 0,
                        MatterTypeCode = anlog
                    };

                    SaveSignalAnalogs(analogRequest, m_Application, newMeter.DeviceId, deviceCode);
                }
                catch (Exception ex)
                {
                    m_Application.WriteLog($"设备ID:{newMeter.DeviceId} 默认analog:{anlog} 异常");
                }

            }
        }

        public class Analogs
        {
            public int MatterId { get; set; }
            public int EquipCode { get; set; }
            public string MatterName { get; set; }
            public string MatterCode { get; set; }
            public int OrdinalNum { get; set; }
            public int DataUnit { get; set; }
            public int DataPeriod { get; set; }
            public decimal Ratio { get; set; }
            public int StateOption { get; set; }

            public string MatterTypeCode { get; set; }
        }


        public static List<AMS.Monitoring.XlCloud.QKL8154.Public.CompanyBaseInfo.AsyncDataQuery> CompanyBase(long orgId)
        {

            AgencySearch search = new AgencySearch(m_Application);
            search.Filters.Add(new AMS.Monitoring.AgencySearchs.IdFilter(orgId));
            var orgs = search.Search();

            List<long> lst = new List<long>();
            foreach (var org in orgs)
            {
                lst.Add(org.OrgId);
                if (org.Children.Any())
                {
                    GetChildren(org.Children, lst);
                }
            }

            var companyIds = GetCompanyIdsByOrgId(lst);

            Console.WriteLine($"正在读取企业档案：共{companyIds.Count}家企业");

            List<AMS.Monitoring.XlCloud.QKL8154.Public.CompanyBaseInfo.AsyncDataQuery> queryList = new List<AMS.Monitoring.XlCloud.QKL8154.Public.CompanyBaseInfo.AsyncDataQuery>();
            ProgressBar progressBar = new ProgressBar(Console.CursorLeft, Console.CursorTop, 50, ProgressBarType.Multicolor);
            //BlockingCollection<CompanyBaseInfo.AsyncDataQuery> queryList = new BlockingCollection<CompanyBaseInfo.AsyncDataQuery>();
            List<Task> tasks = new List<Task>();

            var pageSize = 10;
            var page = companyIds.Count / pageSize;
            page = companyIds.Count % pageSize == 0 ? page : page + 1;

            object lockobj = new object();
            int k = 0;


            AMS.Monitoring.XlCloud.QKL8154.Public.CompanyBaseInfo companyService = new AMS.Monitoring.XlCloud.QKL8154.Public.CompanyBaseInfo();
            for (int i = 0; i < page; i++)
            {
                var batchs = companyIds.Skip(i * pageSize).Take(pageSize).ToList();

                //AMS.Monitoring.XlCloud.QKL8154.Public.CompanyBaseInfo companyService = new AMS.Monitoring.XlCloud.QKL8154.Public.CompanyBaseInfo();
                //var result = companyService.GetCompanyBaseInfo(batchs, m_Application);

                //var task = Task.Run(() =>
                //{
                try
                {
                    var result = companyService.GetCompanyBaseInfo(batchs, m_Application);
                    queryList.AddRange(result);
                    lock (lockobj)
                    {
                        k += 1;
                        progressBar.Dispaly(Function.ToInt(((k * 1.0) / page) * 100));
                    }

                    //foreach (var item in result)
                    //{
                    //    bool istrue = queryList.TryAdd(item);
                    //    if (!istrue)
                    //    {
                    //        m_Application.WriteLog($"企业:{item.UnitId}:{item.SPName}加入失败");
                    //    }
                    //}

                    //lock (lockobj)
                    //{
                    //    k += 1;
                    //    progressBar.Dispaly(Function.ToInt(((k * 1.0) / page) * 100));
                    //}
                }
                catch (Exception ex)
                {
                    m_Application.WriteLog($"企业:{ string.Join(",", batchs) }加入失败");
                }


                //});
                //tasks.Add(task);
                Thread.Sleep(10);
            }

            //Task.WaitAll(tasks.ToArray());


            //while (queryList.Any())
            //{
            //    CompanyBaseInfo.AsyncDataQuery query = queryList.Take();
            //    //queryList.TryTake(out query)
            //    lstCompany.Add(query);
            //}

            return queryList;

        }

        public static List<int> GetCompanyIdsByOrgId(List<long> orgIds)
        {
            string sql = $@"  select p.*  from pf_org_unit   p
                             inner join unit u on p.unitId =u.UnitId
                             where u.UnitTypeId = 1 and orgId in ({ string.Join(",", orgIds)}) ";

            List<int> lst = new List<int>();
            using (var conn = m_Application.GetDbConnection())
            {
                using (var dr = conn.ExecuteReader(sql))
                {
                    while (dr.Read())
                    {
                        int unitId = Function.ToInt(dr["unitid"]);
                        lst.Add(unitId);
                    }
                }
            }
            return lst;
        }

        public static void GetChildren(List<Agency> agencies, List<long> lst)
        {
            foreach (var item in agencies)
            {
                lst.Add(item.OrgId);
                if (item.Children.Any())
                {
                    GetChildren(item.Children, lst);
                }
            }
        }

        public static void WriteCompanys(List<CompanyBaseInfo.AsyncDataQuery> asyncDatas)
        {

            string sql = " select unitid,name from unit where  ";


            HSSFWorkbook workbook = new HSSFWorkbook();//创建一个excel文件
            ISheet sheet = workbook.CreateSheet("企业数据");//创建一个sheet
            IRow row = null;//设置行
            ICell cell = null;//设置单元格

            //创建第一行
            row = sheet.CreateRow(0);
            //创建第一列
            cell = row.CreateCell(0);
            //给第一列赋值
            cell.SetCellValue("企业ID");
            cell = row.CreateCell(1);
            cell.SetCellValue("企业名称");

            cell = row.CreateCell(2);
            cell.SetCellValue("社会信用代码");

            cell = row.CreateCell(3);
            cell.SetCellValue("地址");

            cell = row.CreateCell(4);
            cell.SetCellValue("运营公司");

            for (int i = 0; i < asyncDatas.Count; i++)
            {
                row = sheet.CreateRow(i + 1);//第几行
                row.CreateCell(0).SetCellValue(asyncDatas[i].UnitId);
                row.CreateCell(1).SetCellValue(asyncDatas[i].SPName);
                row.CreateCell(2).SetCellValue(asyncDatas[i].SPBaseInfo.CreditCode);
                row.CreateCell(3).SetCellValue(asyncDatas[i].SPBaseInfo.Address);
                row.CreateCell(4).SetCellValue(asyncDatas[i].SPBaseInfo.OperCompanyName);
            }

            string savePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory + "/file", "企业数据.xlsx");
            File.Delete(savePath);
            File.Create(savePath).Close();//创建完毕后，需关闭该IO通道，以使后续读写可继续进行

            using (var fs = File.OpenWrite(savePath))
            {
                workbook.Write(fs);//向打开的这个xls文件中写入数据  
            }

            workbook = null;

        }

        public static void WriteTotalMeters(List<CompanyBaseInfo.AsyncDataQuery> asyncDatas)
        {
            HSSFWorkbook workbook = new HSSFWorkbook();//创建一个excel文件
            ISheet sheet = workbook.CreateSheet("总表信息");//创建一个sheet
            IRow row = null;//设置行
            ICell cell = null;//设置单元格

            //创建第一行
            row = sheet.CreateRow(0);
            //创建第一列
            cell = row.CreateCell(0);
            //给第一列赋值
            cell.SetCellValue("企业ID");
            cell = row.CreateCell(1);
            cell.SetCellValue("企业名称");

            cell = row.CreateCell(2);
            cell.SetCellValue("设备ID");

            cell = row.CreateCell(3);
            cell.SetCellValue("设备名称");

            int i = 0;
            foreach (var company in asyncDatas)
            {
                foreach (var meter in company.TotalMeters)
                {
                    row = sheet.CreateRow(i + 1);//第几行
                    row.CreateCell(0).SetCellValue(company.UnitId);
                    row.CreateCell(1).SetCellValue(company.SPName);
                    row.CreateCell(2).SetCellValue(meter.DeviceId);
                    row.CreateCell(3).SetCellValue(meter.TotalMeterName);
                    i += 1;
                }
            }

            string savePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory + "/file", "总表信息.xlsx");
            File.Delete(savePath);
            File.Create(savePath).Close();//创建完毕后，需关闭该IO通道，以使后续读写可继续进行

            using (var fs = File.OpenWrite(savePath))
            {
                workbook.Write(fs);//向打开的这个xls文件中写入数据  
            }

            workbook = null;

        }

        public static void WriteMeters(List<CompanyBaseInfo.AsyncDataQuery> asyncDatas)
        {
            HSSFWorkbook workbook = new HSSFWorkbook();//创建一个excel文件
            ISheet sheet = workbook.CreateSheet("监测点信息");//创建一个sheet
            IRow row = null;//设置行
            ICell cell = null;//设置单元格

            //创建第一行
            row = sheet.CreateRow(0);
            //创建第一列
            cell = row.CreateCell(0);
            //给第一列赋值
            cell.SetCellValue("单元ID");
            cell = row.CreateCell(1);
            cell.SetCellValue("单元名称");

            cell = row.CreateCell(2);
            cell.SetCellValue("设备ID");

            cell = row.CreateCell(3);
            cell.SetCellValue("设备名称");

            cell = row.CreateCell(4);
            cell.SetCellValue("点位编码");

            cell = row.CreateCell(5);
            cell.SetCellValue("MN号");

            int i = 0;
            foreach (var company in asyncDatas)
            {
                foreach (var pFUnit in company.MFs)
                {
                    foreach (var indevices in pFUnit.MonitorEquipConfigs)
                    {
                        row = sheet.CreateRow(i + 1);//第几行
                        row.CreateCell(0).SetCellValue(pFUnit.UnitId);
                        row.CreateCell(1).SetCellValue(pFUnit.MFName);
                        row.CreateCell(2).SetCellValue(indevices.MonitorEquipCode);

                        var device = company.MonitorEquips.FirstOrDefault(p => p.MonitorEquipCode == indevices.MonitorEquipCode);
                        if (device != null)
                        {
                            row.CreateCell(3).SetCellValue(device.MonitorEquipName);
                            row.CreateCell(4).SetCellValue(device.Identifier);
                        }
                        else
                        {
                            row.CreateCell(3).SetCellValue("");
                            row.CreateCell(4).SetCellValue("");
                        }

                        var terminal = company.DataCollectors.FirstOrDefault(p => device.DataCollectorCode == Function.ToInt(p.DataCollectorCode));
                        if (terminal == null)
                        {
                            row.CreateCell(5).SetCellValue("");
                        }
                        else
                        {
                            row.CreateCell(5).SetCellValue(terminal.mn);
                        }


                        i += 1;
                    }

                }

                foreach (var ePUnits in company.TFs)
                {
                    foreach (var indevices in ePUnits.UnitIndevices)
                    {
                        row = sheet.CreateRow(i + 1);//第几行
                        row.CreateCell(0).SetCellValue(ePUnits.UnitId);
                        row.CreateCell(1).SetCellValue(ePUnits.TFName);
                        row.CreateCell(2).SetCellValue(indevices.MonitorEquipCode);

                        var device = company.MonitorEquips.FirstOrDefault(p => p.MonitorEquipCode == indevices.MonitorEquipCode);
                        if (device != null)
                        {
                            row.CreateCell(3).SetCellValue(device.MonitorEquipName);
                            row.CreateCell(4).SetCellValue(device.Identifier);
                        }
                        else
                        {
                            row.CreateCell(3).SetCellValue("");
                            row.CreateCell(4).SetCellValue("");
                        }

                        var terminal = company.DataCollectors.FirstOrDefault(p => device.DataCollectorCode == Function.ToInt(p.DataCollectorCode));
                        if (terminal == null)
                        {
                            row.CreateCell(5).SetCellValue("");
                        }
                        else
                        {
                            row.CreateCell(5).SetCellValue(terminal.mn);
                        }
                        i += 1;
                    }

                }

            }

            string savePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory + "/file", "监测点信息.xlsx");
            File.Delete(savePath);
            File.Create(savePath).Close();//创建完毕后，需关闭该IO通道，以使后续读写可继续进行

            using (var fs = File.OpenWrite(savePath))
            {
                workbook.Write(fs);//向打开的这个xls文件中写入数据  
            }

            workbook = null;

        }

        public static void WriteUnits(List<CompanyBaseInfo.AsyncDataQuery> asyncDatas)
        {
            HSSFWorkbook workbook = new HSSFWorkbook();//创建一个excel文件
            ISheet sheet = workbook.CreateSheet("产污信息表");//创建一个sheet
            IRow row = null;//设置行
            ICell cell = null;//设置单元格

            //创建第一行
            row = sheet.CreateRow(0);
            //创建第一列
            cell = row.CreateCell(0);
            //给第一列赋值
            cell.SetCellValue("企业ID");
            cell = row.CreateCell(1);
            cell.SetCellValue("企业名称");

            cell = row.CreateCell(2);
            cell.SetCellValue("社会信用代码");

            cell = row.CreateCell(3);
            cell.SetCellValue("运营公司");

            cell = row.CreateCell(4);
            cell.SetCellValue("单元ID");

            cell = row.CreateCell(5);
            cell.SetCellValue("单元名称");

            int i = 0;
            foreach (var company in asyncDatas)
            {
                foreach (var pFUnit in company.MFs)
                {
                    row = sheet.CreateRow(i + 1);//第几行

                    row.CreateCell(0).SetCellValue(company.UnitId);
                    row.CreateCell(1).SetCellValue(company.SPName);
                    row.CreateCell(2).SetCellValue(company.SPBaseInfo.CreditCode);
                    row.CreateCell(3).SetCellValue(company.SPBaseInfo.OperCompanyName);
                    row.CreateCell(4).SetCellValue(pFUnit.UnitId);
                    row.CreateCell(5).SetCellValue(pFUnit.MFName);
                    i += 1;
                }
            }

            sheet = workbook.CreateSheet("治污信息关系表");//创建一个sheet
            row = null;//设置行
            cell = null;//设置单元格


            //创建第一行
            row = sheet.CreateRow(0);
            //创建第一列
            cell = row.CreateCell(0);
            //给第一列赋值
            cell.SetCellValue("企业ID");
            cell = row.CreateCell(1);
            cell.SetCellValue("企业名称");

            cell = row.CreateCell(2);
            cell.SetCellValue("社会信用代码");

            cell = row.CreateCell(3);
            cell.SetCellValue("运营公司");

            cell = row.CreateCell(4);
            cell.SetCellValue("单元ID");

            cell = row.CreateCell(5);
            cell.SetCellValue("单元名称");

            i = 0;
            foreach (var company in asyncDatas)
            {
                foreach (var ePUnits in company.TFs)
                {
                    row = sheet.CreateRow(i + 1);//第几行

                    row.CreateCell(0).SetCellValue(company.UnitId);
                    row.CreateCell(1).SetCellValue(company.SPName);
                    row.CreateCell(2).SetCellValue(company.SPBaseInfo.CreditCode);
                    row.CreateCell(3).SetCellValue(company.SPBaseInfo.OperCompanyName);
                    row.CreateCell(4).SetCellValue(ePUnits.UnitId);
                    row.CreateCell(5).SetCellValue(ePUnits.TFName);
                    i += 1;
                }
            }


            string savePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory + "/file", "产污治污信息.xlsx");
            File.Delete(savePath);
            File.Create(savePath).Close();//创建完毕后，需关闭该IO通道，以使后续读写可继续进行

            using (var fs = File.OpenWrite(savePath))
            {
                workbook.Write(fs);//向打开的这个xls文件中写入数据  
            }

            workbook = null;

        }

        public static void WriteEPGroup(List<CompanyBaseInfo.AsyncDataQuery> asyncDatas)
        {
            HSSFWorkbook workbook = new HSSFWorkbook();//创建一个excel文件
            ISheet sheet = workbook.CreateSheet("治污下分组");//创建一个sheet
            IRow row = null;//设置行
            ICell cell = null;//设置单元格

            //创建第一行
            row = sheet.CreateRow(0);
            //创建第一列
            cell = row.CreateCell(0);
            //给第一列赋值
            cell.SetCellValue("单元ID");
            cell = row.CreateCell(1);
            cell.SetCellValue("单元名称");

            cell = row.CreateCell(2);
            cell.SetCellValue("组ID");

            int i = 0;
            foreach (var company in asyncDatas)
            {
                foreach (var pFUnit in company.TFs)
                {

                    foreach (var group in pFUnit.MonitorGroupConfig)
                    {
                        row = sheet.CreateRow(i + 1);//第几行
                        row.CreateCell(0).SetCellValue(pFUnit.UnitId);
                        row.CreateCell(1).SetCellValue(pFUnit.TFName);
                        row.CreateCell(2).SetCellValue(group.MonitorGroupNum);
                        i += 1;
                    }
                }
            }

            sheet = workbook.CreateSheet("治污分组下的设备");//创建一个sheet
            row = null;//设置行
            cell = null;//设置单元格


            //创建第一行
            row = sheet.CreateRow(0);
            //创建第一列
            cell = row.CreateCell(0);
            //给第一列赋值
            cell.SetCellValue("单元ID");
            cell = row.CreateCell(1);
            cell.SetCellValue("单元名称");

            cell = row.CreateCell(2);
            cell.SetCellValue("组ID");

            cell = row.CreateCell(3);
            cell.SetCellValue("设备ID");

            cell = row.CreateCell(4);
            cell.SetCellValue("设备名称");


            i = 0;
            foreach (var company in asyncDatas)
            {
                foreach (var ePUnits in company.TFs)
                {

                    foreach (var item in ePUnits.MonitorEquipConfig)
                    {
                        int deviceId = item.MonitorEquipCode;
                        int groupId = item.MonitorGroupNum;

                        row = sheet.CreateRow(i + 1);//第几行

                        row.CreateCell(0).SetCellValue(ePUnits.UnitId);
                        row.CreateCell(1).SetCellValue(ePUnits.TFName);
                        row.CreateCell(2).SetCellValue(groupId);
                        row.CreateCell(3).SetCellValue(deviceId);

                        var device = company.MonitorEquips.FirstOrDefault(p => p.MonitorEquipCode == deviceId);
                        if (device == null)
                        {
                            row.CreateCell(4).SetCellValue("");
                        }
                        else
                        {
                            row.CreateCell(4).SetCellValue(device.MonitorEquipName);
                        }
                        i += 1;

                    }


                }
            }


            string savePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory + "/file", "治污下分组信息.xlsx");
            File.Delete(savePath);
            File.Create(savePath).Close();//创建完毕后，需关闭该IO通道，以使后续读写可继续进行

            using (var fs = File.OpenWrite(savePath))
            {
                workbook.Write(fs);//向打开的这个xls文件中写入数据  
            }

            workbook = null;

        }

        public class MapAreaVm
        {
            public int Area { get; set; }
            public string Port { get; set; }
            public string IP { get; set; }
            public string Ac { get; set; }
        }

        /// <summary>
        /// 获取 区域映射关系
        /// </summary>
        public static Dictionary<int, MapAreaVm> GetAreaMap()
        {
            Dictionary<int, MapAreaVm> dicArea = new Dictionary<int, MapAreaVm>();
            using (var conn = m_Application.GetDbConnection())
            {
                string sql = $" select *  from siteareamap  ";
                using (IDbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    using (var dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            int siteId = Function.ToInt(dr["siteId"]);
                            int area = Function.ToInt(dr["maparea"]);
                            string ip = Function.ToString(dr["IP"]);
                            string port = Function.ToString(dr["Port"]);
                            string ac = Function.ToString(dr["Active"]);

                            if (!dicArea.ContainsKey(siteId))
                            {
                                dicArea.Add(siteId, new MapAreaVm
                                {
                                    Area = area,
                                    Ac = ac,
                                    IP = ip,
                                    Port = port
                                });
                            }
                        }
                    }
                }
            }
            return dicArea;
        }

        /// <summary>
        /// 获取mn号
        /// </summary>
        /// <param name="deviceId"></param>
        /// <param name="extendtype"></param>
        /// <returns></returns>
        public static string GetExtendMn(int deviceId, string extendtype)
        {
            string mn = "";
            using (var conn = m_Application.GetDbConnection())
            {
                mn = Function.ToString(conn.ExecuteScalar($" Select DataValue from  deviceextendstring where deviceid={deviceId} and extendtype = '{extendtype}' and serialnumber =0 "));
            }
            return mn;
        }

        /// <summary>
        /// 获取376点位编码
        /// </summary>
        /// <param name="deviceId"></param>
        /// <param name="extendtype"></param>
        /// <returns></returns>
        public static string GetExtendFactor(int deviceId, string extendtype)
        {
            string factor = "";
            using (var conn = m_Application.GetDbConnection())
            {
                factor = Function.ToString(conn.ExecuteScalar($" select DataValue from deviceextendstring where deviceid ={deviceId} and extendtype = '{extendtype}' and serialnumber > 0 "));
            }
            return factor;
        }
        public void GetCompanys(List<int> companyIds)
        {
            UnitSearch us = new UnitSearch(m_Application);
            us.Filters.Add(new AMS.Monitoring.XlCloud.UnitSearchs.UnitIdFilter(companyIds));
            UnitCollection uc = us.Search();
        }
    }




    /// <summary>
    /// 进度条类型
    /// </summary>
    public enum ProgressBarType
    {
        /// <summary>
        /// 字符
        /// </summary>
        Character,
        /// <summary>
        /// 彩色
        /// </summary>
        Multicolor
    }

    public class ProgressBar
    {

        /// <summary>
        /// 光标的列位置。将从 0 开始从左到右对列进行编号。
        /// </summary>
        public int Left { get; set; }
        /// <summary>
        /// 光标的行位置。从上到下，从 0 开始为行编号。
        /// </summary>
        public int Top { get; set; }

        /// <summary>
        /// 进度条宽度。
        /// </summary>
        public int Width { get; set; }
        /// <summary>
        /// 进度条当前值。
        /// </summary>
        public int Value { get; set; }
        /// <summary>
        /// 进度条类型
        /// </summary>
        public ProgressBarType ProgressBarType { get; set; }


        private ConsoleColor colorBack;
        private ConsoleColor colorFore;


        public ProgressBar() : this(Console.CursorLeft, Console.CursorTop)
        {

        }

        public ProgressBar(int left, int top, int width = 50, ProgressBarType ProgressBarType = ProgressBarType.Multicolor)
        {
            this.Left = left;
            this.Top = top;
            this.Width = width;
            this.ProgressBarType = ProgressBarType;

            // 清空显示区域；
            Console.SetCursorPosition(Left, Top);
            for (int i = left; ++i < Console.WindowWidth;) { Console.Write(" "); }

            if (this.ProgressBarType == ProgressBarType.Multicolor)
            {
                // 绘制进度条背景；
                colorBack = Console.BackgroundColor;
                Console.SetCursorPosition(Left, Top);
                Console.BackgroundColor = ConsoleColor.DarkCyan;
                for (int i = 0; ++i <= width;) { Console.Write(" "); }
                Console.BackgroundColor = colorBack;
            }
            else
            {
                // 绘制进度条背景；
                Console.SetCursorPosition(left, top);
                Console.Write("[");
                Console.SetCursorPosition(left + width - 1, top);
                Console.Write("]");
            }
        }

        public int Dispaly(int value)
        {
            return Dispaly(value, null);
        }

        public int Dispaly(int value, string msg)
        {
            if (this.Value != value)
            {
                this.Value = value;

                if (this.ProgressBarType == ProgressBarType.Multicolor)
                {
                    // 保存背景色与前景色；
                    colorBack = Console.BackgroundColor;
                    colorFore = Console.ForegroundColor;
                    // 绘制进度条进度
                    Console.BackgroundColor = ConsoleColor.Green;
                    Console.SetCursorPosition(this.Left, this.Top);
                    Console.Write(new string(' ', (int)Math.Round(this.Value / (100.0 / this.Width))));
                    Console.BackgroundColor = colorBack;

                    // 更新进度百分比,原理同上.
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.SetCursorPosition(this.Left + this.Width + 1, this.Top);
                    if (string.IsNullOrWhiteSpace(msg)) { Console.Write("{0}%", this.Value); } else { Console.Write(msg); }
                    Console.ForegroundColor = colorFore;
                }
                else
                {
                    // 绘制进度条进度
                    Console.SetCursorPosition(this.Left + 1, this.Top);
                    Console.Write(new string('*', (int)Math.Round(this.Value / (100.0 / (this.Width - 2)))));
                    // 显示百分比
                    Console.SetCursorPosition(this.Left + this.Width + 1, this.Top);
                    if (string.IsNullOrWhiteSpace(msg)) { Console.Write("{0}%", this.Value); } else { Console.Write(msg); }
                }
            }
            return value;
        }
    }

}


