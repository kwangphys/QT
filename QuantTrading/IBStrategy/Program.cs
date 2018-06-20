using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Xml;
using DBAccess;
using IBApi;

namespace IBStrategy
{
    class Program
    {
        //IBStrategy.exe --strategies=43,44,42 --run_config=E:\Temp\AccountConfig.xml
        static void Main(string[] args)
        {
            List<int> strategy_ids = new List<int>();
            string run_config_file;
            Dictionary<string, string> argmap = new Dictionary<string, string>();
            if (args != null && args.Length > 0)
            {
                foreach (string arg in args)
                {
                    if (arg.StartsWith("--strategies"))
                        argmap.Add("strategies", arg.Substring(13));
                    if (arg.StartsWith("--run_config"))
                        argmap.Add("run_config", arg.Substring(13));
                }
            }
            if(!argmap.ContainsKey("run_config"))
            {
                Console.Write("Please input run config file: ");
                argmap.Add("run_config", Console.ReadLine());
            }
            if (!argmap.ContainsKey("strategies"))
            {
                Console.Write("Please input strategy ids: ");
                argmap.Add("strategies", Console.ReadLine());
            }
            string[] strats = argmap["strategies"].Split(',');
            int ret;
            foreach (string strat in strats)
                if (Int32.TryParse(strat, out ret))
                {
                    strategy_ids.Add(ret);
                    Console.WriteLine("Adding strategy " + ret.ToString());
                }
            run_config_file = argmap["run_config"];
            Console.WriteLine("Run Config: " + run_config_file);
            string run_config = File.ReadAllText(run_config_file);
            XmlDocument xml = new XmlDocument();
            xml.LoadXml(run_config);
            XmlNode xn1 = xml.SelectSingleNode("/Config");
            string server = xn1["Server"].InnerText;
            string database = xn1["Database"].InnerText;
            //string engine_type = xn1["EngineType"].InnerText;
            DBAccess.DBAccess.instance.connect(server, database);
            Thread.Sleep(1000);

            EWrapperImpl ibClient = new EWrapperImpl(run_config, true);
            string engine_type = xn1["EngineType"].InnerText;
            if (engine_type == "TWS")
            {
                XmlNode xnib = xn1.SelectSingleNode("IBConfig");
                string ip = xnib["IP"].InnerText;
                int port = Int32.Parse(xnib["Port"].InnerText);
                bool extraAuth = Boolean.Parse(xnib["ExtraAuth"].InnerText);
                ibClient.IBClientSocket.eConnect(ip, port, 0, extraAuth);
                var reader = new EReader(ibClient.IBClientSocket, ibClient.Signal);
                reader.Start();
                new Thread(() => { while (ibClient.IBClientSocket.IsConnected()) { ibClient.Signal.waitForSignal(); reader.processMsgs(); } }) { IsBackground = true }.Start();
            }
            while (ibClient.nextOrderId <= 0) { Thread.Sleep(100); }
            OrderManager.instance.set(Math.Max(ibClient.nextOrderId - 1, DBAccess.DBAccess.instance.getMaxOrderId()));
            ibClient.initialize(strategy_ids);

            //Utilities.addSGXFutures(ibClient);
            //Utilities.addOilFutures(ibClient);
            //Utilities.addNYMEXFutures(ibClient);
            //Utilities.addUSLiquidETFs(ibClient);
            //Utilities.addUSHighVolStocks(ibClient);
            //Utilities.addPreciousMetalSpots(ibClient);
            //Utilities.addFXSpots(ibClient);
            if (engine_type == "TWS")
            {
                while (true) { };
            }
            else
                ibClient.BacktestClientSocket.run();
        }
    }
}
