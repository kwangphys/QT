using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using IBApi;

namespace DBAccess
{
    class Tester
    {
        static private string getXmlPath(string filename)
        {
            return Path.Combine(Environment.GetEnvironmentVariable("QT_XML_PATH"), filename);
        }
        static private string getMktDataPath(string filename)
        {
            return Path.Combine(Environment.GetEnvironmentVariable("QT_MKTDATA_PATH"), filename);
        }
        static void Main()
        {
            DBAccess db = DBUtils.getDB();
            //Console.WriteLine(db.getNextEventTime("EIA Report", new DateTime(2018,1,1)));
            //DBUtils.createAllTables();
            //Console.WriteLine(db.getContract(8991549).Symbol);
            //Console.WriteLine(db.getModelParameters(1, new DateTime(2015,12,4)));
            //DataTable dt = db.getHistoricalData(1, new DateTime(2013, 1, 1), new DateTime(2015, 1, 1), "close");
            //for(int i=0; i < dt.Rows.Count; ++i)
            //{
            //    for (int j = 0; j < dt.Columns.Count; ++j)
            //        Console.Write(dt.Rows[i][j].ToString() + "\t");
            //    Console.WriteLine();
            //}
            //string xml = File.ReadAllText(getXmlPath("model_parameters.xml"));
            //db.runNonQuery("insert into model_parameters values (2, 1, '2015-12-02', '" + xml + "')");
            //db.createCombos();
            //db.createOrders();
            //db.createContracts();
            //db.createCommissions();
            //db.createExecutions();
            //db.createAccounts();
            //db.createStrategies();
            //db.createModelParameters();
            //db.createPositions();
            //db.createErrors();
            //db.createSystemLog();
            //db.createEvents();
            //db.createNearbyContracts();
            //String query = @"select * from test";
            //DataTable dt = db.runQuery(query);
            //Console.WriteLine("Hello");
            //Console.WriteLine(dt.ToString());
            //String model_xml = File.ReadAllText(getXmlPath("modelPairStatArb.xml"));
            //int model_id = db.addModel(-1, "Test IWM/IWB Pair Stat Arb", model_xml, "Testing Russells Pair Trading 100/0.01");
            //List<string> ret = db.getStrategyInfo(1);
            //List<Contract> ret = db.getPairContracts(-1);
            //Console.WriteLine(ret[0].ToString());
            //Console.WriteLine(ret[1].ToString());

            //String model_xml = File.ReadAllText(getXmlPath("modelFixedReversion.xml"));
            //int model_id = db.addModel(-1, "BT_Settlement_Reversion", model_xml, "Brent Settlement Reversion");
            //String strategy_xml = File.ReadAllText(getXmlPath("strategyFixedReversion.xml"));
            //db.addStrategy("Fixed", "BT_Settlement_Reversion", strategy_xml, "Brent Settlement Reversion");

            //String strategy_xml = File.ReadAllText(getXmlPath("RussellsDailyDownloader.xml"));
            //db.addStrategy("MarketDataDownloader", "Russells_100d_100d", strategy_xml, "Russells family 100D daily data");        

            //String strategy_xml = File.ReadAllText(getXmlPath("BrentDataDownloader.xml"));
            //db.addStrategy("MarketDataDownloader", "BD_201609_60_7d", strategy_xml, "Brent Sep - Nov 2016 1 min 7d download");

            //String strategy_xml = File.ReadAllText(getXmlPath("BrentNearbyDataDownloader_Dec14-Nov15.xml"));
            //db.addStrategy("MarketDataDownloader", "BD_Dec14-Nov15Contracts_60_120d", strategy_xml, "Brent multi contracts multi endDates ");

            //String strategy_xml = File.ReadAllText(getXmlPath("XAUUSDDownloader.xml"));
            //db.addStrategy("MarketDataDownloader", "XAUUSD-1Min-16D-endingToday", strategy_xml, "XAUUSD spot gold 16D ending Today");

            //String strategy_xml = File.ReadAllText(getXmlPath("NZDUSDDownloader.xml"));
            //db.addStrategy("MarketDataDownloader", "NZDUSD-1Min-1500D-ending20161130", strategy_xml, "NZDUSD spot 1500D ending 20161122");


            //String strategy_xml = File.ReadAllText(getXmlPath("SGXDataDownloader.xml"));
            //db.addStrategy("MarketDataDownloader", "SGX_201609_60_7d", strategy_xml, "SGX Futures Sep 2016 1 min 7d download");

            //String model_xml = File.ReadAllText(getXmlPath("modelEventReversion.xml"));
            //int model_id = db.addModel(-1, "EIA", model_xml, "Brent EIA Event");
            //String strategy_xml = File.ReadAllText(getXmlPath("strategyEventReversion.xml"));
            //db.addStrategy("EventReversion", "EIA", strategy_xml, "Brent EIA Event");

            //String model_xml = File.ReadAllText(getXmlPath("modelSgxSwitch.xml"));
            //int model_id = db.addModel(134421928, "STW_201609", model_xml, "STW 201609 SGX Switch");
            //String strategy_xml = File.ReadAllText(getXmlPath("strategySgxSwitch.xml"));
            //db.addStrategy("SgxSwitch", "STW_201609", strategy_xml, "STW 201609 SGX Switch");

            //nearby contract example usage
            //int conId = db.addNearbyContract("COIL", "FUT", "IPE", "ICE Brent Crude Nearby_1 Roll_2d");
            //db.setNearbyContractSchedule(conId, 1, 2); //create nearby1 synthetic contract, where indexoffset = 2, i.e roll 2 days before actual expiry
            //int barId = db.addBarDataId(conId, "trades", 60, "IB", "");
            //db.exportBarsToFile(barId, new DateTime(2016, 9, 15), new DateTime(2016, 10, 15), getMktDataPath("test"));

            //DBUtils.generatePnLReport("MyAccount", new DateTime(2016, 12, 1), DateTime.Today, "C:\\MyReportFolder");
        }
    }
}
