using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml;

namespace DBAccess
{
    public class DBUtils
    {
        static DBAccess db = DBAccess.instance;
        static DBUtils()
        {
            db.connect(Environment.GetEnvironmentVariable("QT_DB_NAME"), "UAT");
        }
        public static DBAccess getDB()
        {
            return db;
        }
        public static void createAllTables()
        {
            db.createAccounts();
            db.createBarData();
            db.createBarDataList();
            db.createCalendars();
            db.createCommissions();
            db.createContractDetails();
            db.createContracts();
            db.createErrors();
            db.createEvents();
            db.createExchanges();
            db.createExecutions();
            db.createHolidays();
            db.createModelParameters();
            db.createModels();
            db.createOrders();
            db.createOrderStatus();
            db.createCombos();
            db.createPositions();
            db.createStrategies();
            db.createSystemLog();
            db.createTickData();
            db.createTickDataList();
            db.createNearbyContracts();
        }
        // To use this function, strategy_templates must contain exactly 3 strategy ids corresponding to 3:00-4:30, 3:30-4:30 and 4:30-6:30
        public static void rollTimingStrategy(int[] strategy_templates, int new_contract_id, string new_maturity_tag, string suffix = "")
        {
            string[] model_tags = new string[]
            {
                "BT_1_" + new_maturity_tag + "_DST",
                "BT_2_" + new_maturity_tag + "_DST",
                "BT_3_" + new_maturity_tag + "_DST"
            };
            string[] model_descriptions = new string[]
            {
                "Brent " + new_maturity_tag + " 3:00pm - 4:30pm DST",
                "Brent " + new_maturity_tag + " 3:30pm - 4:30pm DST",
                "Brent " + new_maturity_tag + " 4:30pm - 6:30pm DST"
            };
            string[] strategy_tags = new string[]
            {
                "BT_1_" + new_maturity_tag + "_DST",
                "BT_2_" + new_maturity_tag + "_DST",
                "BT_3_" + new_maturity_tag + "_DST"
            };
            string[] strategy_descriptions = new string[]
            {
                "Brent " + new_maturity_tag + " 3:00pm - 4:30pm DST",
                "Brent " + new_maturity_tag + " 3:30pm - 4:30pm DST",
                "Brent " + new_maturity_tag + " 4:30pm - 6:30pm DST"
            };
            if (suffix != "")
            {
                for (int i = 0; i < 3; ++i)
                {
                    strategy_tags[i] += "_" + suffix;
                    strategy_descriptions[i] += " " + suffix;
                }
            }
            for (int i = 0; i < 3; ++i)
            {
                List<string> infos = db.getStrategyInfo(strategy_templates[i]);
                string strategyClass = infos[0];
                XmlDocument xml = new XmlDocument();
                xml.LoadXml(infos[1]);
                XmlNode xn1 = xml.SelectSingleNode("/Config");
                XmlNode common = xn1.SelectSingleNode("Common");
                XmlNode blockOrder = xn1.SelectSingleNode("BlockOrder");
                XmlNode stopOrderTemplate = blockOrder.SelectSingleNode("StopOrderTemplate");
                int template_model_id = int.Parse(xn1["Model"].InnerText);
                XmlDocument modelXml = new XmlDocument();
                modelXml.LoadXml(db.getModel(template_model_id));
                XmlNode mn1 = modelXml.SelectSingleNode("/Parameters");

                XmlWriterSettings settings = new XmlWriterSettings();
                settings.Encoding = Encoding.UTF8;
                settings.OmitXmlDeclaration = true;
                //Write Model Xml
                StringBuilder sm = new StringBuilder();
                using (XmlWriter writer = XmlWriter.Create(sm, settings))
                {
                    //writer.WriteStartDocument();
                    writer.WriteStartElement("Parameters");
                    writer.WriteElementString("Asset", new_contract_id.ToString());
                    writer.WriteElementString("EntryTime", mn1["EntryTime"].InnerText);
                    writer.WriteElementString("ExitTime", mn1["ExitTime"].InnerText);
                    writer.WriteElementString("LongOrShort", mn1["LongOrShort"].InnerText);
                    writer.WriteElementString("LookbackPeriod", mn1["LookbackPeriod"].InnerText);
                    writer.WriteElementString("ProfitRatio", mn1["ProfitRatio"].InnerText);
                    writer.WriteElementString("StopRatio", mn1["StopRatio"].InnerText);
                    writer.WriteEndElement();
                    //writer.WriteEndDocument();
                }
                string newModelXml = sm.ToString();
                int newModelID = db.addModel(new_contract_id, model_tags[i], newModelXml, model_descriptions[i]);

                StringBuilder sb = new StringBuilder();
                using (XmlWriter writer = XmlWriter.Create(sb, settings))
                {
                    //writer.WriteStartDocument();
                    writer.WriteStartElement("Config");
                    writer.WriteStartElement("Common");
                        writer.WriteElementString("TimeZone", common["TimeZone"].InnerText);
                        writer.WriteElementString("StartTime", common["StartTime"].InnerText);
                        writer.WriteElementString("EndTime", common["EndTime"].InnerText);
                        writer.WriteElementString("Calendar", common["Calendar"].InnerText);
                    writer.WriteEndElement();
                    writer.WriteElementString("Model", newModelID.ToString());
                    writer.WriteElementString("Quantity", xn1["Quantity"].InnerText);
                    writer.WriteElementString("EntryPreOffset", xn1["EntryPreOffset"].InnerText);
                    writer.WriteElementString("EntryPostOffset", xn1["EntryPostOffset"].InnerText);
                    writer.WriteElementString("ExitPreOffset", xn1["ExitPreOffset"].InnerText);
                    writer.WriteElementString("ExitPostOffset", xn1["ExitPostOffset"].InnerText);
                    writer.WriteStartElement("BlockOrder");
                        writer.WriteElementString("UnitSize", blockOrder["UnitSize"].InnerText);
                        writer.WriteElementString("MaxBlockSize", blockOrder["MaxBlockSize"].InnerText);
                        writer.WriteElementString("MaxNumberOfOrders", blockOrder["MaxNumberOfOrders"].InnerText);
                        writer.WriteStartElement("StopOrderTemplate");
                            writer.WriteElementString("OrderType", stopOrderTemplate["OrderType"].InnerText);
                        writer.WriteEndElement();
                    writer.WriteEndElement();
                    writer.WriteEndElement();
                    //writer.WriteEndDocument();
                }
                string newStrategyXml = sb.ToString();
                db.addStrategy(strategyClass, strategy_tags[i], newStrategyXml, strategy_descriptions[i]);
            }
        }
        public static void rollFixedReversionStrategy(int strategy_template, int new_contract_id, string new_maturity_tag, string new_web_ticker, string suffix = "")
        {
            // new_web_ticker is for barchart.com, for example the ticker for Jan17 contract is 'CBF17'
            string model_tag = "BT_Reversion_" + new_maturity_tag;
            string model_description = "Brent Reversion" + new_maturity_tag;
            string strategy_tag = "BT_Reversion_" + new_maturity_tag;
            string strategy_description = "Brent Reversion" + new_maturity_tag;
            if (suffix != "")
            {
                strategy_tag += "_" + suffix;
                strategy_description += " " + suffix;
            }

            List<string> infos = db.getStrategyInfo(strategy_template);
            string strategyClass = infos[0];
            XmlDocument xml = new XmlDocument();
            xml.LoadXml(infos[1]);
            XmlNode xn1 = xml.SelectSingleNode("/Config");
            XmlNode common = xn1.SelectSingleNode("Common");
            XmlNode blockOrder = xn1.SelectSingleNode("BlockOrder");
            int template_model_id = int.Parse(xn1["Model"].InnerText);
            XmlDocument modelXml = new XmlDocument();
            modelXml.LoadXml(db.getModel(template_model_id));
            XmlNode mn1 = modelXml.SelectSingleNode("/Parameters");
            XmlNode settlementPriceHtml = mn1.SelectSingleNode("SettlementPriceHtml");

            XmlWriterSettings settings = new XmlWriterSettings();
            settings.Encoding = Encoding.UTF8;
            settings.OmitXmlDeclaration = true;
            //Write Model Xml
            StringBuilder sm = new StringBuilder();
            using (XmlWriter writer = XmlWriter.Create(sm, settings))
            {
                //writer.WriteStartDocument();
                writer.WriteStartElement("Parameters");
                writer.WriteElementString("Asset", new_contract_id.ToString());
                writer.WriteElementString("EntryStartTime", mn1["EntryStartTime"].InnerText);
                writer.WriteElementString("EntryEndTime", mn1["EntryEndTime"].InnerText);
                writer.WriteElementString("ExitStartTime", mn1["ExitStartTime"].InnerText);
                writer.WriteElementString("ExitEndTime", mn1["ExitEndTime"].InnerText);
                writer.WriteElementString("Expiry", mn1["Expiry"].InnerText);
                writer.WriteElementString("UseClose", mn1["UseClose"].InnerText);
                writer.WriteElementString("RefCloseTime", mn1["RefCloseTime"].InnerText);
                writer.WriteElementString("TrendOrReversion", mn1["TrendOrReversion"].InnerText);
                writer.WriteElementString("Side", mn1["Side"].InnerText);
                writer.WriteElementString("EntryOffset", mn1["EntryOffset"].InnerText);
                writer.WriteElementString("ProfitTarget", mn1["ProfitTarget"].InnerText);
                writer.WriteElementString("StopTarget", mn1["StopTarget"].InnerText);
                writer.WriteStartElement("SettlementPriceHtml");
                    writer.WriteElementString("Url", settlementPriceHtml["Url"].InnerText);
                    string[] old_xpath = settlementPriceHtml["XPathLast"].InnerText.Split('_');
                    string new_xpath = old_xpath[0] + "_" + new_web_ticker + "_" + old_xpath[2];
                    writer.WriteElementString("XPathLast", new_xpath);
                    writer.WriteElementString("XPathDate", settlementPriceHtml["XPathDate"].InnerText);
                writer.WriteEndElement();
                writer.WriteEndElement();
                //writer.WriteEndDocument();
            }
            string newModelXml = sm.ToString();
            int newModelID = db.addModel(new_contract_id, model_tag, newModelXml, model_description);

            StringBuilder sb = new StringBuilder();
            using (XmlWriter writer = XmlWriter.Create(sb, settings))
            {
                //writer.WriteStartDocument();
                writer.WriteStartElement("Config");
                writer.WriteStartElement("Common");
                    writer.WriteElementString("TimeZone", common["TimeZone"].InnerText);
                    writer.WriteElementString("StartTime", common["StartTime"].InnerText);
                    writer.WriteElementString("EndTime", common["EndTime"].InnerText);
                    writer.WriteElementString("Calendar", common["Calendar"].InnerText);
                writer.WriteEndElement();
                writer.WriteElementString("Model", newModelID.ToString());
                writer.WriteElementString("Quantity", xn1["Quantity"].InnerText);
                writer.WriteStartElement("BlockOrder");
                    writer.WriteElementString("UnitSize", blockOrder["UnitSize"].InnerText);
                    writer.WriteElementString("MaxBlockSize", blockOrder["MaxBlockSize"].InnerText);
                    writer.WriteElementString("MaxNumberOfOrders", blockOrder["MaxNumberOfOrders"].InnerText);
                writer.WriteEndElement();
                writer.WriteEndElement();
                //writer.WriteEndDocument();
            }
            string newStrategyXml = sb.ToString();
            db.addStrategy(strategyClass, strategy_tag, newStrategyXml, strategy_description);
        }
        public static void createEIAEvents()
        {
            Dictionary<DateTime, DateTime> mappings = new Dictionary<DateTime, DateTime>();
            mappings.Add(new DateTime(2015, 10, 14, 10, 30, 0), new DateTime(2015, 10, 15, 11, 0, 0));  //Columbus
            mappings.Add(new DateTime(2015, 11, 11, 10, 30, 0), new DateTime(2015, 10, 15, 11, 0, 0));  //Veterans
            mappings.Add(new DateTime(2016, 1, 20, 10, 30, 0), new DateTime(2016, 1, 21, 11, 0, 0));    //Martin Luther King Jr.
            mappings.Add(new DateTime(2016, 2, 17, 10, 30, 0), new DateTime(2016, 2, 18, 11, 0, 0));    //Presidents
            mappings.Add(new DateTime(2016, 6, 1, 10, 30, 0), new DateTime(2016, 6, 2, 11, 0, 0));      //Memorial
            mappings.Add(new DateTime(2016, 7, 6, 10, 30, 0), new DateTime(2016, 7, 7, 11, 0, 0));      //Independence
            mappings.Add(new DateTime(2016, 9, 7, 10, 30, 0), new DateTime(2016, 9, 8, 11, 0, 0));      //Labor
            mappings.Add(new DateTime(2016, 10, 12, 10, 30, 0), new DateTime(2016, 10, 13, 11, 0, 0));  //Columbus
            mappings.Add(new DateTime(2016, 12, 28, 10, 30, 0), new DateTime(2016, 12, 29, 11, 0, 0));  //Christmas
            mappings.Add(new DateTime(2017, 1, 4, 10, 30, 0), new DateTime(2017, 1, 5, 11, 0, 0));      //New Year's
            mappings.Add(new DateTime(2017, 1, 18, 10, 30, 0), new DateTime(2017, 1, 19, 11, 0, 0));      //Martin Luther king
            mappings.Add(new DateTime(2017, 2, 22, 10, 30, 0), new DateTime(2017, 2, 23, 11, 0, 0));      //President's
            mappings.Add(new DateTime(2017, 5, 31, 10, 30, 0), new DateTime(2017, 6, 1, 11, 0, 0));      //Memorial
            mappings.Add(new DateTime(2017, 7, 5, 10, 30, 0), new DateTime(2017, 7, 6, 11, 0, 0));      //Independence
            mappings.Add(new DateTime(2017, 9, 6, 10, 30, 0), new DateTime(2017, 9, 7, 11, 0, 0));      //Labor
            mappings.Add(new DateTime(2017, 10, 11, 10, 30, 0), new DateTime(2017, 10, 12, 11, 0, 0));      //Columbus
            mappings.Add(new DateTime(2017, 12, 27, 10, 30, 0), new DateTime(2017, 12, 28, 11, 0, 0));      //Christmas
            mappings.Add(new DateTime(2018, 1, 3, 10, 30, 0), new DateTime(2018, 1, 4, 11, 0, 0));      //New Year's

            DateTime startDate = new DateTime(2015, 10, 14);
            DateTime endDate = new DateTime(2018, 1, 5);
            DBAccess db = DBAccess.instance;
            db.runNonQuery("delete from events where name='EIA Report'");
            List<DateTime> dates = new List<DateTime>();
            DateTime d = startDate.Date;
            while (d.DayOfWeek != DayOfWeek.Wednesday)
                d = d.AddDays(1);
            d = d.AddHours(10);
            d = d.AddMinutes(30);
            while (d.Date <= endDate)
            {
                dates.Add(mappings.ContainsKey(d) ? mappings[d] : d);
                d = d.AddDays(7);
            }
            foreach (DateTime dt in dates)
                db.runNonQuery("insert into events values ('EIA Report', " + db.getSqlDateTime(dt) + ",'')");
        }
        public static void generatePnLReport(string account, DateTime startDate, DateTime endDate, string reportFolder = null)
        {
            Report r = new Report(account, startDate, endDate);
            string summary = r.reportSummary();
            Console.Write(summary);
            if (reportFolder != null)
            {
                string path = Path.Combine(reportFolder, account);
                if (!Directory.Exists(path))
                    Directory.CreateDirectory(path);
                string detailedFilename = Path.Combine(path, "Details.csv");
                File.WriteAllText(detailedFilename, r.reportTradeDetails());
                string summaryFilename = Path.Combine(path, "Summary.txt");
                File.WriteAllText(summaryFilename, summary);
            }
            Console.WriteLine("Reports are ready!");
        }
    }
}
