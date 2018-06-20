using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using DBAccess;
using IBApi;

namespace IBStrategy
{
    public class Utilities
    {
        public static void addUSLiquidETFs(EWrapperImpl ibClient)
        {
            string[] names = new string[] {
                "IWM",
                "IWN",
                "IWO",
                "IWF",
                "IWD",
                "IWB",
                "IWV",
                "IUSV",
                "IUSG",
                "IWS",
                "IWC",
                "IWR",
                "IWP",

                "SPY",
                //"UWTI",
                "VXX",
                "UGAZ",
                "EEM",
                "XLF",
                "EWJ",
                "NUGT",
                "QQQ",
                "FXI",
                "USO",
                "EFA",
                "XIV",
                "EWZ",
                "TZA",
                "XLE",
                "UVXY",
                "XLU",
                "VWO",
                "DGAZ",
                "RSX",
                "TLT",
                "XOP",
                "SDS",
                "IYR",
                "XLI",
                "XLK",
                "JNK",
                "EWT",
                "XLV",
                "UCO",
                "XLP",
                "EZU",
                "OIH",
                "TVIX",
                "HYG",
                "SQQQ",
                "SLV",
                "EWG",
                "UNG",
                "DUST",
                "AMLP",
                "XLY",
                "EPI",
                "VGK",
                "DIA",
                "VEA",
                "TNA",
                "SPXU",
                "KRE",
                "XLB",
                "JDST",
                "DBEF",
                "VNQ",
                "SMH",
                "SSO",
                "JNUG",
                "OIL",
                "QID",
                "IVV",
                "SH",
                "TBT",
                "FAS",
                "EWH",
                "LQD",
                "ITB",
                "XHB",
                "TQQQ",
                "IAU",
                "FAZ",
                "EWU",
                "UUP",
                "FEZ",
                "IEMG",
                "UPRO",
                "VTI",
                "XME",
                "EWY",
                "BKLN",
                "EWI",
                "INDA",
                "EWW",
                "BND",
                "XRT",
                "EWA",
                "PFF",
                "EWC",
                "AGG",
                "VEU",
                //"DWTI",
                "AMJ",
                "DBC"
            };
            foreach (string symbol in names)
            {
                Contract contract = new Contract();
                contract.Symbol = symbol;
                contract.SecType = "STK";
                contract.Currency = "USD";
                contract.Exchange = "SMART";
                int reqId = TickerGenerator.instance.get();
                ibClient.IBClientSocket.reqContractDetails(reqId, contract);
                while (!ibClient.contractDetailsMap.ContainsKey(reqId)) { Thread.Sleep(100); }
                ContractDetails details = ibClient.contractDetailsMap[reqId];
                if (DBAccess.DBAccess.instance.setContract(details, "US", new TimeSpan(9, 30, 0), new TimeSpan(16, 0, 0), true))
                    Console.WriteLine("Added " + symbol);
            }
            Console.WriteLine("US Liquid ETFs all loaded!");
        }
        public static void addUSHighVolStocks(EWrapperImpl ibClient)
        {
            //These names are of top intraday volatilities, 
            //with daily volume > 2M and price in [20,200] 
            string[] names = new string[] {
                "IONS",
                "MNK",
                "X",
                "ACAD",
                "CLVS",
                "CRC",
                "CC",
                "FRAC",
                "NVDA",
                "HTZ",
                "JUNO",
                "ATI",
                "SM",
                //"TCK",
                "SLW",
                "MOMO",
                "KSS",
                "NEM",
                "ALXN",
                "FNSR",
                "TECK",
                "GPOR",
                "FSLR",
                "AEM",
                "JBLU",
                "PBF",
                "M",
                "RICE",
                "GPS",
                "MU",
                "RRC",
                "AA",
                "STX",
                "HFC",
                "JWN",
                "HES",
                "MUR",
                "MBLY",
                "URBN",
                "CF",
                "XIV",
                "STLD",
                "QCOM",
                "LW",
                "PK",
                "PTEN",
                "CMC",
                "VRTX",
                "YELP",
                "MAT",
                "CLR",
                "MYL",
                "COG",
                "WDC",
                "TEVA",
                "NBL",
                "BAC",
                "CELG",
                "GME",
                "MPC",
                "YUMC",
                "AOBC",
                "BMY",
                "SWFT",
                "DKS",
                "DVN",
                "NFLX",
                "PE",
                "ETP",
                "SQQQ",
                "MOS",
                "WYNN",
                "BBBY",
                "CXW",
                "NFX",
                "KORS",
                "NUE",
                "AR",
                "TSO",
                "BBY",
                "UAL",
                "WMB",
                "UA",
                "AAL",
                "SWKS",
                "LVS",
                "JD",
                "ARNC",
                "TRIP",
                "MCK",
                "BX",
                "KMI",
                "DAL",
                "UAA",
                "DISH",
                "AET",
                "PAA",
                "ABC",
                "VLO",
                "CIEN",
                "SKX",
                "TRGP",
                "NOV",
                "APC",
                "GM",
                "APA",
                //"AWAY",
                "MS",
                "HPE",
                "PHM",
                "FITB",
                "C",
                "DLTR",
                "LUV",
                "KSU",
                "COP",
                "LB",
                "SUM",
                "CFG",
                "LNG",
                "ESRX",
                "WNR",
                "BHI",
                "ZAYO",
                "SXL",
                "DISCA",
                "MGM",
                "ZION",
                "EVHC",
                "KMX",
                "FTNT",
                "DHI",
                "JBL",
                "YNDX",
                "EOG",
                "LULU",
                "HBI",
                "XLNX",
                "TMUS",
                "OLN",
                "BABA",
                "MNST",
                "BIDU",
                "GGP",
                "HUN",
                "DB",
            };
            foreach (string symbol in names)
            {
                Contract contract = new Contract();
                contract.Symbol = symbol;
                contract.SecType = "STK";
                contract.Currency = "USD";
                contract.Exchange = "SMART";
                int reqId = TickerGenerator.instance.get();
                ibClient.IBClientSocket.reqContractDetails(reqId, contract);
                while (!ibClient.contractDetailsMap.ContainsKey(reqId)) { Thread.Sleep(100); }
                ContractDetails details = ibClient.contractDetailsMap[reqId];
                if (DBAccess.DBAccess.instance.setContract(details, "US", new TimeSpan(9, 30, 0), new TimeSpan(16, 0, 0), true))
                    Console.WriteLine("Added " + symbol);
            }
            Console.WriteLine("US High Vol stocks all loaded!");
        }
        public static void addOilFutures(EWrapperImpl ibClient)
        {
            string[] names = new string[] {
                "COIL"
            };
            string[] expiries = new string[]{
//                "201412",
//                "201501",
//                "201502",
                "201503",
                "201504",
                "201505",
                "201506",
                "201507",
                "201508",
                "201509",
                "201510",
                "201511",
                "201512",
                "201601",
                "201602",
                "201603",
                "201604",
                "201605",
                "201606",
                "201607",
                "201608",
                "201609",
                "201610",
                "201611",
                "201612",
                "201701",
                "201702",
                "201703",
                "201704",
                "201705",
                "201706",
                "201707",
                "201708",
                "201709"
            };
            foreach (string symbol in names)
                foreach (string expiry in expiries)
                {
                    Contract contract = new Contract();
                    contract.Symbol = symbol;
                    contract.SecType = "FUT";
                    contract.Currency = "USD";
                    contract.Exchange = "IPE";
                    contract.LastTradeDateOrContractMonth = expiry;
                    contract.IncludeExpired = true;
                    int reqId = TickerGenerator.instance.get();
                    ibClient.IBClientSocket.reqContractDetails(reqId, contract);
                    while (!ibClient.contractDetailsMap.ContainsKey(reqId)) { Thread.Sleep(100); }
                    ContractDetails details = ibClient.contractDetailsMap[reqId];
                    if (DBAccess.DBAccess.instance.setContract(details, "ICE", new TimeSpan(1, 0, 0), new TimeSpan(23, 0, 0), true))
                        Console.WriteLine("Added " + symbol);
                }
        }
        public static void addNYMEXFutures(EWrapperImpl ibClient)
        {
            string[] names = new string[] {
                "CL",
                "RB",
                "HO",
                "NG"
            };
            string[] expiries = new string[]{
                "201503",
                "201504",
                "201505",
                "201506",
                "201507",
                "201508",
                "201509",
                "201510",
                "201511",
                "201512",
                "201601",
                "201602",
                "201603",
                "201604",
                "201605",
                "201606",
                "201607",
                "201608",
                "201609",
                "201610",
                "201611",
                "201612",
                "201701",
                "201702",
                "201703",
                "201704",
                "201705",
                "201706",
                "201707",
                "201708",
                "201709"
            };
            foreach (string symbol in names)
                foreach (string expiry in expiries)
                {
                    Contract contract = new Contract();
                    contract.Symbol = symbol;
                    contract.SecType = "FUT";
                    contract.Currency = "USD";
                    contract.Exchange = "NYMEX";
                    contract.LastTradeDateOrContractMonth = expiry;
                    contract.IncludeExpired = true;
                    int reqId = TickerGenerator.instance.get();
                    ibClient.IBClientSocket.reqContractDetails(reqId, contract);
                    while (!ibClient.contractDetailsMap.ContainsKey(reqId)) { Thread.Sleep(100); }
                    ContractDetails details = ibClient.contractDetailsMap[reqId];
                    if (DBAccess.DBAccess.instance.setContract(details, "CME", new TimeSpan(18, 0, 0), new TimeSpan(17, 0, 0), true)) // EST
                        Console.WriteLine("Added " + symbol);
                }
        }
        public static void addSGXFutures(EWrapperImpl ibClient)
        {
            string[] expiries = new string[]{
                "201703",
                "201702",
                "201701",
                "201612",
                "201611",
                "201610",
                "201609",
                "201608",
                "201607",
                "201606",
                "201605",
                "201604",
                "201603",
                "201602",
                "201601",
                "201512"
            };
            string[] expiries_nk = new string[]
            {
                "201703",
                "201612",
                "201609",
                "201606",
                "201603",
                "201512"
            };
            Tuple<string, string, TimeSpan, TimeSpan, string[]>[] symbols = new Tuple<string, string, TimeSpan, TimeSpan, string[]>[] {
                new Tuple<string, string, TimeSpan, TimeSpan, string[]>("STW", "USD", new TimeSpan(8,30,0), new TimeSpan(13,45,0), expiries),
                new Tuple<string, string, TimeSpan, TimeSpan, string[]>("SSG", "SGD", new TimeSpan(8,30,0), new TimeSpan(17,10,0), expiries),
                new Tuple<string, string, TimeSpan, TimeSpan, string[]>("XINA50", "USD", new TimeSpan(9,0,0), new TimeSpan(16,30,0), expiries),
                new Tuple<string, string, TimeSpan, TimeSpan, string[]>("NIFTY", "USD", new TimeSpan(9,0,0), new TimeSpan(18,15,0), expiries),
                new Tuple<string, string, TimeSpan, TimeSpan, string[]>("SGXNK", "JPY", new TimeSpan(7,30,0), new TimeSpan(14,30,0), expiries_nk),
            };
            foreach (var info in symbols)
            {
                foreach (string expiry in info.Item5)
                {
                    Contract stw = new Contract();
                    stw.Symbol = info.Item1;
                    stw.SecType = "FUT";
                    stw.Currency = info.Item2;
                    stw.Exchange = "SGX";
                    stw.LastTradeDateOrContractMonth = expiry;
                    stw.IncludeExpired = true;
                    int reqId = TickerGenerator.instance.get();
                    ibClient.IBClientSocket.reqContractDetails(reqId, stw);
                    while (!ibClient.contractDetailsMap.ContainsKey(reqId)) { Thread.Sleep(100); }
                    ContractDetails details = ibClient.contractDetailsMap[reqId];
                    if (DBAccess.DBAccess.instance.setContract(details, "SGX", info.Item3, info.Item4, true))
                        Console.WriteLine("Added " + stw.Symbol + " " + expiry);
                }
            }
        }

        public static void addPreciousMetalSpots(EWrapperImpl ibClient)
        {
            string[] names = new string[] {
                "XAUUSD",
                "XAGUSD",
                "XPTUSD"
            };
            foreach (string symbol in names)
            {
                Contract contract = new Contract();
                contract.Symbol = symbol;
                contract.SecType = "CMDTY";
                contract.Currency = "USD";
                
                int reqId = TickerGenerator.instance.get();
                ibClient.IBClientSocket.reqContractDetails(reqId, contract);
                while (!ibClient.contractDetailsMap.ContainsKey(reqId)) { Thread.Sleep(100); }
                ContractDetails details = ibClient.contractDetailsMap[reqId];
                if (DBAccess.DBAccess.instance.setContract(details, "US", new TimeSpan(9, 30, 0), new TimeSpan(16, 0, 0), true))
                    Console.WriteLine("Added " + symbol);
            }
        }

        public static void addFXSpots(EWrapperImpl ibClient)
        {
            string[][] names = new string[][] {
                new string[]{ "NZD", "USD" },
                new string[]{ "AUD", "USD" },
                new string[]{ "GBP", "USD" },
                new string[]{ "USD", "JPY" },
                new string[]{ "EUR", "USD" },
                new string[]{ "USD", "CAD" },
                new string[]{ "USD", "CHF" }
            };
            foreach (string[] symbol in names)
            {
                Contract contract = new Contract();
                contract.Symbol = symbol[0];
                contract.SecType = "CASH";
                contract.Currency = symbol[1];

                int reqId = TickerGenerator.instance.get();
                ibClient.IBClientSocket.reqContractDetails(reqId, contract);
                while (!ibClient.contractDetailsMap.ContainsKey(reqId)) { Thread.Sleep(100); }
                ContractDetails details = ibClient.contractDetailsMap[reqId];
                if (DBAccess.DBAccess.instance.setContract(details, "CME", new TimeSpan(17, 15, 0), new TimeSpan(17, 0, 0), true))
                    Console.WriteLine("Added " + symbol);
            }
        }
    }
}
