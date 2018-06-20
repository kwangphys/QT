using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace IBApi
{
    public interface EClientInterface
    {
        //Connection and Server
        void eDisconnect();
        void setServerLogLevel(int logLevel);
        void reqCurrentTime();
        //Market Data
        void reqMktData(int tickerId, Contract contract, string genericTickList, bool snapshot, List<TagValue> mktDataOptions);
        void cancelMktData(int tickerId);
        void calculateImpliedVolatility(int reqId, Contract contract, double optionPrice, double underPrice, List<TagValue> impliedVolatilityOptions);
        void cancelCalculateImpliedVolatility(int reqId);
        void calculateOptionPrice(int reqId, Contract contract, double volatility, double underPrice, List<TagValue> optionPriceOptions);
        void cancelCalculateOptionPrice(int reqId);
        void reqMarketDataType(int marketDataType);
        //Orders
        void placeOrder(int id, Contract contract, Order order);
        void cancelOrder(int orderId);
        void reqOpenOrders();
        void reqAllOpenOrders();
        void reqAutoOpenOrders(bool autoBind);
        void reqIds(int numIds);
        void exerciseOptions(int tickerId, Contract contract, int exerciseAction, int exerciseQuantity, string account, int ovrd);
        void reqGlobalCancel();
        //Account and Portfolio
        void reqAccountUpdates(bool subscribe, string acctCode);
        void reqAccountSummary(int reqId, string group, string tags);
        void cancelAccountSummary(int reqId);
        void reqAccountUpdatesMulti(int requestId, string account, string modelCode, bool ledgerAndNLV);
        void cancelAccountUpdatesMulti(int requestId);
        void reqPositions();
        void cancelPositions();
        void reqPositionsMulti(int requestId, string account, string modelCode);
        void cancelPositionsMulti(int requestId);
        //Executions
        void reqExecutions(int reqId, ExecutionFilter filter);
        //Contract Details
        void reqContractDetails(int reqId, Contract contract);
        //Market Depth
        void reqMarketDepth(int tickerId, Contract contract, int numRows, List<TagValue> mktDepthOptions);
        void cancelMktDepth(int tickerId);
        //News Bulletins
        void reqNewsBulletins(bool allMessages);
        void cancelNewsBulletin();
        //Financial Advisors
        void reqManagedAccts();
        void requestFA(int faDataType);
        void replaceFA(int faDataType, string xml);
        //Market Scanners
        void reqScannerParameters();
        void reqScannerSubscription(int reqId, ScannerSubscription subscription, List<TagValue> scannerSubscriptionOptions);
        void cancelScannerSubscription(int tickerId);
        //Historical Data
        void reqHistoricalData(int tickerId, Contract contract, string endDateTime,
            string durationString, string barSizeSetting, string whatToShow, int useRTH, int formatDate, List<TagValue> chartOptions);
        void cancelHistoricalData(int reqId);
        //Real Time Bars
        void reqRealTimeBars(int tickerId, Contract contract, int barSize, string whatToShow, bool useRTH, List<TagValue> realTimeBarsOptions);
        void cancelRealTimeBars(int tickerId);
        //Fundamental Data
        void reqFundamentalData(int reqId, Contract contract, String reportType, List<TagValue> fundamentalDataOptions);
        void cancelFundamentalData(int reqId);
        //Display Groups
        void queryDisplayGroups(int requestId);
        void subscribeToGroupEvents(int requestId, int groupId);
        void updateDisplayGroup(int requestId, string contractInfo);
        void unsubscribeFromGroupEvents(int requestId);
        //Other
        void reqSecDefOptParams(int reqId, string underlyingSymbol, string futFopExchange, string underlyingSecType, int underlyingConId);
        //User Defined
        void subscribeTimerEvent(TimeSpan t, onTime handler);
        DateTime getCurrentLocalTime();
        void sleepUntil(DateTime expiry, ManualResetEvent blockEvent);
    }
}
