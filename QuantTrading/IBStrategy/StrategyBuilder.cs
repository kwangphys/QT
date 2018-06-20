using System;
using System.Collections.Generic;

namespace IBStrategy
{
    class StrategyBuilder
    {
        public static Strategy buildStrategy(int strategy_id, IBApi.EClientInterface socket)
        {
            List<string> strategy_info = DBAccess.DBAccess.instance.getStrategyInfo(strategy_id);
            switch(strategy_info[0])
            {
                case "MarketDataDownloader": return new MarketDataDownloader( strategy_id, strategy_info[1], socket );
                case "Fixed": return new Fixed(strategy_id, strategy_info[1], socket);
                case "ExecutionTest": return new ExecutionTest(strategy_id, strategy_info[1], socket);
                default: throw new Exception("Unknown strategy class: " + strategy_info[0]);
            }
        }
    }
}
