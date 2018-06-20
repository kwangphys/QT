using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using IBApi;

namespace IBStrategy
{
    public class OrderFactory
    {
        public static List<Order> createBracketOrder(Order parent, double profitTarget, double stopTarget, DateTime expiry, OrderTemplate profitTemplate = null, OrderTemplate stopTemplate = null, OrderTemplate expiryTemplate = null)
        {
            List<Order> orders = new List<Order>();
            parent.Transmit = false;
            orders.Add(parent);
            if (expiry != DateTime.MinValue)
            {
                Order expiryOrder;
                if (expiryTemplate == null)
                {
                    expiryOrder = new Order();
                    expiryOrder.OrderType = "MKT";
                }
                else
                {
                    expiryOrder = expiryTemplate.clone();
                    if (!expiryTemplate.isSet("OrderType"))
                        expiryOrder.OrderType = "MKT";
                }
                expiryOrder.GoodAfterTime = expiry.ToString("yyyyMMdd HH:mm:ss");
                expiryOrder.Action = parent.Action.Equals("BUY") ? "SELL" : "BUY";
                expiryOrder.TotalQuantity = parent.TotalQuantity;
                //expiryOrder.OrderId = _og.get();
                expiryOrder.ParentId = parent.OrderId;
                expiryOrder.Transmit = false;
                orders.Add(expiryOrder);
            }
            if (profitTarget != 0)
            {
                Order profitOrder;
                if (profitTemplate == null)
                {
                    profitOrder = new Order();
                    profitOrder.OrderType = "LMT";
                }
                else
                {
                    profitOrder = profitTemplate.clone();
                    if (!profitTemplate.isSet("OrderType"))
                        profitOrder.OrderType = "LMT";
                }
                profitOrder.Action = parent.Action.Equals("BUY") ? "SELL" : "BUY";
                profitOrder.TotalQuantity = parent.TotalQuantity;
                profitOrder.LmtPrice = profitTarget;
                //profitOrder.OrderId = _og.get();
                profitOrder.ParentId = parent.OrderId;
                profitOrder.Transmit = false;
                orders.Add(profitOrder);
            }
            if (stopTarget != 0)
            {
                Order stopOrder;
                if (stopTemplate == null)
                {
                    stopOrder = new Order();
                    stopOrder.OrderType = "STP";
                }
                else
                {
                    stopOrder = stopTemplate.clone();
                    if (!stopTemplate.isSet("OrderType"))
                        stopOrder.OrderType = "STP";
                    if (stopOrder.OrderType == "STP LMT" && !stopTemplate.isSet("LmtPriceOffset"))
                        stopOrder.LmtPriceOffset = stopTarget * 0.1; // assume offset is 10% of stop price if not set
                }
                stopOrder.Action = parent.Action.Equals("BUY") ? "SELL" : "BUY";
                stopOrder.TotalQuantity = parent.TotalQuantity;
                stopOrder.AuxPrice = stopTarget;
                if (stopOrder.OrderType == "STP LMT")
                    stopOrder.LmtPrice = stopTarget + stopOrder.LmtPriceOffset * (stopOrder.Action.Equals("BUY") ? 1 : -1);
                //stopOrder.OrderId = _og.get();
                stopOrder.ParentId = parent.OrderId;
                stopOrder.Transmit = false;
                orders.Add(stopOrder);
            }
            orders[orders.Count() - 1].Transmit = true;
            return orders;
        }
    }
}
