using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using IBApi;

namespace IBStrategy
{
    public class OrderTemplate
    {
        private XmlNode _node;
        private Order _template;
        private HashSet<string> _fields;
        
        public Order Template
        {
            get { return _template; }
        }
        public OrderTemplate(XmlNode xml = null)
        {
            _node = xml;
            _template = new Order();
            _fields = new HashSet<string>();
            if (xml != null)
            {
                _template.OrderType = xml["OrderType"].InnerText;
                _fields.Add("OrderType");
                if (xml["LimitPriceOffset"] != null)
                {
                    _template.LmtPriceOffset = Double.Parse(xml["LimitPriceOffset"].InnerText);
                    _fields.Add("LmtPriceOffset");
                }
                XmlElement algo = xml["IBAlgo"];
                if (algo != null)
                {
                    List<TagValue> ps = new List<TagValue>();
                    XmlNodeList nodes = algo.GetElementsByTagName("*");
                    foreach (XmlNode node in nodes)
                    {
                        if (node.Name == "Strategy")
                        {
                            _template.AlgoStrategy = node.InnerText;
                            _fields.Add("AlgoStrategy");
                        }
                        else
                        {
                            ps.Add(new TagValue(node.Name, node.InnerText));
                        }
                    }
                    _template.AlgoParams = ps;
                    _fields.Add("AlgoParams");
                }
            }
        }
        public Order clone()
        {
            Order o = new Order();
            Type otype = o.GetType();
            foreach (string field in _fields)
                otype.GetProperty(field).SetValue(o, otype.GetProperty(field).GetValue(_template, null), null);
            return o;
        }
        public bool isSet(string field)
        {
            return _fields.Contains(field);
        }
        public XmlNode getNode()
        {
            return _node;
        }
    }
}
