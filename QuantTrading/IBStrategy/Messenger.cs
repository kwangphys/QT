using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading;

namespace IBStrategy
{
    public class Messenger
    {
        //Singleton interface
        private static Messenger _instance;
        private readonly Object _lock_email = new Object();
        private readonly Object _lock_msg = new Object();
        private readonly Object _lock_error = new Object();
        private DBAccess.DBAccess _db;
        private SmtpClient _mailClient;
        private string _address;
        private Queue<MailMessage> _emails;
        private Queue<string> _messages;
        private Queue<string> _errors;
        public Messenger()
        {
            _db = DBAccess.DBAccess.instance;
            lock (_lock_msg)
            {
                _messages = new Queue<string>();
            }
            Thread tm = new Thread(new ThreadStart(runMessageAgent));
            tm.CurrentCulture = CultureInfo.InvariantCulture;
            tm.CurrentUICulture = CultureInfo.InvariantCulture;
            tm.Start();
            lock (_lock_error)
            {
                _errors = new Queue<string>();
            }
            Thread te = new Thread(new ThreadStart(runErrorAgent));
            te.CurrentCulture = CultureInfo.InvariantCulture;
            te.CurrentUICulture = CultureInfo.InvariantCulture;
            te.Start();
        }
        public void setupEmail(string smtp, int port, string address, string password, bool enableSsl)
        {
            lock (_lock_email)
            {
                _mailClient = new SmtpClient(smtp);
                _mailClient.Port = port;
                _address = address;
                _mailClient.Credentials = new System.Net.NetworkCredential(_address, password);
                _mailClient.EnableSsl = enableSsl;
                _emails = new Queue<MailMessage>();
            }
            Thread t = new Thread(new ThreadStart(runEmailAgent));
            t.CurrentCulture = CultureInfo.InvariantCulture;
            t.CurrentUICulture = CultureInfo.InvariantCulture;
            t.Start();
        }
        public static Messenger instance
        {
            get
            {
                if (_instance == null)
                    _instance = new Messenger();
                return _instance;
            }
        }
        public void sendEmail(string subject, string body)
        {
            if (_mailClient != null)
            {
                lock (_lock_email)
                {
                    _emails.Enqueue(new MailMessage(_address, _address, subject, body));
                }
            }
        }
        void runEmailAgent()
        {
            while (true)
            {
                while (_emails.Count == 0)
                    Thread.Sleep(100);
                try
                {
                    lock (_lock_email)
                    {
                        _mailClient.Send(_emails.Dequeue());
                    }
                }
                catch (Exception e)
                {
                    logError(DateTime.Now, -3, "Email", 0, 0, e);
                }
            }
        }
        public void logMessage(DateTime timestamp, int code, string level, string type, int contract_id, int strategy_id, string message)
        {
            lock (_lock_msg)
            {
                string con = contract_id == 0 ? "null" : contract_id.ToString();
                string stg = strategy_id == 0 ? "null" : strategy_id.ToString();
                _messages.Enqueue("insert into system_log values (" + _db.getSqlDateTime(timestamp) + "," + code.ToString() + "," + _db.getSqlString(level) + "," + _db.getSqlString(type) + "," + con + "," + stg + "," + _db.getSqlString(message) + ")");
            }
        }
        void runMessageAgent()
        {
            while (true)
            {
                while (_messages.Count == 0)
                    Thread.Sleep(100);
                lock (_lock_msg)
                {
                    _db.runNonQuery(_messages.Dequeue());
                }
                Thread.Sleep(100);
            }
        }
        public void logError(DateTime timestamp, int code, string type, int contract_id, int strategy_id, string message, string stack)
        {
            lock (_lock_error)
            {
                string con = contract_id == 0 ? "null" : contract_id.ToString();
                string stg = strategy_id == 0 ? "null" : strategy_id.ToString();
                _errors.Enqueue("insert into errors values (" + _db.getSqlDateTime(timestamp) + "," + code.ToString() + "," + _db.getSqlString(type) + "," + con + "," + stg + "," + _db.getSqlString(message) + "," + _db.getSqlString(stack) + ")");
            }
        }
        public void logError(DateTime timestamp, int code, string type, int contract_id, int strategy_id, Exception e)
        {
            string errorMsg = e.Message;
            string stackTrace = e.StackTrace;
            while (e.InnerException != null)
            {
                e = e.InnerException;
                errorMsg += "\r\n" + e.Message;
                stackTrace += "\r\n" + e.StackTrace;
            }
            logError(timestamp, code, type, contract_id, strategy_id, errorMsg, stackTrace);
        }
        void runErrorAgent()
        {
            while (true)
            {
                while (_errors.Count == 0)
                    Thread.Sleep(100);
                lock (_lock_error)
                {
                    _db.runNonQuery(_errors.Dequeue());
                }
                Thread.Sleep(100);
            }
        }
    }
}
