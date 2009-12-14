/*
 * User: scorder
 * Date: 7/7/2009
 */
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;


namespace MongoDB.Driver
{
    public class Mongo : IDisposable
    {
        private Connection connection;
        private bool _hasPrivateConnection;
        private bool _disposed;
        private Database _currentDB;
        
        private String host;    
        public string Host {
            get { return host; }
            set { host = value; }
        }
        
        private int port;   
        public int Port {
            get { return port; }
            set { port = value; }
        }
        
        public Mongo():this(Connection.DEFAULTHOST,Connection.DEFAULTPORT)
        {        
        }
        
        public Mongo(String host):this(host,Connection.DEFAULTPORT)
        {
        }
        
        public Mongo(String host, int port) : this( new Connection(host, port)) 
        {
        }

        public Mongo(Connection connection)
        {
            _disposed = false;
            this.Host = connection.Host;
            this.Port = connection.Port;
            this.connection = connection;
            _hasPrivateConnection = true;
        }

        ~Mongo()
        {
            this.Dispose(false);
        }

        public Mongo(String leftHost, String rightHost):this(leftHost,Connection.DEFAULTPORT,rightHost,Connection.DEFAULTPORT,false){}        

        public Mongo(String leftHost, int leftPort, String rightHost, int rightPort):this(leftHost,leftPort,rightHost,rightPort,false){}
        
        public Mongo(String leftHost, int leftPort, String rightHost, int rightPort, bool slaveOk){
            this.Host = leftHost;
            this.port = leftPort;
            connection = new PairedConnection(leftHost,leftPort,rightHost,rightPort,slaveOk);
        }
        
        public Database GetDB(String name)
        {
            return new Database(connection, name);
        }

        public Database this[ String name ]  {
            get
            {
                return this.GetDB(name);
            }
        }

        public void SetCurrentDB(String name)
        {
            _currentDB = this[name];
        }
        public void SetCurrentDB(MongoDB.Driver.Database database)
        {
            _currentDB = database;
        }

        public void SetCurrentDB(String name, string username, string password)
        {
            _currentDB = this[name];
            this.CurrentDB.Authenticate(username, password);
        }

        public Database CurrentDB
        {
            get { return _currentDB; }
        }
        
        public Boolean Connect(){
            connection.Open();
            return connection.State == ConnectionState.Opened;
        }
        
        public Boolean Disconnect(){
            connection.Close();
            return connection.State == ConnectionState.Closed;
        }

        public Connection Connection
        {
            get { return this.connection; }
            set { 
                this.connection = value;
                _hasPrivateConnection = false;
            }
        }

        #region IDisposable Membri di

        protected void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_hasPrivateConnection && this.Connection != null)
                    {
                        this.Connection.Dispose();
                    }
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
