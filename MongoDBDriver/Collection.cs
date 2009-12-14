using System;
using System.Collections.Generic;
using System.IO;

using MongoDB.Driver.Bson;
using MongoDB.Driver.IO;

namespace MongoDB.Driver
{
    public class Collection
    {
        private Connection connection;
        
        private string name;        
        public string Name {
            get { return name; }
        }
        
        private string dbName;      
        public string DbName {
            get { return dbName; }
        }
        
        public string FullName{
            get{ return dbName + "." + name;}
        }
        
        private CollectionMetaData metaData;        
        public CollectionMetaData MetaData {
            get { 
                if(metaData == null){
                    metaData = new CollectionMetaData(this.dbName,this.name, this.connection);
                }
                return metaData;
            }
        }
        
        private static OidGenerator oidGenerator = new OidGenerator();
                
        public Collection(string name, Connection conn, string dbName)
        {
            this.name = name;
            this.connection = conn;
            this.dbName = dbName;
        }
                
        public Document FindOne(Document spec){
            Cursor cur = this.Find(spec, -1,0,null);
            foreach(Document doc in cur.Documents){
                cur.Dispose();
                return doc;
            }
            //FIXME Decide if this should throw a not found exception instead of returning null.
            return null; //this.Find(spec, -1, 0, null)[0];
        }
        public Cursor FindAll(){
            Document spec = new Document();
            return this.Find(spec, 0, 0, null);
        }
        
        public Cursor Find(Document spec){
            return this.Find(spec, 0, 0, null);
        }
        
        public Cursor Find(Document spec, int limit, int skip){
            return this.Find(spec, limit, skip, null);
        }
        
        public Cursor Find(Document spec, int limit, int skip, Document fields){
            if(spec == null) spec = new Document();
            Cursor cur = new Cursor(connection, this.FullName, spec, limit, skip, fields);
            return cur;
        }
        
        public long Count(){
            return this.Count(new Document());
        }
        
        public long Count(Document spec){
            Database db = new Database(this.connection, this.dbName);
            try{
                Document ret = db.SendCommand(new Document().Append("count",this.Name).Append("query",spec));
                double n = (double)ret["n"];
                return Convert.ToInt64(n);
            }catch(MongoCommandException){
                //FIXME This is an exception condition when the namespace is missing. -1 might be better here but the console returns 0.
                return 0;
            }
            
        }
        
        public void Insert(Document doc){
            Document[] docs = new Document[]{doc,};
            this.Insert(docs);
        }
        
        public void Insert(IEnumerable<Document> docs){
            InsertMessage im = new InsertMessage();
            im.FullCollectionName = this.FullName;
            List<BsonDocument> bdocs = new List<BsonDocument>();
            foreach(Document doc in docs){
                if(doc.Contains("_id") == false) doc["_id"] = oidGenerator.Generate();
                bdocs.Add(BsonConvert.From(doc));
            }
            im.BsonDocuments = bdocs.ToArray();
            try{
                this.connection.SendMessage(im);    
            }catch(IOException ioe){
                throw new MongoCommException("Could not insert document, communication failure", this.connection,ioe);
            }   
        }
        
        public void Delete(Document selector){
            DeleteMessage dm = new DeleteMessage();
            dm.FullCollectionName = this.FullName;
            dm.Selector = BsonConvert.From(selector);
            try{
                this.connection.SendMessage(dm);
            }catch(IOException ioe){
                throw new MongoCommException("Could not delete document, communication failure", this.connection,ioe);
            }
        }
        
        public void Update(Document doc){
            //Try to generate a selector using _id for an existing document.
            //otherwise just set the upsert flag to 1 to insert and send onward.
            Document selector = new Document();
            int upsert = 0;
            if(doc.Contains("_id")  & doc["_id"] != null){
                selector["_id"] = doc["_id"];   
            }else{
                //Likely a new document
                doc["_id"] = oidGenerator.Generate();
                upsert = 1;
            }
            this.Update(doc, selector, upsert);
        }
        
        public void Update(Document doc, Document selector){
            this.Update(doc, selector, 0);
        }
        
        public void Update(Document doc, Document selector, int upsert){
            UpdateMessage um = new UpdateMessage();
            um.FullCollectionName = this.FullName;
            um.Selector = BsonConvert.From(selector);
            um.Document = BsonConvert.From(doc);
            um.Upsert = upsert;
            try{
                this.connection.SendMessage(um);
            }catch(IOException ioe){
                throw new MongoCommException("Could not update document, communication failure", this.connection,ioe);
            }           
            
        }
        
        public void UpdateAll(Document doc, Document selector){
            //TODO do this server side with generated code.
            Cursor toUpdate = this.Find(selector);
            foreach(Document udoc in toUpdate.Documents){
                Document updSel = new Document();
                updSel["_id"] = udoc["_id"];
                udoc.Update(doc);
                this.Update(udoc, updSel,0);
            }
        }
    }
}
