using System;
using System.IO;
using MongoDB.Driver;
using System.Collections.Generic;

namespace MongoDB.Driver.GridFS
{
    public class GridFS
    {
        private const int DEFAULT_CHUNKSIZE = 256 * 1024;
        private const string DEFAULT_ROOT = "fs";
        private Mongo mongo;
        Collection chunks;
        Collection files;
        private Document _fileDocument;
        private List<Chunk> _chunksDocument;
        private string _filename;

        #region Ctors

        private GridFS()
        {
            this.ChunkSize = DEFAULT_CHUNKSIZE;
            _fileDocument = null;
            _chunksDocument = new List<Chunk>();
            _filename = string.Empty;
            this.Oid = new Oid();
            this.UploadDate = DateTime.MinValue;
            this.Length = int.MinValue;
            this.ContentType = string.Empty;
        }

        public GridFS(Mongo mongo)
            : this(mongo, mongo.CurrentDB == null ? mongo.GetDB("test") : mongo.CurrentDB, DEFAULT_ROOT)
        {
        }

        public GridFS(Mongo mongo, Database db) 
            : this (mongo, db, DEFAULT_ROOT)
        {
        }

        public GridFS(Mongo mongo, string bucketName)
            : this(mongo, mongo.CurrentDB == null ? mongo.GetDB("test") : mongo.CurrentDB, bucketName)
        {
        }

        public GridFS(Mongo mongo, Database db, string bucketName)
            : this()
        {
            this.mongo = mongo;
            this.bucketName = bucketName;
            chunks = db[this.bucketName + ".chunks"];
            files = db[this.bucketName + ".files"];
            this.mongo.SetCurrentDB(db);
        }

        #endregion

        #region Public Members

        public string Filename 
        {
            get { return _filename; }
            set
            {
                if (value != _filename )
                {
                    _filename = value;
                    this.Refresh();
                }
            }
        }

        public Oid Oid { get; set; }

        public DateTime UploadDate { get; set; }

        public string ContentType { get; set; }

        public int Length { get; set; }

        public bool Exists
        {
            get 
            {
                return _fileDocument != null; 
            }
        }

        public Collection Collection
        {
            get { return this.files; }
            set
            {
                this.files = value;
                this.chunks = this.mongo[this.files.DbName][this.files.Name.Replace(".files", ".chunks")];
            }
        }

        public string CollectionName
        {
            get { return this.files.Name; }
            set
            {
                if( !value.EndsWith(".files" ))
                    value += ".files";
                this.Collection = this.mongo.CurrentDB[value];
            }
        }

        public int ChunkSize
        {
            get;
            set;
        }

        public string MD5
        {
            get;
            internal set;
        }

        #endregion

        #region Public Methods

        public byte[] Read()
        {
            if (this.Exists)
            {
                return  this.GetFile(this.Filename);
            }
            else
            {
                throw new FileNotFoundException("Impossibile leggere il file '" + this.Filename + "' dalla Collection '" + this.files.FullName + "'");
            }
        }

        public void Write(byte[] buffer)
        {
            this.Write(buffer, false, null);
        }

        public void Write(byte[] buffer, bool overwrite)
        {
            this.Write(buffer, overwrite, null);
        }

        public void Write(byte[] buffer, bool overwrite, string contentType)
        {
            this.ChunkFile(buffer, this.Filename, overwrite, contentType);
        }

        public void Refresh()
        {
            if (string.IsNullOrEmpty(this.Filename))
            {
                this.FileDocument = null;
            }
            else
            {
                bool closeConnection = false;
                try
                {
                    if (this.mongo.Connection.State != ConnectionState.Opened)
                    {
                        this.mongo.Connection.Open();
                        closeConnection = true;
                    }
                    Document searcher = new Document().Append("filename", this.Filename);
                    this.FileDocument = files.FindOne(searcher);
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    if (closeConnection)
                    {
                        this.mongo.Connection.Close();
                    }
                }
            }
        }

        public void Delete()
        {
            if (this.Exists)
            {
                bool closeConnection = false;
                try
                {
                    if (this.mongo.Connection.State != ConnectionState.Opened)
                    {
                        this.mongo.Connection.Open();
                        closeConnection = true;
                    }

                    foreach (Chunk erased in this._chunksDocument)
                    {
                        chunks.Delete(erased.Document);
                    }
                    files.Delete(this.FileDocument);

                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    if (closeConnection)
                    {
                        this.mongo.Connection.Close();
                    }
                }

                this.FileDocument = null;
            }
            else
            {
                throw new FileNotFoundException("Impossibile eliminare il file '" + this.Filename + "'");
            }
        }

        public void Touch()
        {
            if (this.Exists)
            {
                bool closeConnection = false;
                try
                {
                    if (this.mongo.Connection.State != ConnectionState.Opened)
                    {
                        this.mongo.Connection.Open();
                        closeConnection = true;
                    }

                    this.UploadDate = DateTime.Now;
                    this.FileDocument["uploadDate"] = this.UploadDate;

                    files.Update(this.FileDocument);

                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    if (closeConnection)
                    {
                        this.mongo.Connection.Close();
                    }
                }
            }
            else
            {
                throw new FileNotFoundException("Impossibile eseguire il Touch per il file '" + this.Filename + "'");
            }
        }

        #endregion

        #region private Properties
        private string bucketName;

        private Document FileDocument
        {
            get { return _fileDocument; }
            set 
            {
                if (_fileDocument != value)
                {
                    _fileDocument = value;
                    _chunksDocument.Clear();
                    if (_fileDocument != null)
                    {
                        this._filename = _fileDocument["filename"].ToString();
                        this.ChunkSize = Convert.ToInt32(_fileDocument["chinkSize"]);
                        this.Length = Convert.ToInt32(_fileDocument["length"]);
                        this.Oid = (MongoDB.Driver.Oid)_fileDocument["_id"];
                        this.UploadDate = Convert.ToDateTime(_fileDocument["uploadDate"]);
                        this.MD5 = _fileDocument["md5"].ToString();
                    }
                    else
                    {
                        this.ChunkSize = DEFAULT_CHUNKSIZE;
                        this.Length = int.MinValue;
                        this.Oid = new Oid();
                        this.UploadDate = DateTime.MinValue;
                        this.MD5 = null;
                    }
                }
            }
        }

        #endregion

        #region private Methods

        private byte[] GetFile(string mongoFilename)
        {
            if (!this.Exists)
            {
                return null;
            }

            byte[] retBuffer = null;
            
            System.Security.Cryptography.MD5 sscMD5 = System.Security.Cryptography.MD5.Create();

            if (_chunksDocument.Count == 0)
            {

                bool closeConnection = false;
                try
                {

                    if (this.mongo.Connection.State != ConnectionState.Opened)
                    {
                        this.mongo.Connection.Open();
                        closeConnection = true;
                    }

                    List<Chunk> chunksList = new List<Chunk>();
                    Document searcher = new Document();
                    searcher.Add("files_id", this.Oid);

                    foreach (Document d in chunks.Find(searcher).Documents)
                    {
                        Chunk c = Chunk.CreateFromDocument(d);
                        _chunksDocument.Add(c);
                    }
                    chunksList.Sort(delegate(Chunk a, Chunk b)
                    {
                        if (a == null && b == null) return 0;
                        else if (a == null) return -1;
                        else if (b == null) return 1;
                        else
                        {
                            return a.Number - b.Number;
                        }
                    });

                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    if (closeConnection)
                    {
                        this.mongo.Connection.Close();
                    }
                }
            }

            retBuffer = new byte[this.Length];

            int destIndex = 0;
            foreach (Chunk c in _chunksDocument)
            {
                byte[] chunkBuffer = c.Data;
                Array.Copy(chunkBuffer, 0, retBuffer, destIndex, chunkBuffer.Length);
                destIndex += chunkBuffer.Length;
            }

            byte[] mHash = sscMD5.ComputeHash(retBuffer);

            if (this.MD5 != Convert.ToBase64String(mHash))
            {
                throw new InvalidDataException("Hash MD5 errato per il file '" + mongoFilename + "' dalla collection '" + this.files.FullName + "'");
            }

            return retBuffer;
        }

        private void ChunkFile(byte[] data, string mongoname, bool overwrite, string contentType)
        {
            if (string.IsNullOrEmpty(contentType))
            {
                contentType = "file/undef";
                if (mongoname.LastIndexOf('.') > 0)
                {
                    contentType = "file/" + mongoname.Substring(mongoname.LastIndexOf('.') + 1);
                }
            }

            _chunksDocument.Clear();
            System.Security.Cryptography.MD5 sscMD5 = System.Security.Cryptography.MD5.Create();
            Document file = null;
            bool closeConnection = false;
            try
            {
                if (this.mongo.Connection.State != ConnectionState.Opened)
                {
                    this.mongo.Connection.Open();
                    closeConnection = true;
                }

                file = files.FindOne(this.FileDocument);
                if (file != null)
                {
                    if (overwrite)
                    {
                        file["length"] = data.Length;
                        file["contentType"] = contentType;
                        file["uploadDate"] = DateTime.Now;
                        file["chunkSize"] = this.ChunkSize;

                        byte[] mHash = sscMD5.ComputeHash(data);

                        file["md5"] = Convert.ToBase64String(mHash);

                        files.Update(file);

                        Document eraser = new Document();
                        eraser.Add("files_id", file["_id"]);
                        foreach (Document erased in chunks.Find(eraser).Documents)
                        {
                            chunks.Delete(erased);
                        }

                        if (data.Length <= this.ChunkSize)
                        {
                            Chunk chunk = new Chunk();
                            chunk.FileID = this.Oid;
                            chunk.Number = 0;
                            chunk.Data = data;
                            _chunksDocument.Add(chunk);
                        }
                        else
                        {
                            int chucksNumbers = data.Length / this.ChunkSize + (data.Length % this.ChunkSize > 0 ? 1 : 0);
                            for (int i = 0; i < chucksNumbers; i++)
                            {
                                byte[] buffer = new byte[i < chucksNumbers - 1 ? this.ChunkSize : data.Length % this.ChunkSize];
                                Array.Copy(data, i * this.ChunkSize, buffer, 0, buffer.Length);

                                Chunk chunk = new Chunk();
                                chunk.FileID = this.Oid;
                                chunk.Number = i;
                                chunk.Data = buffer;
                                _chunksDocument.Add(chunk);
                            }
                        }
                    }
                    else
                    {
                        throw new Exception("Il file '" + file["filename"] + " esiste nella collection '" + this.files.FullName + "'");
                    }
                }
                else
                {
                    file = new Document();
                    OidGenerator oidg = new OidGenerator();
                    file.Add("_id", oidg.Generate());
                    file.Add("contentType", contentType);
                    file.Add("length", data.Length);
                    file.Add("chunkSize", DEFAULT_CHUNKSIZE);
                    file.Add("uploadDate", DateTime.Now);

                    byte[] mHash = sscMD5.ComputeHash(data);

                    file.Add("md5", Convert.ToBase64String(mHash));

                    files.Insert(file);

                    if (data.Length <= this.ChunkSize)
                    {
                        Chunk chunk = new Chunk();
                        chunk.FileID = this.Oid;
                        chunk.Number = 0;
                        chunk.Data = data;
                        _chunksDocument.Add(chunk);
                    }
                    else
                    {
                        int chucksNumbers = data.Length / this.ChunkSize + (data.Length % this.ChunkSize > 0 ? 1 : 0);
                        for (int i = 0; i < chucksNumbers; i++)
                        {
                            byte[] buffer = new byte[i < chucksNumbers - 1 ? this.ChunkSize : data.Length % this.ChunkSize];
                            Array.Copy(data, i * this.ChunkSize, buffer, 0, buffer.Length);

                            Chunk chunk = new Chunk();
                            chunk.FileID = this.Oid;
                            chunk.Number = i;
                            chunk.Data = buffer;
                            _chunksDocument.Add(chunk);
                        }
                    }
                }

                this.CreateIndex();
                foreach (Chunk c in _chunksDocument)
                {
                    chunks.Insert(c.Document);
                }
                this.FileDocument = file;

            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                if (closeConnection)
                {
                    this.mongo.Connection.Close();
                }
            }

        }

        private void CreateIndex()
        {
            Document d = new Document();

            d.Add("name", "file_id_1_n_1");

            Document result = mongo.CurrentDB["system.indexes"].FindOne(d);
            if (result == null)
            {
                d.Add("ns", this.chunks.FullName);

                Document subd = new Document();
                subd.Add("file_id", 1);
                subd.Add("n", 1);

                d.Add("key", subd);

                mongo.CurrentDB["system.indexes"].Insert(d);
            }

        }

        #endregion
    }


}
