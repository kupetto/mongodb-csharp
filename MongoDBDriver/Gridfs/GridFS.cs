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
        private List<Document> _chunksDocument;

        #region Ctors

        private GridFS()
        {
            _chunksDocument = new List<Document>();
        }

        public GridFS(Mongo mongo)
            : this(mongo, mongo.CurrentDB == null ? mongo.GetDB("fs") : mongo.CurrentDB, DEFAULT_ROOT)
        {
        }

        public GridFS(Mongo mongo, Database db) : this (mongo, db, DEFAULT_ROOT)
        {
        }

        public GridFS(Mongo mongo, string bucketName)
            : this(mongo, mongo.CurrentDB == null ? mongo.GetDB("fs") : mongo.CurrentDB, bucketName)
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

        public void StoreFile(string localFile, string mongoFilename, bool overwrite)
        {
            StoreFile(localFile, mongoFilename, overwrite, null);
        }

        public void StoreFile(Stream stream, string mongoFilename, bool overwrite)
        {
            StoreFile(stream, mongoFilename, overwrite, null);
        }

        public void StoreFile(byte[] bytes, string mongoFilename, bool overwrite)
        {
            StoreFile(bytes, mongoFilename, overwrite, null);
        }

        public void StoreFile(string localFile, string mongoFilename, bool overwrite, string contentType)
        {
            byte[] buffer = null;
            using (FileStream fs = new FileStream(localFile, FileMode.Open))
            {
                try
                {
                    buffer = new byte[fs.Length];
                    fs.Read(buffer, 0, buffer.Length);
                }
                finally
                {
                    fs.Close();
                }
            }
            this.ChunkFile(buffer, mongoFilename, overwrite, contentType);
        }

        public void StoreFile(Stream stream, string mongoFilename, bool overwrite, string contentType)
        {
            byte[] buffer = null;
            using (BinaryReader fs = new BinaryReader(stream))
            {
                try
                {
                    buffer = new byte[fs.BaseStream.Length];
                    fs.Read(buffer, 0, buffer.Length);
                }
                finally
                {
                    fs.Close();
                }
            }
            this.ChunkFile(buffer, mongoFilename, overwrite, contentType);
        }

        public void StoreFile(byte[] bytes, string mongoFilename, bool overwrite, string contentType)
        {
            this.ChunkFile(bytes, mongoFilename, overwrite, contentType);
        }

        public byte[] GetFile(string mongoFilename)
        {
            byte[] retBuffer = null;
            _fileDocument = new Document();
            _fileDocument.Add("filename", mongoFilename);
            _chunksDocument.Clear();
            System.Security.Cryptography.MD5 sscMD5 = System.Security.Cryptography.MD5.Create();

            bool closeConnection = false;
            try
            {
                if (this.mongo.Connection.State != ConnectionState.Opened)
                {
                    this.mongo.Connection.Open();
                    closeConnection = true;
                }

                Document file = files.FindOne(_fileDocument);
                if (file != null)
                {
                    List<Document> chunksList = new List<Document>();
                    Document searcher = new Document();
                    searcher.Add("files_id", file["_id"]);
                    foreach (Document d in chunks.Find(searcher).Documents)
                    {
                        chunksList.Add(d);
                    }
                    chunksList.Sort(delegate(Document a, Document b)
                    {
                        if (a == null && b == null) return 0;
                        else if (a == null) return -1;
                        else if (b == null) return 1;
                        else
                        {
                            return Convert.ToInt32(a["n"]) - Convert.ToInt32(b["n"]);
                        }
                    });
                    _fileDocument = file;
                    _chunksDocument = chunksList;
                }
                else
                {
                    throw new FileNotFoundException("Impossibile ottenere il file '" + mongoFilename + "' dalla collection '" + this.files.FullName + "'");
                }

                retBuffer = new byte[Convert.ToInt32( _fileDocument["length"] )];
                int destIndex = 0;
                foreach (Document d in _chunksDocument)
                {
                    byte[] chunkBuffer = (byte[])d["data"];
                    //int number = Convert.ToInt32(d["n"]);
                    Array.Copy(chunkBuffer, 0, retBuffer, destIndex, chunkBuffer.Length);
                    destIndex += chunkBuffer.Length;
                }

                string hash = _fileDocument["md5"].ToString();
                byte[] mHash = sscMD5.ComputeHash(retBuffer);

                if (hash != Convert.ToBase64String(mHash))
                {
                    throw new InvalidDataException("Hash MD5 errato per il file '" + mongoFilename + "' dalla collection '" + this.files.FullName + "'");
                }
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

            return retBuffer;
        }

        public void DeleteFile(string mongoFilename)
        {            
            Document doc = new Document();
            doc.Add("filename", mongoFilename);
            Document result = files.FindOne(doc);
            if (result != null)
            {
                this.CreateIndex();
                Document eraser = new Document();
                eraser.Add("files_id", result["_id"]);
                foreach (Document erased in chunks.Find(eraser).Documents)
                {
                    chunks.Delete(erased);
                }
                files.Delete(result);
            }
        }
       
        #region Properties
        private string bucketName;

        //private void ChunkFile(byte[] data, string mongoname, bool overwrite)
        //{
        //    ChunkFile(data, mongoname, overwrite, null);
        //}

        private void ChunkFile(byte[] data, string mongoname, bool overwrite, string contentType)
        {
            _fileDocument = new Document();
            _fileDocument.Add("filename", mongoname);

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

            bool closeConnection = false;
            try
            {
                if (this.mongo.Connection.State != ConnectionState.Opened)
                {
                    this.mongo.Connection.Open();
                    closeConnection = true;
                }

                Document file = files.FindOne(_fileDocument);
                if (file != null)
                {
                    if (overwrite)
                    {
                        _fileDocument = file;
                        _fileDocument["length"] = data.Length;
                        _fileDocument["contentType"] = contentType;
                        _fileDocument["uploadDate"] = DateTime.Now;

                        byte[] mHash = sscMD5.ComputeHash(data);

                        _fileDocument["md5"] = Convert.ToBase64String(mHash);

                        files.Update(_fileDocument);

                        Document eraser = new Document();
                        eraser.Add("files_id", _fileDocument["_id"]);
                        foreach (Document erased in chunks.Find(eraser).Documents)
                        {
                            chunks.Delete(erased);
                        }

                        if (data.Length <= DEFAULT_CHUNKSIZE)
                        {
                            Document chunk = new Document();
                            chunk.Add("files_id", _fileDocument["_id"]);
                            chunk.Add("n", 0);
                            chunk.Add("data", data);
                            _chunksDocument.Add(chunk);
                        }
                        else
                        {
                            int chucksNumbers = data.Length / DEFAULT_CHUNKSIZE + (data.Length % DEFAULT_CHUNKSIZE > 0 ? 1 : 0);
                            for (int i = 0; i < chucksNumbers; i++)
                            {
                                byte[] buffer = new byte[i < chucksNumbers - 1 ? DEFAULT_CHUNKSIZE : data.Length % DEFAULT_CHUNKSIZE];
                                Array.Copy(data, i * DEFAULT_CHUNKSIZE, buffer, 0, buffer.Length);

                                Document chunk = new Document();
                                chunk.Add("files_id", _fileDocument["_id"]);
                                chunk.Add("n", i);
                                chunk.Add("data", buffer);
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
                    OidGenerator oidg = new OidGenerator();
                    _fileDocument.Add("_id", oidg.Generate());
                    _fileDocument.Add("contentType", contentType);
                    _fileDocument.Add("length", data.Length);
                    _fileDocument.Add("chunkSize", DEFAULT_CHUNKSIZE);
                    _fileDocument.Add("uploadDate", DateTime.Now);

                    byte[] mHash = sscMD5.ComputeHash(data);

                    _fileDocument.Add("md5", Convert.ToBase64String(mHash));

                    files.Insert(_fileDocument);

                    if (data.Length <= DEFAULT_CHUNKSIZE)
                    {
                        Document chunk = new Document();
                        chunk.Add("files_id", _fileDocument["_id"]);
                        chunk.Add("n", 0);
                        chunk.Add("data", data);
                        _chunksDocument.Add(chunk);
                    }
                    else
                    {
                        int chucksNumbers = data.Length / DEFAULT_CHUNKSIZE + (data.Length % DEFAULT_CHUNKSIZE > 0 ? 1 : 0);
                        for (int i = 0; i < chucksNumbers; i++)
                        {
                            byte[] buffer = new byte[i < chucksNumbers - 1 ? DEFAULT_CHUNKSIZE : data.Length % DEFAULT_CHUNKSIZE];
                            Array.Copy(data, i * DEFAULT_CHUNKSIZE, buffer, 0, buffer.Length);

                            Document chunk = new Document();
                            chunk.Add("files_id", _fileDocument["_id"]);
                            chunk.Add("n", i);
                            chunk.Add("data", buffer);
                            _chunksDocument.Add(chunk);
                        }
                    }                    
                }
                if (_fileDocument != null && _chunksDocument.Count > 0)
                {
                    this.CreateIndex();
                    foreach (Document cc in _chunksDocument)
                    {
                        chunks.Insert(cc);
                    }
                }
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
