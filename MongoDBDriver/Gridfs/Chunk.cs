using System;
using System.Collections.Generic;
using System.Text;

namespace MongoDB.Driver.GridFS
{
	public class Chunk
	{
        internal Document Document
        {
            get
            {
                Document d = new Document();
                d.Append("_id", this.Oid);
                d.Append("files_id", this.FileID);
                d.Append("n", this.Number);
                d.Append("data", this.Data);
                return d;
            }
        }

        public Oid Oid
        {
            get;
            internal set;
        }

        public Oid FileID
        {
            get;
            internal set;
        }

        public int Number
        {
            get;
            internal set;
        }

        public byte[] Data
        {
            get;
            internal set;
        }

        public static Chunk CreateFromDocument(Document document)
        {
            if (document != null && document.Contains("files_id") && document.Contains("data") && document.Contains("n"))
            {
                Chunk chunk = new Chunk();
                chunk.Oid = (MongoDB.Driver.Oid)document["_id"];
                chunk.FileID = (MongoDB.Driver.Oid)document["files_id"];
                chunk.Number = Convert.ToInt32(document["n"]);
                chunk.Data = (byte[])document["data"];
                return chunk;
            }
            else
            {
                throw new ArgumentException("Documento non valido per creare un Chunk");
            }
        }

        public static Chunk CreateNew(Oid fileId, int number, byte[] data)
        {
            OidGenerator generator = new OidGenerator();
            Chunk chunk = new Chunk();
            chunk.Oid = generator.Generate();
            chunk.Data = data;
            chunk.FileID = fileId;
            chunk.Number = number;
            return chunk;
        }
	}
}
