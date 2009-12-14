using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TestMongo
{
    class Program
    {
        static void Main(string[] args)
        {
            MongoDB.Driver.Connection c = new MongoDB.Driver.Connection("partescano");
            c.Open();

            MongoDB.Driver.Database db = new MongoDB.Driver.Database(c, "goldfix_db");
            MongoDB.Driver.Mongo mongo = new MongoDB.Driver.Mongo(c);


            //MongoDB.Driver.GridFS.GridFS fs = new MongoDB.Driver.GridFS.GridFS(mongo, db);

            //byte[] b = System.IO.File.ReadAllBytes(@"c:\tmp\test_big.rar");
            
            for (int i = 0; i < 1; i++)
            {
                //fs.StoreFile(b, "File_" + i.ToString(), true);
            }

            for (int i = 0; i < 1; i++)
            {
                //fs.DeleteFile("File_" + i.ToString());
            }

            MongoDB.Driver.Document cerca=new MongoDB.Driver.Document();
            cerca.Add("j", 5);
            

            MongoDB.Driver.Cursor cursor = db.GetCollection("things").

            mongo.Disconnect();
            c.Close();
        }
    }
}
