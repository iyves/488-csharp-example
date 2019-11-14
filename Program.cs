using MongoDB.Bson;
using MongoDB.Driver;

using System;
using System.Linq;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

using CsvHelper;
using MongoDB.Bson.Serialization;
using NLog;


namespace CS488
{
    class IntHolder
    {
        public int counter = 0;
    }

    class Program
    {
        // The format in which to print the current time
        const string date_fmt = "G";

        // For logging purposes
        private static Logger logger = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            gcp_connect(); // Warning bc I am not awaiting this function, but it's ok bc I'm not doing concurrency rn
            Console.ReadLine();
        }

        static async Task gcp_connect()
        {   
            var client = new MongoClient("mongodb://34.82.135.70:27017"); // Change this with the external IP of your primary instance!    D
            IMongoDatabase db = client.GetDatabase("airbnb"); // Creates the database if it doesn't already exist

            logger.Info("About to query...\n");
            var collection = db.GetCollection<BsonDocument>("listings_detailed");
            var src = @"C:/Users/Whyve/Documents/CS488/Dataset/CSV/Listings";

            logger.Info($"Importing documets from '{src}'...");
            var count = import_folder(src, collection);
                    
            logger.Info($"Imported {count.Result.counter} documents from {src}!");

            Console.WriteLine("Here are all the one-night stays that cost at most $20:");
            Console.WriteLine(new string('-', 50));
            var filter = "{price: {'$lte': 20}, minimum_nights_avg_ntm: 1}"; // Example of how you may query the db
            await collection.Find(filter)
                .ForEachAsync(doc => Console.WriteLine(doc));
        }

        static async Task<IntHolder> import_folder(string src, IMongoCollection<BsonDocument> collection)
        {
            // Check that src directory exists
            if (!System.IO.Directory.Exists(src))
            {
                throw new ArgumentException();
            }

            // Loop through all files in the directory
            var files = System.IO.Directory.EnumerateFiles(src);

            IntHolder total_count = new IntHolder();
            Regex pattern = new Regex(@"\.[a-zA-Z]+$");
            foreach (string file in files)
            {
                logger.Debug($"Importing file: {file}");
                int count = 0;
                // Check file format
                switch (pattern.Match(file).Value)
                {
                    case ".json":
                        count = await import_json(file, collection); // TODO this is a waste of async :/
                        break;
                    case ".csv":
                        count = await import_csv(file, collection);
                        break;
                    default:
                        logger.Error($"Unable to import non-json file: {file}");
                        continue;
                }

                // Check amt of records read
                if (count > 0)
                {
                    logger.Info($"Successfully imported file: {file}");
                    lock (total_count)
                    {
                        total_count.counter += count;
                    }
                    logger.Debug($"count = {count}; total_count = {total_count.counter}");
                }
                else
                {
                    logger.Error($"Failed to import the file: {file}");
                }
            }
            return total_count;
        }

        static async Task<int> import_json(string src, IMongoCollection<BsonDocument> collection)
        {
            string data = System.IO.File.ReadAllText(src);
            //logger.Debug(data);
            var document = BsonSerializer.Deserialize<BsonDocument>(data);
            int count = document.ElementCount;
            logger.Debug($"Translated {src} into a BSON document with {count} elements!");
            //await collection.InsertOneAsync(document);
            logger.Debug($"Finished importing {src} into {collection.CollectionNamespace}");
            return count;
        }

        static async Task<int> import_csv(string src, IMongoCollection<BsonDocument> collection)
        {
            int count = 0;
            int insert_amt = 1000;
            var documents = new List<BsonDocument>();

            using (var fin = new StreamReader(src))
            using (var csv = new CsvReader(fin))
            using (var cr = new CsvDataReader(csv))
            {
                var headers = new object[cr.FieldCount];
                var records = new object[cr.FieldCount];
                logger.Debug($"Parsed {cr.FieldCount} columns in file {src}");
                var ret = cr.GetValues(headers);
                Debug.Assert(ret > 0);
                logger.Debug($"Returned {ret} : " + string.Join(',', headers));

                // Keep reading until EOF
                while (cr.Read())
                {
                    // Read record line
                    ret = cr.GetValues(records);
                    Debug.Assert(ret > 0);

                    // Create a dictionary mapping each header element to its respective record element
                    var zipped = headers.Zip(records, (h, r) => new { h, r } )
                                        .Where(item => item.r.ToString() != "")
                                        .ToDictionary(item => item.h, item => {
                                            int i;
                                            double d;
                                            string r = item.r.ToString();
                                            if (r.StartsWith('$')) r = r.Substring(1);
                                            if (int.TryParse(r, out i)) return i;
                                            if (double.TryParse(r, out d)) return d;
                                            return item.r;                           
                                        });

                    // Add dictionary to import buffer
                    documents.Add(zipped.ToBsonDocument());
                    ++count;

                    if (count % insert_amt == 0) // Insert documents in batches of insert_amt size
                    {
                        await collection.InsertManyAsync(documents); // TODO -> use InsertManyAsync for concurrency?
                        logger.Debug($"Uploded {insert_amt} records to the database...");
                        documents.Clear();
                    }
                }
                if (documents.Count != 0) // Insert remainder documents
                {
                    await collection.InsertManyAsync(documents);
                }
            }
            return count;
        }     
    }
}
