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
            // Don't forget to whitelist your ip!
            string connection_string = "mongodb+srv://username:password@cluster_ip/test?retryWrites=true&w=majority";
            string db_name = "airbnb";
            bool do_import = false;
            
            // Wait until connection completes before proceeding, and then create db if it doesn't already exist
            MongoClient client = new MongoClient(connection_string);
            IMongoDatabase db = client.GetDatabase(db_name);


            // (collection_name, import_folder, import_chunk_size)
            IList<Tuple<string, string, int>> sources = new List<Tuple<string, string, int>>();
            sources.Add(Tuple.Create("Listings", @"C:/Users/Whyve/Documents/CS488/Dataset/CSV/Listings", 1000));
            sources.Add(Tuple.Create("Reviews", @"C:/Users/Whyve/Documents/CS488/Dataset/CSV/Reviews", 25000));

            // Import data to db from specified folders
            if (do_import)
            {
                IMongoCollection<BsonDocument> col;
                foreach (Tuple<string, string, int> src in sources)
                {
                    // Set the collection name to the first element in the tuple
                    col = db.GetCollection<BsonDocument>(src.Item1);
                    logger.Info($"Importing documets from '{src.Item2}'...");

                    // Import all .json and .csv files from the location specified in the second element of the tuple
                    // The third element of the tuple specifies how many rows to read in before importing them into the db
                    var count = import_folder(src.Item2, col, src.Item3);
                    logger.Info($"Imported {count.Result.counter} documents from {src.Item2}!");
                }
            }

            IMongoCollection<BsonDocument> collection = db.GetCollection<BsonDocument>("Listings");

            // (price_limit, min_nights_limit)
            List<Tuple<int, int>> test_inputs = new List<Tuple<int, int>>();
            test_inputs.Add(Tuple.Create(20, 1));
            test_inputs.Add(Tuple.Create(100, 7));
            test_inputs.Add(Tuple.Create(700, 31));
            foreach (Tuple<int, int> input in test_inputs)
            {
                Console.WriteLine('\n' + new string('-', 100) + '\n');
                logger.Debug($"Querying with price_limit={input.Item1} and min_nights_limit={input.Item2}");
                var count = Test_Query(collection, input.Item1, input.Item2);
                logger.Info($"Returned {count.Result} records!");

            }
            
            // Keep terminal open when program finishes
            Console.ReadLine();
        }

        // Import all .csv and .json files from the src string into the collection in batches of import_chunk_size
        static async Task<IntHolder> import_folder(string src, IMongoCollection<BsonDocument> collection, int import_chunk_size)
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
                        count = await import_csv(file, collection, import_chunk_size);
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

        // Read in an entire .json file as a string, turn it into a bson document, and import it all at once into the db
        static async Task<int> import_json(string src, IMongoCollection<BsonDocument> collection)
        {
            string data = System.IO.File.ReadAllText(src);
            var document = BsonSerializer.Deserialize<BsonDocument>(data);
            int count = document.ElementCount;
            logger.Debug($"Translated {src} into a BSON document with {count} elements!");
            await collection.InsertOneAsync(document);
            logger.Debug($"Finished importing {src} into {collection.CollectionNamespace}");
            return count;
        }

        // Read in rows from a .csv file in a stream
        static async Task<int> import_csv(string src, IMongoCollection<BsonDocument> collection, int import_chunk_size)
        {
            int count = 0;
            var documents = new List<BsonDocument>();

            using (var fin = new StreamReader(src))
            using (var csv = new CsvReader(fin))
            using (var cr = new CsvDataReader(csv))
            {
                // Create utility arrays that can hold all elements of the header row and any given data row
                int amt_cols = cr.FieldCount;
                Debug.Assert(amt_cols > 0);
                var headers = new object[amt_cols];
                var records = new object[amt_cols];
                logger.Debug($"Parsed {amt_cols} columns in file {src}");
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
                    // Weed out any empty string elements
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

                    // Add documents in batches to the db
                    if (count % import_chunk_size == 0)
                    {
                        await collection.InsertManyAsync(documents);
                        logger.Debug($"Uploded {import_chunk_size} records to {collection.CollectionNamespace.CollectionName}...");
                        documents.Clear();
                    }
                }

                // Add any remaining docs to the db
                if (documents.Count != 0)
                {
                    await collection.InsertManyAsync(documents);
                }
            }
            return count;
        }     

        // Run a test query on the Listings collection
        // Queries for all listings of up to price_limit with a min_nights_limit 
        // Returns the amount of documents from query
        static async Task<int> Test_Query(IMongoCollection<BsonDocument> collection, int price_limit, int min_nights_limit)
        {
            int count = 0;

            Console.WriteLine($"Here are all of the {min_nights_limit}-night stays that cost at most ${price_limit}:");
            Console.WriteLine(new string('-', 50));

            // Create the query filter (WHERE)
                                                var b = Builders<BsonDocument>.Filter;
            var filter = b.Lte("price", price_limit) & b.Eq("minimum_nights_avg_ntm", min_nights_limit);
            
            // Create the projection
            // We can also use a builder here, but string is more consise and managable
            var projection = "{_id:0, id:1, price:1, neighbourhood_cleansed:1, accomodates:1, smart_location:1, minimum_nights_avg_ntm:1}";

            // Specify sorting
            SortDefinition<BsonDocument> sort = "{ price: 1 }";

            // Run the query asynchronously
            await collection.Find(filter)
                            .Project(projection)
                            .Sort(sort)
                            .ForEachAsync(async document =>
                            {
                                count++;
                                var result = document.ToDictionary();
                                Console.Write($"Book out at listing #{result["id"]} in {result["neighbourhood_cleansed"]} - {result["smart_location"]}");
                                if (result.ContainsKey("accomodates"))
                                {
                                    Console.Write($", which accomodates {result["accomodates"]} people ");
                                }
                                Console.WriteLine($" for the lovely {result["minimum_nights_avg_ntm"]} price of ${result["price"]}!");
                            });

            return count;
        }
    }
}
