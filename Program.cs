using MongoDB.Bson;
using MongoDB.Driver;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using CsvHelper;

namespace CS488
{
    class Program
    {

        static void Main(string[] args)
        {
            gcp_connect(); // Warning bc I am not awaiting this function, but it's ok bc I'm not doing concurrency rn
            Console.ReadLine();
        }

        static async Task gcp_connect()
        {   
            var client = new MongoClient("mongodb://34.82.100.205:27017"); // Change this with the external IP of your primary instance!    D
            IMongoDatabase db = client.GetDatabase("airbnb"); // Creates the database if it doesn't already exist

            Console.Write("About to query...\n");
            var listings = db.GetCollection<BsonDocument>("listings");
            var src = "../../../data/listings.csv"; // Location of listings.csv (can also use a full path)

            Console.WriteLine($"Importing documets from {src}...");
            int count = import_csv(src, listings);
            Console.WriteLine($"Imported {count} documents from {src}!");

            Console.WriteLine("Here are all the one-night stays that cost at most $20:");
            Console.WriteLine(new string('-', 50));
            var filter = "{price: {'$lte': 20}, minimum_nights: 1}"; // Example of how you may query the db
            await listings.Find(filter)
                .ForEachAsync(doc => Console.WriteLine(doc));
        }

        static int import_csv(string src, IMongoCollection<BsonDocument> collection)
        {
            int count = 0;
            int insert_amt = 100;
            var documents = new List<BsonDocument>();

            using (var fin = new StreamReader(src))
            using (var csv = new CsvReader(fin))
            {
                var listing = new Listing();
                var records = csv.EnumerateRecords(listing);

                foreach (var r in records)
                {
                    documents.Add(toBson(r));
                    ++count;
                    if (count % insert_amt == 0) // Insert documents in batches of insert_amt size
                    {
                        collection.InsertMany(documents); // TODO -> use InsertManyAsync for concurrency?
                        documents.Clear();
                    }
                }
                if (documents.Count != 0) // Insert remainder documents
                {
                    collection.InsertMany(documents);
                }

                return count;
            }


            // TODO: Edit Listings class so that this function can work from there
            BsonDocument toBson(Listing ls)
            {
                BsonDocument to_return = new BsonDocument();
                to_return.Add("id", ls.id);
                to_return.Add("name", ls.name);
                to_return.Add("host_id", ls.host_id) ;
                to_return.Add("host_name", ls.host_name);
                to_return.Add("neighbourhood_group", ls.neighbourhood_group);
                to_return.Add("neighbourhood", ls.neighbourhood);
                to_return.Add("latitude", ls.latitude);
                to_return.Add("longitude", ls.longitude);
                to_return.Add("room_type", ls.room_type);
                to_return.Add("price", ls.price);
                to_return.Add("minimum_nights", ls.minimum_nights);
                to_return.Add("number_of_reviews", ls.number_of_reviews);
                to_return.Add("last_review", ls.last_review);
                to_return.Add("reviews_per_month", ls.reviews_per_month);
                to_return.Add("calculated_host_listings_count", ls.calculated_host_listings_count);
                to_return.Add("availability_365", ls.availability_365);
                return to_return;
            }
        }

        // Utility class for csv import via CsvHelper
        // For this simple implementation, field order matches .csv column order
        // For this simple implementation, adding member functions will lead to bugs (perhaps has to do w/ memory layout)
        // There are more dynamic options: https://joshclose.github.io/CsvHelper/getting-started
        public class Listing
        {
            public int id { get; set; }
            public string name { get; set; }
            public int host_id { get; set; }
            public string host_name { get; set; }
            public string neighbourhood_group { get; set; }
            public string neighbourhood { get; set; }
            public double latitude { get; set; }
            public double longitude { get; set; }
            public string room_type { get; set; }
            public int price { get; set; }
            public int minimum_nights { get; set; }
            public int number_of_reviews { get; set; }
            public string last_review { get; set; }
            public string reviews_per_month { get; set; }
            public int calculated_host_listings_count { get; set; }
            public int availability_365 { get; set; }
        }
    }
}
