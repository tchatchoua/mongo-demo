using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoPlayByPlay
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var client = new MongoClient("mongodb://tchatchoua:Password90@cluster0-shard-00-00.ovdf2.mongodb.net:27017,cluster0-shard-00-01.ovdf2.mongodb.net:27017,cluster0-shard-00-02.ovdf2.mongodb.net:27017?ssl=true&replicaSet=atlas-siquxg-shard-0&authSource=admin&retryWrites=true&w=majority");
            var database = client.GetDatabase("sample_training");

            var collection = database.GetCollection<BsonDocument>("grades");

            // await CreateDocument(collection);

            // await CreateDocumentAsync(collection);

            // await UpdateDocumentAsync(collection);

            await DeleteDocumentAsync(collection);

        }

        static async Task ReadDocumentAsync(IMongoCollection<BsonDocument> collection)
        {
            // Get all documents
            // var firstDocument = (await collection.FindAsync(new BsonDocument())).FirstOrDefault();

            // Get by student id

            // Raw BsonDocument
            /*var filter = new BsonDocument {{ "student_id", 10000 }};*/

            // Bson using builder
            /*Builders<BsonDocument>.Filter.Eq("student_id", 10000);*/

            // Complex query using a combination of builder and BsonDocument and filtering array elements
            var highExamScoreFilter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>(
                "scores", new BsonDocument { { "type", "exam" }, { "score", new BsonDocument { { "$gte", 95 } } }
                });

            /*var studentDocument = (await collection.FindAsync(highExamScoreFilter)).FirstOrDefault();*/

            // Create a sort BsonDocument
            var sort = Builders<BsonDocument>.Sort.Descending("student_id");

            // Using cursor
            var cursor = await collection.Find(highExamScoreFilter).Sort(sort).ToCursorAsync();
            foreach (var document in cursor.ToEnumerable())
            {
                Console.WriteLine(document);
            }

        }

        static async Task CreateDocumentAsync(IMongoCollection<BsonDocument> collection)
        {
            // Creating a sample BsonDocument
            var document = new BsonDocument { { "student_id", 10000 }, {
                    "scores",
                    new BsonArray {
                        new BsonDocument { { "type", "exam" }, { "score", 88.12334193287023 } },
                        new BsonDocument { { "type", "quiz" }, { "score", 74.92381029342834 } },
                        new BsonDocument { { "type", "homework" }, { "score", 89.97929384290324 } },
                        new BsonDocument { { "type", "homework" }, { "score", 82.12931030513218 } }
                    }
                }, { "class_id", 480 }
            };

            await collection.InsertOneAsync(document);
        }

        static async Task UpdateDocumentAsync(IMongoCollection<BsonDocument> collection)
        {
            // Let first target the document to update
            /*var filter = Builders<BsonDocument>.Filter.Eq("student_id", 10000);*/

            // Create an update BsonDocument
            /*var update = Builders<BsonDocument>.Update.Set("class_id", 483);*/

            var arrayFilter = Builders<BsonDocument>.Filter.Eq("student_id", 10000)
                              & Builders<BsonDocument>.Filter.Eq("scores.type", "quiz");

            var arrayUpdate = Builders<BsonDocument>.Update.Set("scores.$.score", 89.97381029342834);

            await collection.UpdateOneAsync(arrayFilter, arrayUpdate);
        }

        static async Task DeleteDocumentAsync(IMongoCollection<BsonDocument> collection)
        {
            // Single delete
           /* var deleteFilter = Builders<BsonDocument>.Filter.Eq("student_id", 10000);

            await collection.DeleteOneAsync(deleteFilter);*/

           // Multiple delete
           var deleteLowExamFilter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>("scores",
                new BsonDocument { { "type", "exam" }, {"score", new BsonDocument { { "$lt", 60 }}}
                });

            await collection.DeleteManyAsync(deleteLowExamFilter);
        }
    }
}
