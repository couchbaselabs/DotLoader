using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Tasks;
using App.Metrics.Logging;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Query;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NET_App.Workloads;
using Newtonsoft.Json.Linq;
using ILoggerFactory = Microsoft.Extensions.Logging.ILoggerFactory;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;


await new WorkloadMain().Main();

class WorkloadMain
{
    private ICluster cluster;
    private ILogger _logger;
    private double _hoursToRun = 1;
    private const int threads = 4;

    public async Task Main()
    {
        IServiceCollection serviceCollection = new ServiceCollection();
        serviceCollection.AddLogging(builder => builder
            .AddFilter(level => level >= LogLevel.Debug)
        );
        var loggerFactory = serviceCollection.BuildServiceProvider().GetService<ILoggerFactory>();
        loggerFactory.AddFile("Logs/DotLoader-{Date}.txt", LogLevel.Debug);


        var testSettings = await JsonFileReader.ReadAsync<Settings>("../../../config.json");
        _hoursToRun = testSettings.runTimeMins;

        var options = new ClusterOptions
        {
            UserName = testSettings.username,
            Password = testSettings.password,
            HttpIgnoreRemoteCertificateMismatch = true,
            KvIgnoreRemoteCertificateNameMismatch = true,
            Logging = loggerFactory
        };
        options.ApplyProfile("wan-development");

        cluster = await Cluster.ConnectAsync(
            testSettings.connectionString,
            options
        );

        await cluster.WaitUntilReadyAsync(TimeSpan.FromSeconds(10));

        List<ICouchbaseCollection> collections = new List<ICouchbaseCollection>();

        foreach (var b in testSettings.buckets)
        {
            var bucketObj = await cluster.BucketAsync(b.bucketName);
            var scopeObj = await bucketObj.ScopeAsync(b.scope);
            foreach (var c in b.collections)
            {           
                var collectionObj = await scopeObj.CollectionAsync(c);
                collections.Add(collectionObj);
            }
        }

        Enum.TryParse(testSettings.operation, out KeyValueWorkload.KvOperation operation);
        var kvWl = new KeyValueWorkload(collections.ToArray(), operation);
        
        CancellationTokenSource sourceToken = new CancellationTokenSource();
        sourceToken.CancelAfter(TimeSpan.FromMinutes(_hoursToRun));
        CancellationToken cancellationToken = sourceToken.Token;

        int docsPerThread = (testSettings.numDocs / threads);

        var tasks = new List<Task>();

        for (var i = 0; i < threads; i++)
        {
            tasks.Add(kvWl.StartAsync(docsPerThread, testSettings.docSize, i*docsPerThread, testSettings.runForTime, cancellationToken));
        }
        tasks.Add(kvWl.PrintResultsPeriodically(cancellationToken));
        await Task.WhenAll(tasks);

        kvWl.PrintResults();
    }

    private static class JsonFileReader
    {
        public static async Task<T> ReadAsync<T>(string filePath)
        {
            using FileStream stream = File.OpenRead(filePath);
            return await JsonSerializer.DeserializeAsync<T>(stream);
        }
    }
    private class Settings
    {
        public string connectionString  {get; set;}
        public string username {get; set;}
        public string password {get; set;}
        public Bucket[] buckets {get; set;}

        //Type of (KV) operation to perform
        public string operation { get; set; }
        //Total number of docs to act upon
        public int numDocs { get; set; }
        //Size (length) of the document
        public int docSize { get; set; } 
        //Run operations continuously for time, or just do the operations for numDocs
        public bool runForTime { get; set; }
        //Time in hours to run for if the runForTime is set
        public double runTimeMins { get; set; }
    }

    private class Bucket
    {
        public string bucketName {get; set;}
        public string scope {get; set;}
        public string[] collections {get; set;}
    }
    
}
