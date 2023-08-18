using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Tasks;
using App.Metrics.Logging;
using Couchbase;
using Couchbase.KeyValue;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NET_App.Workloads;

using Serilog;
using ILogger = Serilog.ILogger;
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
        var log = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.File("Logs/log.txt", fileSizeLimitBytes: 25 * 1024 * 1024, rollOnFileSizeLimit: true, rollingInterval: RollingInterval.Minute)
            .CreateLogger();
        
        IServiceCollection serviceCollection = new ServiceCollection();
        serviceCollection.AddLogging(builder => builder
                .AddFilter(level => level >= LogLevel.Debug)
        ); 
        var loggerFactory = serviceCollection.BuildServiceProvider().GetService<ILoggerFactory>();
        loggerFactory.AddSerilog(log);

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

        var runKv = testSettings.workloads.ToLower().Contains("kv");
        var runQuery = testSettings.workloads.ToLower().Contains("query");
        
        Enum.TryParse(testSettings.operation, out KeyValueWorkload.KvOperation operation);
        var kvWl = new KeyValueWorkload(collections.ToArray(), operation);
        var queryWl = new QueryWorkload(cluster, collections.ToArray());
        
        CancellationTokenSource sourceToken = new CancellationTokenSource();
        sourceToken.CancelAfter(TimeSpan.FromMinutes(_hoursToRun));
        CancellationToken cancellationToken = sourceToken.Token;

        int docsPerThread = (testSettings.numDocs / threads);

        var tasks = new List<Task>();

        if (runKv)
        {
            Console.WriteLine("Running KV workloads");
            for (var i = 0; i < threads; i++)
            {
                tasks.Add(kvWl.StartAsync(docsPerThread, testSettings.docSize, i*docsPerThread, testSettings.runForTime, cancellationToken));
            }
            tasks.Add(kvWl.PrintResultsPeriodically(cancellationToken));
        }

        if (runQuery)
        {
            Console.WriteLine("Running Query workloads");
            if (testSettings.createIndexes)
            {
                await queryWl.BuildIndexes();
            }
            tasks.Add(queryWl.StartAsync(cancellationToken));
            tasks.Add(queryWl.PrintResultsPeriodically(cancellationToken));
        }

        await Task.WhenAll(tasks);

        if (runKv)
        {
            kvWl.PrintResults();
        }

        if (runQuery)
        {
            queryWl.PrintResults();
        }
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
        public string workloads { get; set; }
        public bool createIndexes { get; set; }



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
