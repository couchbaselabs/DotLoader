using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Tasks;
using App.Metrics.Logging;
using Couchbase;
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
            .AddFilter(level => level >= LogLevel.Trace)
        );
        using ILoggerFactory loggerFactory =
            LoggerFactory.Create(builder =>
                builder.AddConsole(options =>
                {
                    options.IncludeScopes = true;
                    options.TimestampFormat = "hh:mm:ss ";
                }));
        _logger = loggerFactory.CreateLogger("workload");


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

        var bucket = await cluster.BucketAsync(testSettings.bucket);
        var scope = await bucket.DefaultScopeAsync();
        var collection = await bucket.DefaultCollectionAsync();

        Enum.TryParse(testSettings.operation, out KeyValueWorkload.KvOperation operation);
        var kvWl = new KeyValueWorkload(cluster, bucket, scope, collection, _logger, operation);
        
        CancellationTokenSource sourceToken = new CancellationTokenSource();
        sourceToken.CancelAfter(TimeSpan.FromMinutes(_hoursToRun));
        CancellationToken cancellationToken = sourceToken.Token;

        int docsPerThread = (testSettings.numDocs / threads);

        var tasks = new List<Task>();

        for (var i = 0; i < threads; i++)
        {
            tasks.Add(kvWl.StartAsync(docsPerThread, testSettings.docSize, i*docsPerThread, testSettings.runForTime, cancellationToken));
        }
        await Task.WhenAll(tasks);

        PrintResults(kvWl.Results);
    }

    private static void PrintResults(ConcurrentDictionary<string, int> dict)
    {
        Console.WriteLine("Operation Results:");
        dict.Select(i => $"{i.Key}: {i.Value}").ToList().ForEach(Console.WriteLine);
    }
    
    public static class JsonFileReader
    {
        public static async Task<T> ReadAsync<T>(string filePath)
        {
            using FileStream stream = File.OpenRead(filePath);
            return await JsonSerializer.DeserializeAsync<T>(stream);
        }
    }
    public class Settings
    {
        public string connectionString  {get; set;}
        public string username {get; set;}
        public string password {get; set;}
        public string bucket {get; set;}
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
    
}
