using System.Collections.Concurrent;
using System.Data.Common;
using Couchbase;
using Couchbase.KeyValue;
using Couchbase.Management.Query;
using Couchbase.Query;
using Microsoft.Extensions.Logging;
using NET_App.Models;

namespace NET_App.Workloads;

/**
 * WIP - Do not use 
 */
class QueryWorkload
{
    private ICluster _cluster;
    private ICouchbaseCollection[] _collections;
    private int _collectionsLength;

    private ConcurrentDictionary<string, int> results = new();

    public ConcurrentDictionary<string, int> Results => results;

    private static string[] queries =
    {
        "select x.* from `{0}`.`{1}`.`{2}` x limit 3000;",
        "select meta().id, firstName, lastName, email, age, address from `{0}`.`{1}`.`{2}` where age between 0 and 50 limit 3000;",
        "select firstName, lastName, attributes, address from `{0}`.`{1}`.`{2}` where attributes.hair.type=\"wavy\" limit 3000;",
        "select age, count(*) from `{0}`.`{1}`.`{2}` where maritalStatus='Single' group by age order by age limit 3000;",
        "select firstName, gender, address.city, hobby from `{0}`.`{1}`.`{2}` where gender=\"feminine\" limit 3000;",
        "select age, maritalStatus from `{0}`.`{1}`.`{2}` where hobbies is not null limit 3000",
        "select gender, count(*) from `{0}`.`{1}`.`{2}` group by gender order by gender limit 3000;",
        "select firstName, email, attributes from `{0}`.`{1}`.`{2}` where attributes.weight between 100 and 150 and attributes.height between 150 and 250 limit 3000;",
    };

    private static string[] indexes =
    {
        "CREATE PRIMARY INDEX ON `{0}`.`{1}`.`{2}`;",
        "CREATE INDEX ix_name ON `{0}`.`{1}`.`{2}` (firstName);",
        "CREATE INDEX ix_email ON `{0}`.`{1}`.`{2}` (email);",
        "CREATE INDEX ix_age_over_age on `{0}`.`{1}`.`{2}`(age) where age between 30 and 50;",
        "CREATE INDEX ix_age_over_firstName on `{0}`.`{1}`.`{2}`(firstName) where age between 0 and 50;",
        "CREATE INDEX ix_age_marital on `{0}`.`{1}`.`{2}`(marital,age) USING GSI;",
        "CREATE INDEX ix_gender_address_city_hobby on `{0}`.`{1}`.`{2}`(Gender,address.city, DISTINCT ARRAY hobby FOR hobby in Hobbies END) where Gender=\"feminine\";",
        "BUILD INDEX ON `{0}`.`{1}`.`{2}`(`#primary`, `ix_name`, `ix_email`, `ix_age_over_age`, `ix_age_over_firstName`, `ix_age_marital`)"
    };

    public QueryWorkload(ICluster cluster, ICouchbaseCollection[] collections)
    {
        _cluster = cluster;
        _collections = collections;
        _collectionsLength = collections.Length;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var batchSize = 5;
        var startBatch = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            var tasks = new List<Task>();
            foreach (var collection in _collections)
            {
                var queryLength = queries.Length;
                for (var i = 0; i < queryLength; i++)
                {
                    var task = _cluster.QueryAsync<dynamic>(String.Format(queries[i],
                        collection.Scope.Bucket.Name, collection.Scope.Name, collection.Name));
                    tasks.Add(task);
                }
            }
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception e)
            {
                results.AddOrUpdate(e.GetType().ToString(), 1, (k, existingValue) => existingValue + 1);
            }

            foreach (var task in tasks)
            {
                if (task.IsCompletedSuccessfully)
                {
                    results.AddOrUpdate("success", 1, (k, existingValue) => existingValue + 1);
                }
                else
                {
                    results.AddOrUpdate(task.Exception.InnerExceptions.GetType().ToString(), 1,
                        (k, existingValue) => existingValue + 1);
                }
            }
        }
    }

    public async Task BuildIndexes()
    {
        foreach (var collection in _collections)
        {
            var indexLength = indexes.Length;
            for (var i = 0; i < indexLength; i++)
            {
                try
                {
                    await _cluster.QueryAsync<dynamic>(String.Format(indexes[i],
                        collection.Scope.Bucket.Name, collection.Scope.Name, collection.Name));
                }
                catch (Exception e)
                {
                    Console.WriteLine("Got error building indexes: " + e.Message);
                }
            }
        }
    }
    
    public async Task PrintResultsPeriodically(CancellationToken cancellationToken)
    {
        var timer = new PeriodicTimer(TimeSpan.FromMinutes(1));

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                PrintResults();
            }
        }
        catch(OperationCanceledException)
        {
            
        }

    }
    public void PrintResults()
    {
        Console.WriteLine(DateTime.Now.ToString("t") + " Query Results:");
        results.Select(i => $"{i.Key}: {i.Value}").ToList().ForEach(Console.WriteLine);
    }
}