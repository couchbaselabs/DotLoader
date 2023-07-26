using Couchbase;
using Couchbase.Query;
using Microsoft.Extensions.Logging;

namespace NET_App.Workloads;

/**
 * WIP - Do not use 
 */
class QueryWorkload
{
    private ICluster _cluster;
    private ILogger _logger;

    public QueryWorkload(ICluster cluster, ILogger logger)
    {
        _cluster = cluster;
        _logger = logger;
    }
    
    public async Task StartAsync(string query, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting query tests");
        
        var random = new Random();
        
        //await Task.Delay(random.Next(500, 2000), cancellationToken);
        
        while (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation("Querying?");

            try
            {
                _logger.LogInformation("try?");

                var qResult = await _cluster.QueryAsync<Object>(query);
                _logger.LogInformation("queired?");


                await foreach (var qResultRow in qResult.Rows)
                {
                    Console.WriteLine(qResultRow);
                }

                if (qResult.MetaData?.Status == QueryStatus.Success)
                {
                    Console.WriteLine("hello?");

                    // var res = await qResult.Rows.FirstOrDefaultAsync();
                    _logger.LogInformation($"Query Success, Id: [TEST]");
                }
                else
                {
                    Console.WriteLine("wtf");

                    _logger.LogWarning($"ERROR! Query Error: {qResult.MetaData?.Status}, {qResult.Errors.Count}");
                }
            }
            catch (CouchbaseException e)
            {
                Console.WriteLine("exception");

                _logger.LogWarning(e, "ERROR! Query Exception");
            }
            catch (Exception e)
            {
                Console.WriteLine("bad");

            }
            finally
            {
                Console.WriteLine("finally");

                //await Task.Delay(random.Next(20_0, 30_0), cancellationToken);
            }
        }
        Console.WriteLine("ended");
    }
}

// while (true)
// {
//             
//     var qResult = await cluster.QueryAsync<Object>("select meta().id from default0._default.VolumeCollection0 where country is not null and `type` is not null limit 100;");
//     await foreach (var qResultRow in qResult.Rows)
//     {
//         Console.WriteLine(qResultRow.ToString());
//     }
// }
//
//
//
//
// var logger1 = loggerFactory.CreateLogger("query-1");
// var logger2 = loggerFactory.CreateLogger("query-2");
// var logger3 = loggerFactory.CreateLogger("query-3");
//
// var queryTest1 = new QueryTest(cluster, logger1);
// var queryTest2 = new QueryTest(cluster, logger2);
// var queryTest3 = new QueryTest(cluster, logger3);
//
//
// // CancellationTokenSource sourceToken = new CancellationTokenSource();//(_housrToRun * 60 * 60 *1000).Token;
// // sourceToken.CancelAfter(TimeSpan.FromSeconds(30));
// // CancellationToken cancellationToken = sourceToken.Token;
//
// var first = Task.Run(() =>
// {
//     return queryTest1.StartAsync("select * from default limit 10;", cancellationToken).ConfigureAwait(true);
// });
// var second = Task.Run(() =>
// {
//     return queryTest2.StartAsync("select * from default limit 1;", cancellationToken).ConfigureAwait(true);
// });
// var third = Task.Run(() =>
// {
//     return queryTest3.StartAsync("select * from default limit 1;", cancellationToken).ConfigureAwait(true);
// });
//
// await Task.WhenAll(first, second, third);
// _logger.LogInformation("Donezo");