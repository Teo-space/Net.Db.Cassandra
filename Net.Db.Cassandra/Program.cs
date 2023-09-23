using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using System.Collections.Generic;
/*
docker run --name cassandra -p 127.0.0.1:9042:9042 -p 127.0.0.1:9160:9160   -d cassandra -e CASSANDRA_USER=user -e CASSANDRA_PASSWORD=password

docker run --name cassandra -p 127.0.0.1:9042:9042 -p 127.0.0.1:9160:9160   -d cassandra 


*/

print("Hello Cassandra!");

var cluster = Cluster.Builder()
.AddContactPoints("127.0.0.1")
.WithPort(9042)
//.WithLoadBalancingPolicy(new DCAwareRoundRobinPolicy("<Data Centre (e.g AWS_VPC_US_EAST_1)>"))
.WithAuthProvider(new PlainTextAuthProvider("user", "password"))
.Build();


var session = cluster.Connect();
print($"Connected to cluster: {cluster.Metadata.ClusterName}", ConsoleColor.Green);

{
    session.Execute(
@"
CREATE  KEYSPACE IF NOT EXISTS TESTS
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } 
");

    var keyspaceNames = session
                    .Execute("SELECT * FROM system_schema.keyspaces")
                    .Select(row => row.GetValue<string>("keyspace_name"))
                    ;
    foreach (var keyspace in keyspaceNames)
    {
        print(keyspace, ConsoleColor.Cyan);
    }
}

MappingConfiguration.Global
.Define(
   new Map<Article>()
      .KeyspaceName("TESTS")
      .TableName("Articles")
      .PartitionKey(x => x.ArticleId)
      .ClusteringKey(x => x.ArticleVersionId)
      .Column(x => x.ArticleId)
      .Column(x => x.ArticleVersionId)
      .Column(x => x.Name)
      .Column(x => x.Description)
);

var table = new Table<Article>(session);
table.CreateIfNotExists();


Insert(1);
Count();
Select(1);
Update(1);
Select(1);









void Insert(long VersionId = 0)
{
    print("Insert", ConsoleColor.DarkMagenta);
    var insert = table.Insert(new Article(0, VersionId, $"Test Article {VersionId}", "Descripion"));
    insert.Execute();
    print("Ok");
}
void Update(long VersionId = 0)
{
    print("update", ConsoleColor.DarkMagenta);
    var update = table.Insert(new Article(0, VersionId, $"Test Article {VersionId} update", "Descripion"));
    update.Execute();
    print("Ok");
}
void Count()
{
    print("Where", ConsoleColor.DarkMagenta);
    var elements = table.Where(x => x.ArticleId == 0);
    var result = elements.Execute();
    print("Ok");
    print($"result.Count() {result.Count()}");
}
void Select(long VersionId = 0)
{
    print("FirstOrDefault", ConsoleColor.DarkMagenta);
    var cqlQuerySingleElement = table.FirstOrDefault(x => x.ArticleId == 0 && x.ArticleVersionId == VersionId);
    var result = cqlQuerySingleElement.Execute();
    print("Ok");
    print(result);
}
