using System.Linq;
using System.Text.RegularExpressions;
using NUnit.Framework;
using Simple.Data.Ado;
using Simple.Data.SqlServer;

namespace Simple.Data.SqlTest
{
    [TestFixture]
    public class SqlQueryPagerTest
    {
        static readonly Regex Normalize = new Regex(@"\s+", RegexOptions.Multiline);

        [Test]
        public void ShouldApplyLimitUsingTop()
        {
            var sql = "select a,b,c from d where a = 1 order by c";
            var expected = new[] { "select top 5 a,b,c from d where a = 1 order by c" };

            var pagedSql = new SqlQueryPager().ApplyLimit(sql, 5);
            var modified = pagedSql.Select(x => Normalize.Replace(x, " ").ToLowerInvariant());

            Assert.IsTrue(expected.SequenceEqual(modified));
        }

        [Test]
        public void ShouldApplyLimitUsingTopWithDistinct()
        {
            var sql = "select distinct a,b,c from d where a = 1 order by c";
            var expected = new[] { "select distinct top 5 a,b,c from d where a = 1 order by c" };

            var pagedSql = new SqlQueryPager().ApplyLimit(sql, 5);
            var modified = pagedSql.Select(x => Normalize.Replace(x, " ").ToLowerInvariant());

            Assert.IsTrue(expected.SequenceEqual(modified));
        }

        [Test]
        public void ShouldApplyPagingUsingOrderBy()
        {
            var sql = "select [dbo].[d].[a],[dbo].[d].[b],[dbo].[d].[c] from [dbo].[d] where [dbo].[d].[a] = 1 order by [dbo].[d].[c]";
            var expected = new[]{
                "with __data as (" +
                    "select " +
                        "[dbo].[d].[a]," +
                        "[dbo].[d].[b]," +
                        "[dbo].[d].[c], " +
                        "row_number() over (" +
                            "order by [dbo].[d].[c] " +
                        ") as [_#_] " +
                    "from [dbo].[d] " +
                    "where [dbo].[d].[a] = 1) " +
                "select [dbo].[d].[a], [dbo].[d].[b], [dbo].[d].[c] " +
                "from __data where [_#_] between 6 and 15"};

            var pagedSql = new SqlQueryPager().ApplyPaging(sql, new[] {"[dbo].[d].[a]"}, 5, 10);
            var modified = pagedSql.Select(x => Normalize.Replace(x, " ").ToLowerInvariant()).ToArray();

            Assert.AreEqual(expected[0], modified[0]);
        }

        [Test]
        public void ShouldApplyPagingUsingOrderByKeysIfNotAlreadyOrdered()
        {
            var sql = "select [dbo].[d].[a],[dbo].[d].[b],[dbo].[d].[c] from [dbo].[d] where [dbo].[d].[a] = 1";
            var expected = new[]{
                "with __data as (" +
                    "select " +
                        "[dbo].[d].[a]," +
                        "[dbo].[d].[b]," +
                        "[dbo].[d].[c], " +
                        "row_number() over (" +
                            "order by [dbo].[d].[a]" +
                        ") as [_#_] " +
                    "from [dbo].[d] " +
                    "where [dbo].[d].[a] = 1) " +
                "select [dbo].[d].[a], [dbo].[d].[b], [dbo].[d].[c] " +
                "from __data where [_#_] between 11 and 30"};

            var pagedSql = new SqlQueryPager().ApplyPaging(sql, new[] {"[dbo].[d].[a]"}, 10, 20);
            var modified = pagedSql.Select(x => Normalize.Replace(x, " ").ToLowerInvariant()).ToArray();

            Assert.AreEqual(expected[0], modified[0]);
        }

        [Test]
        public void ShouldCopeWithAliasedColumns()
        {
            var sql = "select [dbo].[d].[a],[dbo].[d].[b] as [foo],[dbo].[d].[c] from [dbo].[d] where [dbo].[d].[a] = 1";
            var expected =new[]{
                "with __data as (" +
                    "select " +
                        "[dbo].[d].[a]," +
                        "[dbo].[d].[b] as [foo],[dbo].[d].[c], " +
                        "row_number() over (" +
                            "order by [dbo].[d].[a]" +
                        ") as [_#_] " +
                    "from [dbo].[d] " +
                    "where [dbo].[d].[a] = 1) " +
                "select [dbo].[d].[a], [foo], [dbo].[d].[c] " +
                "from __data where [_#_] between 21 and 25"};

            var pagedSql = new SqlQueryPager().ApplyPaging(sql, new[]{"[dbo].[d].[a]"}, 20, 5);
            var modified = pagedSql.Select(x => Normalize.Replace(x, " ").ToLowerInvariant()).ToArray();

            Assert.AreEqual(expected[0], modified[0]);
        }

        [Test]
        public void ShouldCopeWithColumnsThatEndInFrom()
        {
            var sql = @"SELECT [dbo].[PromoPosts].[Id],[dbo].[PromoPosts].[ActiveFrom],[dbo].[PromoPosts].[ActiveTo],[dbo].[PromoPosts].[Created],[dbo].[PromoPosts].[Updated] 
    from [dbo].[PromoPosts] 
    ORDER BY [dbo].[PromoPosts].[ActiveFrom]";

            var expected = "WITH __Data AS (" +
                               "SELECT " +
                                    "[dbo].[PromoPosts].[Id]," +
                                    "[dbo].[PromoPosts].[ActiveFrom]," +
                                    "[dbo].[PromoPosts].[ActiveTo]," +
                                    "[dbo].[PromoPosts].[Created]," +
                                    "[dbo].[PromoPosts].[Updated], " +
                                    "ROW_NUMBER() OVER (" +
                                        "ORDER BY [dbo].[PromoPosts].[ActiveFrom] " +
                                    ") AS [_#_] " +
                                "from [dbo].[PromoPosts]) " +
                           "SELECT [dbo].[PromoPosts].[Id], [dbo].[PromoPosts].[ActiveFrom], [dbo].[PromoPosts].[ActiveTo], [dbo].[PromoPosts].[Created], [dbo].[PromoPosts].[Updated] " +
                           "FROM __Data WHERE [_#_] BETWEEN 1 AND 25";
            expected = expected.ToLowerInvariant();
            

            var pagedSql = new SqlQueryPager().ApplyPaging(sql, new[] {"[dbo].[PromoPosts].[Id]"}, 0, 25).Single();
            var modified = Normalize.Replace(pagedSql, " ").ToLowerInvariant();
            
            Assert.AreEqual(expected, modified);
        }

        [Test]
        public void ShouldExcludeLeftJoinedTablesFromSubSelect()
        {
            var sql = @"SELECT [dbo].[MainClass].[ID],
    [dbo].[MainClass].[SomeProperty],
    [dbo].[MainClass].[SomeProperty2],
    [dbo].[MainClass].[SomeProperty3],
    [dbo].[MainClass].[SomeProperty4],
    [dbo].[ChildClass].[ID] AS [__withn__ChildClass__ID],
    [dbo].[ChildClass].[SomeProperty] AS [__withn__ChildClass__SomeProperty],
    [dbo].[ChildClass].[SomeProperty2] AS [__withn__ChildClass__SomeProperty2] FROM [dbo].[MainClass] LEFT JOIN [dbo].[JoinTable] ON ([dbo].[MainClass].[ID] = [dbo].[JoinTable].[MainClassID]) LEFT JOIN [dbo].[ChildClass] ON ([dbo].[ChildClass].[ID] = [dbo].[JoinTable].[ChildClassID]) WHERE ([dbo].[MainClass].[SomeProperty] > @p1 AND [dbo].[MainClass].[SomeProperty] <= @p2)";

            var expected = "with __data as (" +
                               "select " +
                                   "[dbo].[mainclass].[id], " +
                                   "[dbo].[mainclass].[someproperty], " +
                                   "[dbo].[mainclass].[someproperty2], " +
                                   "[dbo].[mainclass].[someproperty3], " +
                                   "[dbo].[mainclass].[someproperty4], " +
                                   "[dbo].[childclass].[id] as [__withn__childclass__id], " +
                                   "[dbo].[childclass].[someproperty] as [__withn__childclass__someproperty], " +
                                   "[dbo].[childclass].[someproperty2] as [__withn__childclass__someproperty2], " +
                                   "row_number() over (" +
                                       "order by [dbo].[promoposts].[id]" +
                                   ") as [_#_] " +
                               "from [dbo].[mainclass] " +
                               "left join [dbo].[jointable] on ([dbo].[mainclass].[id] = [dbo].[jointable].[mainclassid]) " +
                               "left join [dbo].[childclass] on ([dbo].[childclass].[id] = [dbo].[jointable].[childclassid]) " +
                               "where ([dbo].[mainclass].[someproperty] > @p1 and [dbo].[mainclass].[someproperty] <= @p2)) " +
                           "select " +
                                "[dbo].[mainclass].[id], " +
                                "[dbo].[mainclass].[someproperty], " +
                                "[dbo].[mainclass].[someproperty2], " +
                                "[dbo].[mainclass].[someproperty3], " +
                                "[dbo].[mainclass].[someproperty4], " +
                                "[__withn__childclass__id], " +
                                "[__withn__childclass__someproperty], " +
                                "[__withn__childclass__someproperty2] " +
                           "from __data where [_#_] between 1 and 25";

            var pagedSql = new SqlQueryPager().ApplyPaging(sql, new[] {"[dbo].[PromoPosts].[Id]"}, 0, 25).Single();
            var modified = Normalize.Replace(pagedSql, " ").ToLowerInvariant();
            Assert.AreEqual(expected, modified);
        }

        [Test]
        public void ShouldThrowIfTableHasNoPrimaryKey([Values(null, new string[0])]string[] keys)
        {
            var sql = "select [dbo].[d].[a] from [dbo].[b]";

            Assert.Throws<AdoAdapterException>(
                () => new SqlQueryPager().ApplyPaging(sql, keys, 5, 10).ToList()
            );
        }

        [Test]
        public void ShouldApplyPagingWithOrderByColumnsFromJoinedTables()
        {
            // given
            const string sql =
                @"SELECT 
                    [dbo].[MainTable].[id], 
                    [dbo].[MainTable].[name],
                    [dbo].[JoinedTable].[id] AS [__withn__JoinedTable__ID]  
                FROM [dbo].[MainTable] 
                INNER JOIN [dbo].[JoinedTable] ON [dbo].[MainTable].[id] = [dbo].[JoinedTable].[joinId] 
                WHERE [dbo].[MainTable].[value] > 21
                ORDER BY [dbo].[JoinedTable].[a] DESC, [dbo].[MainTable].[id] ASC";
            var tested = new SqlQueryPager();
            var keys = new [] {"[dbo].[Table1].[a]"};
            const int skip = 6;
            const int take = 2;

            // when
            var pagedSql = tested.ApplyPaging(sql, keys, skip, take)
                .Single();

            // then
            var normalized = Normalize.Replace(pagedSql, " ").ToLowerInvariant();
            const string expected =
                "with __data as (" +
                    "select " +
                        "[dbo].[maintable].[id], " +
                        "[dbo].[maintable].[name], " +
                        "[dbo].[joinedtable].[id] as [__withn__joinedtable__id], " +
                        "row_number() over (" +
                            "order by [dbo].[joinedtable].[a] desc, " +
                            "[dbo].[maintable].[id] asc " +
                        ") as [_#_] " +
                    "from [dbo].[maintable] " +
                    "inner join [dbo].[joinedtable] on [dbo].[maintable].[id] = [dbo].[joinedtable].[joinid] " +
                    "where [dbo].[maintable].[value] > 21) " +
                "select [dbo].[maintable].[id], [dbo].[maintable].[name], [__withn__joinedtable__id] " +
                "from __data where [_#_] between 7 and 8";
            Assert.AreEqual(expected, normalized);
        }
    }
}
