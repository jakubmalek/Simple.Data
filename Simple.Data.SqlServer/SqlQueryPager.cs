using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Text;
using System.Text.RegularExpressions;
using Simple.Data.Ado;

namespace Simple.Data.SqlServer
{
    [Export(typeof(IQueryPager))]
    public class SqlQueryPager : IQueryPager
    {
        private static readonly Regex SelectMatch = new Regex(@"^SELECT\s*(DISTINCT)?", RegexOptions.IgnoreCase);

        public IEnumerable<string> ApplyLimit(string sql, int take)
        {
            yield return SelectMatch.Replace(sql, match => match.Value + " TOP " + take + " ");
        }

        public IEnumerable<string> ApplyPaging(string sql, string[] keys, int skip, int take)
        {
            var queryParts = Regex.Split(sql, "\\s");
            var pagingOrderBy = new StringBuilder("ORDER BY ");
            var result = new StringBuilder("WITH __Data AS (");
            string expectedOverByStartingWord = null;
            var orderBystartIndex = -1;
            for (var i = 0; i < queryParts.Length; i++)
            {
                var part = queryParts[i].Trim();
                if (part.Length == 0)
                {
                    continue;
                }
                if ("BY".Equals(part, StringComparison.InvariantCultureIgnoreCase) && expectedOverByStartingWord != null)
                {
                    orderBystartIndex = i + 1;
                    break;
                }
                if ("ORDER".Equals(part, StringComparison.InvariantCultureIgnoreCase))
                {
                    expectedOverByStartingWord = part;
                    continue;
                }
                if (expectedOverByStartingWord != null)
                {
                    result.Append(" " + expectedOverByStartingWord);
                    expectedOverByStartingWord = null;
                }
                if ("FROM".Equals(part, StringComparison.InvariantCultureIgnoreCase))
                {
                    result.Append(", ROW_NUMBER() OVER ({0}) AS [_#_] ");
                }
                result.Append(part + " ");
            }
            result.Append(string.Format(") SELECT * FROM __Data WHERE [_#_] BETWEEN {0} AND {1} ORDER BY [_#_]", skip + 1, skip + take));
            if (orderBystartIndex >= 0 && orderBystartIndex < queryParts.Length)
            {
                for (var i = orderBystartIndex; i < queryParts.Length; i++)
                {
                    if (queryParts[i].Length > 0)
                    {
                        pagingOrderBy.Append(queryParts[i] + " ");    
                    }
                }
            }
            else
            {
                if (keys == null || keys.Length == 0)
                {
                    throw new AdoAdapterException("Cannot apply paging to table with no primary key.");
                }
                pagingOrderBy.Append(string.Join(", ", keys));
            }
            yield return string.Format(result.ToString(), pagingOrderBy);
        }
    }
}
