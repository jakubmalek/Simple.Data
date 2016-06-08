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
        private static readonly Regex ColumnAliasMatch = new Regex(@"^(.*)\sAS(.*)?", RegexOptions.IgnoreCase);

        public IEnumerable<string> ApplyLimit(string sql, int take)
        {
            yield return SelectMatch.Replace(sql, match => match.Value + " TOP " + take + " ");
        }

        public IEnumerable<string> ApplyPaging(string sql, string[] keys, int skip, int take)
        {
            var queryParts = Regex.Split(sql, "\\s");
            var result = new StringBuilder();
            var pagingOrderBy = new StringBuilder("ORDER BY ");
            var columns = new StringBuilder();
            string expectedOverByStartingWord = null;
            var orderBystartIndex = -1;
            var columnsDefinitionStarted = false;
            for (var i = 0; i < queryParts.Length; i++)
            {
                var part = queryParts[i].Trim();
                if (part.Length == 0)
                {
                    continue;
                }
                if ("FROM".Equals(part, StringComparison.InvariantCultureIgnoreCase))
                {
                    columnsDefinitionStarted = false;
                }
                if (columnsDefinitionStarted)
                {
                    columns.Append(part + " ");
                }
                if ("SELECT".Equals(part, StringComparison.InvariantCultureIgnoreCase) && columns.Length == 0)
                {
                    columnsDefinitionStarted = true;
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
                if (result.Length > 0)
                {
                    result.Append(' ');
                }
                result.Append(part);
            }
            var columnsList = ListColumnsToSelect(columns.ToString());
            result.Append(string.Format(") SELECT {0} FROM __Data WHERE [_#_] BETWEEN {1} AND {2}", columnsList, skip + 1, skip + take));
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
            yield return "WITH __Data AS (" + string.Format(result.ToString(), pagingOrderBy);
        }

        private static string ListColumnsToSelect(string columnsSql)
        {
            if (string.IsNullOrEmpty(columnsSql))
            {
                return "*";
            }
            var result = new StringBuilder();
            foreach (var column in columnsSql.Split(','))
            {
                var selectColumnDefinition = column.Trim();
                var columnWithAliasMatch = ColumnAliasMatch.Match(selectColumnDefinition);
                if (columnWithAliasMatch.Success && columnWithAliasMatch.Groups.Count > 2)
                {
                    var alias = columnWithAliasMatch.Groups[2];
                    result.Append(alias + ", ");    
                }
                else
                {
                    result.Append(selectColumnDefinition + ", ");    
                }
            }
            result.Length = result.Length - 2;
            return result.ToString();
        }
    }
}
