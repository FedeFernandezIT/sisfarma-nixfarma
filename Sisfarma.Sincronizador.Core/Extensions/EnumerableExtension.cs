using System.Collections.Generic;

namespace System.Linq
{
    public static class EnumerableExtension
    {
        public static IEnumerable<TSource> TakeLast<TSource>(this IEnumerable<TSource> source, int count)
        {
            var taked = new List<TSource>();
            for (int i = source.Count(); i > source.Count() - count; i--)
                taked.Add(source.ElementAt(i - 1));

            taked.Reverse();
            return taked;
        }
    }
}
