using System;

namespace Sisfarma.Sincronizador.Extensions
{
    public static class DateTimeExtension
    {
        public static string ToIsoString(this DateTime? @this)
            => @this.HasValue
                ? @this.Value.ToIsoString()
                : DateTime.MinValue.ToIsoString();

        public static string ToIsoString(this DateTime @this)
            => @this.ToString("yyyy-MM-ddTHH:mm:ss");

        public static int ToDateInteger(this DateTime @this, string format = "yyyyMMdd")
            => @this.ToString(format)
                .ToIntegerOrDefault();
        public static int ToTimeInteger(this DateTime @this, string format = "HHmm")
            => @this.ToString(format)
                .ToIntegerOrDefault();
    }
}
