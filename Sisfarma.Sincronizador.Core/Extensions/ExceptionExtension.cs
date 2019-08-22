using Sisfarma.RestClient.Exceptions;
using System;

namespace Sisfarma.Sincronizador.Core.Extensions
{
    public static class ExceptionExtension
    {
        public static string ToLogErrorMessage(this Exception @this) 
            =>  $"Fecha UTC: {DateTime.UtcNow.ToIsoString()}{Environment.NewLine}" +
                $"Message: {@this.Message}{Environment.NewLine}StackTrace: {@this.StackTrace}";

        public static string ToLogErrorMessage(this RestClientException @this)
            => $"Fecha UTC: {DateTime.UtcNow.ToIsoString()}{Environment.NewLine}" +
                $"Request: {@this.Request.ToString()}{Environment.NewLine}Response: {@this.Response.ToString()}{Environment.NewLine}" +
                $"Message: {@this.Message}{Environment.NewLine}StackTrace: {@this.StackTrace}";
    }
}
