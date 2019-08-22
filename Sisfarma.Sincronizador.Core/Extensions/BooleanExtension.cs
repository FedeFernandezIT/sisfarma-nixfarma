namespace Sisfarma.Sincronizador.Core.Extensions
{
    public static class BooleanExtension
    {
        public static int ToInteger(this bool @this)
            => @this ? 1 : 0;

        public static int ToInteger(this bool? @this)
            => @this?.ToInteger() ?? 0;
    }
}
