namespace Sisfarma.Sincronizador.Core.Extensions
{
    public static class NumberExtensions
    {
        public static bool ToBoolean(this long @this) 
            => @this == 1;

        public static bool ToBoolean(this int @this)
            => @this == 1;

        public static bool ToBoolean(this short @this)
            => @this == 1;
    }
}
