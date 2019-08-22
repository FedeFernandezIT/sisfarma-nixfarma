using System;
using System.Security.Policy;

namespace Sisfarma.RestClient
{
    public abstract class BaseRestClient
    {
        protected Uri _baseAddress;
        protected Uri _resource;

        protected BaseRestClient()
        {
        }

        protected BaseRestClient(Uri baseAddress)
        {
            _baseAddress = baseAddress ?? throw new ArgumentNullException(nameof(baseAddress));
        }

        protected BaseRestClient(Uri baseAddress, Uri resource)
        {
            _baseAddress = baseAddress ?? throw new ArgumentNullException(nameof(baseAddress));

            _resource = resource ?? throw new ArgumentNullException(nameof(resource));
            if (resource.IsAbsoluteUri)
            {
                throw new ArgumentException("No es una ruta relativa", nameof(resource));
            }
        }
    }
}