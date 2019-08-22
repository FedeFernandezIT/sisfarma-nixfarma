using System;
using Sisfarma.Sincronizador.Domain.Entities.Fisiotes;

namespace Sisfarma.Sincronizador.Domain.Core.ExternalServices.Fisiotes
{
    public interface IEncargosExternalService : IEncargosExternalServiceNew
    {
        bool Exists(int encargo);
        Encargo GetByEncargoOrDefault(int encargo);
        void Insert(Encargo ee);
        Encargo LastOrDefault();
        void UpdateFechaDeEntrega(DateTime fechaEntrega, long idEncargo);
        void UpdateFechaDeRecepcion(DateTime fechaRecepcion, long idEncargo);
    }

    public interface IEncargosExternalServiceNew
    {
        void Sincronizar(Encargo encargo);
    }
}