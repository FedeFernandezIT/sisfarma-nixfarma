﻿using System.Collections.Generic;
using Sisfarma.Sincronizador.Domain.Entities.Farmacia;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IClientesRepository
    {
        bool EsBeBlue(string tipoCliente);

        bool Exists(int id);

        T GetAuxiliarById<T>(string cliente) where T : ClienteAux;

        List<Cliente> GetGreatThanId(long id);

        Cliente GetOneOrDefaultById(long id);

        decimal GetTotalPuntosById(string idCliente);

        bool HasSexoField();
    }
}