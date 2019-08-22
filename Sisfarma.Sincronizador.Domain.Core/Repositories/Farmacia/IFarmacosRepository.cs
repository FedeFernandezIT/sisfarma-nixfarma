using Sisfarma.Sincronizador.Domain.Entities.Farmacia;
using System;
using System.Collections.Generic;

namespace Sisfarma.Sincronizador.Domain.Core.Repositories.Farmacia
{
    public interface IFarmacosRepository
    {
        IEnumerable<Farmaco> GetAllByFechaUltimaEntradaGreaterOrEqual(DateTime ultimoFechaActualizacionStockSincronizado);
        IEnumerable<Farmaco> GetAllByFechaUltimaSalidaGreaterOrEqual(DateTime ultimoFechaActualizacionStockSincronizado);
        IEnumerable<Farmaco> GetAllWithoutStockByIdGreaterOrEqual(string ultimoMedicamentoSincronizado);
        IEnumerable<Farmaco> GetWithStockByIdGreaterOrEqual(string ultimoMedicamentoSincronizado);
        bool AnyGraterThatDoesnHaveStock(string codigo);        
        bool AnyGreaterThatHasStock(string codigo);
        bool Exists(string codigo);
    }
}
