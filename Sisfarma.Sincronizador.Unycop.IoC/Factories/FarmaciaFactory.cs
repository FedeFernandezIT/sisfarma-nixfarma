using Sisfarma.Sincronizador.Domain.Core.Services;
using Sisfarma.Sincronizador.Nixfarma.Infrastructure.Repositories.Farmacia;

namespace Sisfarma.Sincronizador.Unycop.IoC.Factories
{
    public static class FarmaciaFactory
    {
        public static FarmaciaService Create()
        {
            return new FarmaciaService(
                categorias: new CategoriasRepository(),

                familias: new FamiliaRepository(),

                ventas: new VentasRepository(
                        clientesRepository: new ClientesRepository(),
                        farmacoRepository: new FarmacoRespository(),
                        barraRepository: new CodigoBarraRepository(),
                        proveedorRepository: new ProveedoresRepository(
                                recepcionRespository: new RecepcionRespository()),
                        categoriaRepository: new CategoriaRepository(),
                        familiaRepository: new FamiliaRepository(),
                        laboratorioRepository: new LaboratorioRepository()),

                clientes: new ClientesRepository(),

                farmacos: new FarmacoRespository(
                        categoriaRepository: new CategoriaRepository(),
                        barraRepository: new CodigoBarraRepository(),
                        familiaRepository: new FamiliaRepository(),
                        laboratorioRepository: new LaboratorioRepository(),
                        proveedorRepository: new ProveedoresRepository(
                                recepcionRespository: new RecepcionRespository()),
                        tarifaRepository: new TarifaRepository(),
                        empresaRepository: new EmpresaRepository()),

                pedidos: new PedidosRepository(
                        proveedorRepository: new ProveedoresRepository(
                                recepcionRespository: new RecepcionRespository()),
                        farmacoRepository: new FarmacoRespository(),
                        categoriaRepository: new CategoriaRepository(),
                        familiaRepository: new FamiliaRepository(),
                        laboratorioRepository: new LaboratorioRepository()),

                encargos: new EncargosRepository(
                        clientesRepository: new ClientesRepository(),
                        proveedorRepository: new ProveedoresRepository(
                                recepcionRespository: new RecepcionRespository()),
                        farmacoRepository: new FarmacoRespository(),
                        categoriaRepository: new CategoriaRepository(),
                        familiaRepository: new FamiliaRepository(),
                        laboratorioRepository: new LaboratorioRepository()),

                listas: new ListaRepository(),

                sinonimos: new SinonimosRepository(),

                recepciones: new RecepcionRespository(
                        proveedorRepository: new ProveedoresRepository(
                                recepcionRespository: new RecepcionRespository()),
                        farmacoRepository: new FarmacoRespository(),
                        categoriaRepository: new CategoriaRepository(),
                        familiaRepository: new FamiliaRepository(),
                        laboratorioRepository: new LaboratorioRepository()),

                proveedores: new ProveedoresRepository(
                        recepcionRespository: new RecepcionRespository())
            );
        }
    }
}