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
                        clientesRepository: new ClientesRepository(
                                ventasPremium: new VentasPremiumRepository()),
                        ticketRepository: new TicketRepository(),
                        vendedoresRepository: new VendedoresRepository(),
                        farmacoRepository: new FarmacoRespository(),
                        barraRepository: new CodigoBarraRepository(),
                        proveedorRepository: new ProveedoresRepository(
                                recepcionRespository: new RecepcionRespository()),
                        categoriaRepository: new CategoriaRepository(),
                        familiaRepository: new FamiliaRepository(),
                        laboratorioRepository: new LaboratorioRepository()),

                clientes: new ClientesRepository(
                        ventasPremium: new VentasPremiumRepository()),

                farmacos: new FarmacoRespository(
                        categoriaRepository: new CategoriaRepository(),
                        barraRepository: new CodigoBarraRepository(),
                        familiaRepository: new FamiliaRepository(),
                        laboratorioRepository: new LaboratorioRepository(),
                        proveedorRepository: new ProveedoresRepository(
                                recepcionRespository: new RecepcionRespository())),

                pedidos: new PedidosRepository(
                        proveedorRepository: new ProveedoresRepository(
                                recepcionRespository: new RecepcionRespository()),
                        farmacoRepository: new FarmacoRespository(),
                        categoriaRepository: new CategoriaRepository(),
                        familiaRepository: new FamiliaRepository(),
                        laboratorioRepository: new LaboratorioRepository()),

                encargos: new EncargosRepository(
                        clientesRepository: new ClientesRepository(
                                ventasPremium: new VentasPremiumRepository()),
                        proveedorRepository: new ProveedoresRepository(
                                recepcionRespository: new RecepcionRespository()),
                        farmacoRepository: new FarmacoRespository(),
                        categoriaRepository: new CategoriaRepository(),
                        familiaRepository: new FamiliaRepository(),
                        laboratorioRepository: new LaboratorioRepository(),
                        vendedoresRepository: new VendedoresRepository()),

                subcategorias: new SubcategoriaRepository(),

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
                        recepcionRespository: new RecepcionRespository()),

                vendedores: new VendedoresRepository()
            );
        }
    }
}