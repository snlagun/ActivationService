using Extensions.InternalService.Factories.NinjectFactories.Interfaces;
using Extensions.InternalService.Services.AClasses;
using Extensions.Ninject.Factories.Interfaces;

namespace Extensions.InternalService.Services
{
    public class IncidentActivationService: AIncidentActivationService
    {
        public IncidentActivationService(IRepositoryNinjectFactory repositoryNinjectFactory, IInternalServiceFactory internalServiceFactory) : base(repositoryNinjectFactory, internalServiceFactory)
        {
        }
    }
}