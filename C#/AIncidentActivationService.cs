using System;
using System.Collections.Generic;
using System.Linq;
using Extensions.Common.EnumHelpers;
using Extensions.InternalService.Factories.NinjectFactories.Interfaces;
using Extensions.InternalService.Models.IncidentModels;
using Extensions.InternalService.Services.Interfaces;
using Extensions.Ninject.Factories.Interfaces;
using Extensions.Repository.Interfaces;
using Extensions.XrmServices.Enums.Email;
using Extensions.XrmServices.Enums.Functional;
using Extensions.XrmServices.Enums.Incident;
using Extensions.XrmServices.Enums.Subject;
using Extensions.XrmServices.Models.AnnotationModels;
using Extensions.XrmServices.Models.EmailModels;
using Extensions.XrmServices.Models.IncidentModels;
using Extensions.XrmServices.Models.PhoneCallModels;
using Extensions.XrmServices.Services.Interfaces;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json;
using XRMLibrary.Xrm;

namespace Extensions.InternalService.Services.AClasses
{
    public abstract class AIncidentActivationService : IIncidentActivationService
    {
        private readonly IRepositoryNinjectFactory _repositoryNinjectFactory;
        private readonly IInternalServiceFactory _internalServiceFactory;
        private IXrmRepository<Incident> _incidentXrmRepository;
        private ISqlService _sqlService;
        private IIncidentService _incidentService;
        private ISystemUserService _systemUserService;
        private IPhoneCallService _phoneCallService;
        private IEmailService _emailService;
        private IAnnotationService _annotationService;


        private const string GetListIncidentTextCommand = @"select * from [SupportExtensions].[dbo].[ActivationService_GetIncidentList]()";

        protected AIncidentActivationService(IRepositoryNinjectFactory repositoryNinjectFactory, IInternalServiceFactory internalServiceFactory)
        {
            _repositoryNinjectFactory = repositoryNinjectFactory;
            _internalServiceFactory = internalServiceFactory;
            CheckingRepositoriesAndServices();
        }

        public void Start()
        {
            var listIncident = GetIncidentList();
            if (!listIncident.Any()) return;

            const string businessProcessClient = "BusinessProcessClient";
            const string thirdOrFourthPriorityAwaitClientCheckMoreFiveDays = "ThirdOrFourthPriorityAwaitClientCheckMoreFiveDays";
            const string thirdOrFourthPriorityAwaitClientCheckMoreFourDays = "ThirdOrFourthPriorityAwaitClientCheckMoreFourDays";

            listIncident.Where(x =>!string.IsNullOrEmpty(x.SelectionType) && x.SelectionType == businessProcessClient)
                .ToList().ForEach(y => this.CloseIncidentBusinessProcessClient(y.IncidentId));

            listIncident.Where(x=>!string.IsNullOrEmpty(x.SelectionType) && x.SelectionType == thirdOrFourthPriorityAwaitClientCheckMoreFiveDays)
                .ToList().ForEach(y=>this.SetStateInternalToClosed(y.IncidentId));

            listIncident.Where(x=>!string.IsNullOrEmpty(x.SelectionType) && x.SelectionType == thirdOrFourthPriorityAwaitClientCheckMoreFourDays)
                .ToList().ForEach(y=>this.SendWarningLetter(y.IncidentId));

            //Список типов выбора обращений, которые исключаем
            var excludeSelectionType = new List<string> { businessProcessClient, thirdOrFourthPriorityAwaitClientCheckMoreFiveDays, thirdOrFourthPriorityAwaitClientCheckMoreFourDays };

            //Список обращений для активации и создания заказанного звонка
            var incidentActivation = listIncident.Where(x =>
                !string.IsNullOrEmpty(x.SelectionType) && !excludeSelectionType.Contains(x.SelectionType)).ToList();

            incidentActivation.ForEach(x=>this.ActivateIncident(x.IncidentId, x.SelectionType));
            incidentActivation.Where(x=>x.CreatePhoneCall != null && x.CreatePhoneCall.Value)
                .ToList().ForEach(y=>this.CreatePhoneCall(y.IncidentId));
        }

        private void CheckingRepositoriesAndServices()
        {
            _incidentXrmRepository = _repositoryNinjectFactory.IncidentXrmRepository();
            if (_incidentXrmRepository == null)
                throw new NullReferenceException("IncidentXrmRepository is null");

            _sqlService = _internalServiceFactory.SqlService();
            if (_sqlService == null)
                throw new NullReferenceException("SqlService is null");

            _incidentService = _internalServiceFactory.IncidentService();
            if (_incidentService == null)
                throw new NullReferenceException("IncidentService is null");

            _systemUserService = _internalServiceFactory.SystemUserService();
            if (_systemUserService == null)
                throw new NullReferenceException("SystemUserService is null");

            _phoneCallService = _internalServiceFactory.PhoneCallService();
            if (_phoneCallService == null)
                throw new NullReferenceException("PhoneCallService is null");

            _emailService = _internalServiceFactory.EmailService();
            if (_emailService == null)
                throw new NullReferenceException("EmailService is null");

            _annotationService = _internalServiceFactory.AnnotationService();
            if (_annotationService == null)
                throw new NullReferenceException("AnnotationService is null");
        }

        private IList<IncidentActivationServiceModel> GetIncidentList()
        {
            try
            {
                var dataTable = _sqlService.ExecuteInsertToTable(GetListIncidentTextCommand, "ConnectToSupportExtensions");
                return dataTable.Rows.Count == 0
                    ? new List<IncidentActivationServiceModel>()
                    : JsonConvert.DeserializeObject<List<IncidentActivationServiceModel>>(JsonConvert.SerializeObject(dataTable));
            }
            catch (Exception e)
            {
                return new List<IncidentActivationServiceModel>();
            }
        }

        private void ActivateIncident(Guid? incidentId, string selectionType)
        {
            if (incidentId == null || incidentId.Value == Guid.Empty) return;

            var incident = _incidentXrmRepository.FindEntityById((Guid)incidentId);

            if (incident?.StateCode == null
                || (incident.StateCode != null
                    && incident.StateCode.Value != IncidentState.Active)) return;

            var zSupport = _systemUserService.GetZSupportSystemUser();

            var commentIncidentModel = new CommentIncidentModel
            {
                IncidentId = incidentId,
                CommentString = "Обращение активировано по истечению срока актуальности комментариев.",
                NoteTextAnnotation = "Обращение активировано по истечению срока актуальности комментариев.",
                SubjectAnnotation = $"*service* *IncidentActivationService* *{selectionType}* *{zSupport?.FullName}* *Support:{zSupport?.NickName}*",
                StateInternalOptionSetValue = new OptionSetValue((int)StateInternalIncidentType.Awaiting),
            };

            _incidentService.AddComment(commentIncidentModel);
        }

        private void CreatePhoneCall(Guid? incidentId)
        {
            if (incidentId == null || incidentId.Value == Guid.Empty) return;

            var existPhoneCall = _phoneCallService.GetPhoneCallModelByIncidentId(incidentId);
            if (existPhoneCall?.PhoneCallId != null && existPhoneCall.PhoneCallId.Value != Guid.Empty) return;

            var incident = _incidentXrmRepository.FindEntityById((Guid)incidentId);

            if (incident?.StateCode == null
                || (incident.StateCode != null
                    && incident.StateCode.Value != IncidentState.Active)) return;

            var phoneCallModel = new PhoneCallModel
            {
                Incident = incident.ToEntityReference(),
                Title = "Необходимо связаться по обращению: " + incident.TicketNumber,
                Comment = "Данный заказанный звонок создан автоматически. Необходимо связаться по обращению и актуализировать его статус.",
                ResponsibleContact = incident.ResponsibleContactId ?? incident.PrimaryContactId,
            };

            _phoneCallService.CreatePhoneCall(phoneCallModel);
        }

        private void CloseIncidentBusinessProcessClient(Guid? incidentId)
        {
            if (incidentId == null || incidentId.Value == Guid.Empty) return;

            var incident = _incidentXrmRepository.FindEntityById((Guid)incidentId);

            if (incident?.StateCode == null
                || (incident.StateCode != null
                    && incident.StateCode.Value != IncidentState.Active)) return;

            var zSupport = _systemUserService.GetZSupportSystemUser();

            const string comment = "Обращение закрыто автоматически, т.к. длительное время не получено подтверждение от пользователя.";

            var closeIncidentModel = new CloseIncidentModel
            {
                IncidentId = incidentId,
                CommentString =
                    "Обращение закрыто автоматически, т.к. длительное время не получено подтверждение от пользователя.",
                SubjectAnnotation =
                    $"*WEB* *IncidentActivationService* *AutoClose* *Support:{(zSupport == null ? string.Empty : zSupport.NickName)}*",
                NoteTextAnnotation = $"*WEB* {comment}",
                Reason = "Бизнес-процесс клиента",
                FunctionalEntityReference = new EntityReference(svk_functional.EntityLogicalName,
                    Guid.Parse(EnumHelper<FunctionalType>.GetDisplayValue(FunctionalType.BusinessProcess))),
                SubjectEntityReference = new EntityReference(Subject.EntityLogicalName,
                    Guid.Parse(EnumHelper<SubjectType>.GetDisplayValue(SubjectType.BusinessProcesses))),
                IncidentResolutionSubject = "Обращение закрыто автоматически",
                Status = new OptionSetValue((int)StatusIncidentType.ProblemSolved),
                StateInternalOptionSetValue = new OptionSetValue((int)StateInternalIncidentType.Closed),
                ProblemSide = new OptionSetValue((int)ProblemSideType.Client),
            };
            _incidentService.CloseIncidentFromModel(closeIncidentModel);
        }

        private void SetStateInternalToClosed(Guid? incidentId)
        {
            if (incidentId == null || incidentId.Value == Guid.Empty) return;

            var incident = _incidentXrmRepository.FindEntityById((Guid)incidentId);

            if (incident?.StateCode == null
                || (incident.StateCode != null
                    && incident.StateCode.Value != IncidentState.Active)) return;

            var zSupport = _systemUserService.GetZSupportSystemUser();

            const string comment = "Подтверждения от клиента об актуальности обращения не было получено в течение 5ти рабочих дней.";

            var commentIncidentModel = new CommentIncidentModel
            {
                IncidentId = incidentId,
                CommentString = comment,
                NoteTextAnnotation = $"*WEB* {comment} Обращению присвоен статус \"Закрыто\".",
                SubjectAnnotation = $"*WEB* *IncidentActivationService* *{zSupport?.FullName}* *Support:{zSupport?.NickName}*",
                StateInternalOptionSetValue = new OptionSetValue((int)StateInternalIncidentType.Closed),
            };

            _incidentService.AddComment(commentIncidentModel);
        }

        private void SendWarningLetter(Guid? incidentId)
        {
            if (incidentId == null || incidentId.Value == Guid.Empty) return;

            var incident = _incidentXrmRepository.FindEntityById((Guid)incidentId);

            if (incident?.StateCode == null
                || (incident.StateCode != null
                    && incident.StateCode.Value != IncidentState.Active)) return;

            var zSupport = _systemUserService.GetZSupportSystemUser();

            var templateId = Guid.Parse(EnumHelper<EmailTemplateType>.GetDisplayValue(EmailTemplateType.ClosingWarning));

            var fromParty = new[]
            {
                new ActivityParty
                {
                    PartyId = zSupport.ToEntityReference(),
                },
            };

            var toParty = new[]
            {
                new ActivityParty
                {
                    PartyId = incident.ResponsibleContactId ?? incident.PrimaryContactId,
                },
            };

            var emailModelFromTemplate = new EmailModelFromTemplate
            {
                RegardingObject = incident.ToEntityReference(),
                TemplateId = templateId,
                ToParty = toParty,
                FromParty = fromParty,
            };
            _emailService.SendEmailFromTemplate(emailModelFromTemplate);

            var annotationModel = new AnnotationModel
            {
                Subject = $"*WEB* *IncidentActivationService* *WarningLetter* *{zSupport?.FullName}* *Support:{zSupport?.NickName}*",
                NoteText = "*WEB* Отправлено письмо: \"Служба поддержки ждет отметку о решении обращения\"",
                RegardingObject = incident.ToEntityReference(),
            };
            _annotationService.AddAnnotation(annotationModel);

            var updateIncident = new Incident
            {
                Id = (Guid)incidentId,
                new_date_sent_warn = DateTime.Now,
            };
            _incidentXrmRepository.Update(updateIncident);
        }
    }
}