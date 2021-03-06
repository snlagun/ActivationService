USE [SupportExtensions]
GO
/****** Object:  UserDefinedFunction [dbo].[ActivationService_GetIncidentList]    Script Date: 04.06.2021 7:57:18 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<s.lagun>
-- Create date: <17.05.2021>
-- Description:	<Сервис активации. Получить список обращений>
-- =============================================
ALTER FUNCTION [dbo].[ActivationService_GetIncidentList]()
RETURNS 
@result table
(
	IncidentId uniqueidentifier
	,OwnerId uniqueidentifier
	,SpecializationId uniqueidentifier
	,ProjectId uniqueidentifier
	,AccountId uniqueidentifier
	,ClientCardId uniqueidentifier
	,ContactId uniqueidentifier
	,TicketNumber nvarchar(50)
	,SelectionType nvarchar(100)
	,Modified dateTime
	,CreatePhoneCall bit
	--,AccountTimeZoneBias int
	--,UserTimeZoneBias int
)
AS
BEGIN
	--return;

	declare @isWorkingDateTime int = (select [SupportExtensions].[dbo].[IsWorkingDateTime](GETDATE()));
	if (@isWorkingDateTime = 2) --праздничный/нерабочий день
		return;

	if 
	(
		(
			ISNULL
				(
					(select top 1 LOWER(ParameterValue)
						from [SupportExtensions].[dbo].[ConfigParameters](nolock)
					where 
						ParameterName = 'IsActive'
						and ServiceName = 'ActivationIncidentService')
				, 'false') -- Значение по умолчанию выключено
		) = 'true'
	)
	begin
		;with ActivationServiceSchedule as
		(
			select
				S.City
				,Bias.BiasValue 
			from [SupportExtensions].[dbo].[ActivationServiceSchedule](nolock) S
			cross apply (select [SupportExtensions].[dbo].[GetBiasByUserInterfaceName](CONCAT('%', S.City, '%')) as BiasValue) Bias
			where 
				DATEPART(HOUR, S.UTCTime) = DATEPART(HOUR, GETUTCDATE())
				and Bias.BiasValue is not null
		)
		--select * from ActivationServiceSchedule
		,IncidentIsBusinessProcessClient as
		(
			select * from [SupportExtensions].[dbo].[ActivationService_GetIncidentIsBusinessProcessClient]()
		)		
		,Incident3Or4PriorityAwaitClientCheckSetStateInternalToClosed as
		(
			select * from [SupportExtensions].[dbo].[ActivationService_GetIncident3Or4PriorityAwaitClientCheckSetStateInternalToClosed]()
		)
		,Incident3Or4PriorityAwaitClientCheckWarningLetter as
		(
			select * from [SupportExtensions].[dbo].[ActivationService_GetIncident3Or4PriorityAwaitClientCheckWarningLetter]()
		)
		,IncidentWhereContactIsSalesRepresentative as
		(
			select * from [SupportExtensions].[dbo].[ActivationService_GetIncidentWhereContactIsSalesRepresentative]()
		)
		,Incident1Or2Priority as
		(
			select * from  [SupportExtensions].[dbo].[ActivationService_GetIncident1Or2Priority]()
		)
		,Incident3Or4Priority as
		(
			select * from [SupportExtensions].[dbo].[ActivationService_GetIncident3Or4Priority]()
		)
		,Incident3Or4PriorityAwaitClientCheck as
		(
			select * from [SupportExtensions].[dbo].[ActivationService_GetIncident3Or4PriorityAwaitClientCheck]()
		)
		,Incident3Or4PriorityWhereContactIsAutoinformer as
		(
			select * from  [SupportExtensions].[dbo].[ActivationService_GetIncident3Or4PriorityWhereContactIsAutoinformer]()
		)
		,IncidentWhereParentSpecializationIsDataExchange as
		(
			select * from  [SupportExtensions].[dbo].[ActivationService_GetIncidentWhereParentSpecializationIsDataExchange]()
		)
		,Incident3Or4PriorityWhereOwnerIsTechnicalClientManager as
		(
			select * from  [SupportExtensions].[dbo].[ActivationService_GetIncident3Or4PriorityWhereOwnerIsTechnicalClientManager]()
		)
		,IncidentWhereProjectIsPartner as
		(
			select * from [SupportExtensions].[dbo].[ActivationService_GetIncidentWhereProjectIsPartner]()
		)
		,Incident3Or4PriorityWhereOwnerIs3Line as
		(
			select * from
			(
				select * from  [SupportExtensions].[dbo].[ActivationService_GetIncident3Or4PriorityWhereOwnerIs3Line]()
			) I
			where DATEPART(HOUR, GETDATE()) in 
			( 
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'StartHours_ThirdFourPriorityThirdLine'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
		)
		,UnionAll as
		(
			select * from IncidentIsBusinessProcessClient
			union all
			select * from Incident3Or4PriorityAwaitClientCheckSetStateInternalToClosed
			union all
			select * from Incident3Or4PriorityAwaitClientCheckWarningLetter
			union all
			select * from IncidentWhereContactIsSalesRepresentative
			union all
			select * from Incident1Or2Priority
			union all
			select * from Incident3Or4Priority
			union all
			select * from Incident3Or4PriorityAwaitClientCheck
			union all
			select * from Incident3Or4PriorityWhereContactIsAutoinformer
			union all
			select * from IncidentWhereParentSpecializationIsDataExchange
			union all
			select * from Incident3Or4PriorityWhereOwnerIsTechnicalClientManager
			union all
			select * from Incident3Or4PriorityWhereOwnerIs3Line
			union all
			select * from IncidentWhereProjectIsPartner
		)
		,UnionAllCrossApplySystemUserTimeZoneBias as
		(
			select 
				A.*
				,Bias.BiasValue UserTimeZoneBias 
			from UnionAll A
			cross apply (select [SupportExtensions].[dbo].[GetBiasByUserId](A.OwnerId) as BiasValue) as Bias
			where A.IncidentId in
				(select distinct IncidentId from UnionAll) --Уникальные IncidentId
		)
		,IncidentListJoinSchedule as
		(
			select 
				*
			from UnionAllCrossApplySystemUserTimeZoneBias U
			inner join ActivationServiceSchedule S on (U.UserTimeZoneBias = S.BiasValue)
		)
		--select * from IncidentListJoinSchedule
		,IncidentListRank as
		(
			select 
				*,
				ROW_NUMBER() over (partition by OwnerId order by modified asc) as OwnerIdRank
			from IncidentListJoinSchedule
		)
		,Result as
		(
			select 
				L.*
				,C.ParameterValue CreatePhoneCall
			from IncidentListRank L
			left join [SupportExtensions].[dbo].[ConfigParameters](nolock) C on (C.ParameterName = CONCAT('CreatePhoneCall', '_', L.SelectionType) and C.ServiceName = 'ActivationIncidentService')
			where OwnerIdRank <= 
				ISNULL
				(
					(select top 1 ParameterValue
						from [SupportExtensions].[dbo].[ConfigParameters](nolock)
					where 
						ParameterName = 'NumberIncidentsForActivation'
						and ServiceName = 'ActivationIncidentService')
				, 0)
		)
		--select * from Result

		insert into @result
		select 
			IncidentId
			,OwnerId
			,SpecializationId
			,ProjectId
			,AccountId
			,ClientCardId
			,ContactId
			,TicketNumber
			,SelectionType
			,Modified
			,CreatePhoneCall
		from Result

		return;
	end
	
	return; 
END
