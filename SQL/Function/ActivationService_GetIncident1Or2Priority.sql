USE [SupportExtensions]
GO
/****** Object:  UserDefinedFunction [dbo].[ActivationService_GetIncident1Or2Priority]    Script Date: 04.06.2021 7:54:59 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<s.lagun>
-- Create date: <17.05.2021>
-- Description:	<Сервис активации. Получить список обращений 1 и 2 приоритета>
-- =============================================
ALTER FUNCTION [dbo].[ActivationService_GetIncident1Or2Priority]()
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
	--,AccountTimeZoneBias int
)
AS
BEGIN
	if 
	(
		(
			ISNULL
				(
					(select top 1 LOWER(ParameterValue)
						from [SupportExtensions].[dbo].[ConfigParameters](nolock)
					where 
						ParameterName = 'IsActive_FirstOrSecondPriority'
						and ServiceName = 'ActivationIncidentService')
				, 'false')
		) = 'true'
	)
	begin
		insert into @result
		select 
			I.IncidentId
			,I.OwnerId
			,I.new_specialization SpecializationId
			,I.iok_KK_project ProjectId
			,I.CustomerId AccountId
			,I.New_podderzhka_klienty ClientCardId
			,I.ResponsibleContactId ContactId
			,I.TicketNumber
			,'FirstOrSecondPriority'
			,COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) Modified
			--,Bias.BiasValue
		from [ST_MSCRM].[dbo].[IncidentBase](nolock) I
		left join [ST_MSCRM].[dbo].[SystemUserBase](nolock) S on (I.OwnerId = S.SystemUserId)
		--cross apply (select [SupportExtensions].[dbo].[GetBiasByAccountId](I.CustomerId) as BiasValue) as Bias
		cross apply (select [SupportExtensions].[dbo].[WorkingDayDifference]((COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE())), GETDATE()) as WorkingDayDifferenceValue) as WorkingDayDifference
		where
			I.StateCode is not null
			and I.StateCode in 
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'StateCodes_FirstOrSecondPriority'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.iok_KK_project is not null
			and I.iok_KK_project not in --исключить партнеров
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
				(
					(select top 1 ParameterValue
						from [SupportExtensions].[dbo].[ConfigParameters]
					where 
						ParameterName = 'ExcludeProject'
						and ServiceName = 'ActivationIncidentService')
				, ',')
			)
			and I.iok_parentincident is null --исключить дочерние обращения
			and I.PriorityCode is not null
			and I.PriorityCode in 
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'PriorityCodes_FirstOrSecondPriority'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.CaseTypeCode is not null
			and I.CaseTypeCode not in --исключаем
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'ExcludeCaseTypeCodes_FirstOrSecondPriority'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.New_state_intenal is not null
			and I.New_state_intenal in 
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'StatesInternal_FirstOrSecondPriority'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.new_specialization is not null
			and I.new_specialization not in
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
				(
					(select top 1 ParameterValue
						from [SupportExtensions].[dbo].[ConfigParameters]
					where 
						ParameterName = 'ExcludeSpecialization'
						and ServiceName = 'ActivationIncidentService')
				, ',')
			)
			and S.New_rol_tehpodderzhka is not null
			and S.New_rol_tehpodderzhka in 
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'RolesSystemUser_FirstOrSecondPriority'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and WorkingDayDifference.WorkingDayDifferenceValue >= 
				(
					ISNULL
						(
							(select top 1 ParameterValue
								from [SupportExtensions].[dbo].[ConfigParameters]
							where 
								ParameterName = 'NumberDays_FirstOrSecondPriority'
								and ServiceName = 'ActivationIncidentService')
						, 1)
				)
			order by 
				COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) asc
				--,Bias.BiasValue desc
	end
	return;
END
