SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE FUNCTION ActivationServiceGetIncidentThirdOrFourthPriorityAwaitClientCheckWarningLetter()
RETURNS 
@result table
(
	IncidentId uniqueidentifier
	,TicketNumber nvarchar(50)
)
AS
BEGIN
	if 
	(
		(
			ISNULL
				(
					(select top 1 ParameterValue
						from [SupportExtensions].[dbo].[ConfigParameters]
					where 
						ParameterName = 'IsActive_ThirdOrFourthPriorityAwaitClientCheckMoreFourDays'
						and ServiceName = 'ActivationIncidentService')
				, 'false') -- Значение по умолчанию выключено
		) = 'true'
	)
	begin
		declare @excludeSpecialization table(specializationId uniqueidentifier, specializationName nvarchar(100));
		insert into @excludeSpecialization values 
				('B6551577-BAF9-EA11-9E49-00155DC86C0F', 'IR Quality')
				,('537510A1-AB08-EB11-9E4B-00155DC86C0F', 'Inspector Cloud');

		declare @excludeProject table(projectId uniqueidentifier, projectName nvarchar(100));
		insert into @excludeProject values
			('DA8468B0-45F8-E211-AE2E-00155D017F87', 'Партнеры')
			,('DC01A1E8-47F8-E211-AE2E-00155D017F87', 'Прямые продажи')

		insert into @result
		select 
			I.IncidentId
			,I.TicketNumber
		from [ST_MSCRM].[dbo].[IncidentBase](nolock) I
		left join [ST_MSCRM].[dbo].[SystemUserBase](nolock) S on (I.OwnerId = S.SystemUserId)
		left join [ST_MSCRM].[dbo].[ContactBase](nolock) C on (ISNULL(I.ResponsibleContactId, I.PrimaryContactId) = C.ContactId)
		cross apply (select [ST_MSCRM].[dbo].[GetBiasByAccountId](I.CustomerId) as BiasValue) as Bias
		cross apply (select [SupportExtensions].[dbo].[WorkingDayDifference](COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()), GETDATE()) as WorkingDayDifferenceValue) as WorkingDayDifferenceNotif
		cross apply (select [SupportExtensions].[dbo].[WorkingDayDifference](COALESCE(I.new_date_sent_warn, I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()), GETDATE()) as WorkingDayDifferenceValue) as WorkingDayDifferenceWarn
		where
			I.StateCode is not null
			and I.StateCode in 
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'StateCodes_ThirdOrFourthPriorityAwaitClientCheckMoreFourDays'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.iok_KK_project is not null
			and I.iok_KK_project not in --исключить партнеров
			(
				select projectId from @excludeProject
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
							ParameterName = 'PriorityCodes_ThirdOrFourthPriorityAwaitClientCheckMoreFourDays'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.CaseTypeCode is not null
			and I.CaseTypeCode in 
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'CaseTypeCodes_ThirdOrFourthPriorityAwaitClientCheckMoreFourDays'
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
							ParameterName = 'StatesInternal_ThirdOrFourthPriorityAwaitClientCheckMoreFourDays'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.new_specialization is not null
			and I.new_specialization not in
			(
				select specializationId from @excludeSpecialization
			)
			and S.New_rol_tehpodderzhka is not null
			and S.New_rol_tehpodderzhka in 
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'RolesSystemUser_ThirdOrFourthPriorityAwaitClientCheckMoreFourDays'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and WorkingDayDifferenceNotif.WorkingDayDifferenceValue > 
				(
					ISNULL
						(
							(select top 1 ParameterValue
								from [SupportExtensions].[dbo].[ConfigParameters]
							where 
								ParameterName = 'NumberDays_ThirdOrFourthPriorityAwaitClientCheckMoreFourDays'
								and ServiceName = 'ActivationIncidentService')
						, 1)
				)
			and WorkingDayDifferenceWarn.WorkingDayDifferenceValue > 
				(
					ISNULL
						(
							(select top 1 ParameterValue
								from [SupportExtensions].[dbo].[ConfigParameters]
							where 
								ParameterName = 'NumberDays_ThirdOrFourthPriorityAwaitClientCheckMoreFourDays'
								and ServiceName = 'ActivationIncidentService')
						, 1)
				)
			order by 
				COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) asc
				,Bias.BiasValue desc
	end
	
	RETURN 
END
GO