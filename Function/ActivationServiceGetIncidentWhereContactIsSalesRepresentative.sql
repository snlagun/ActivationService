SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<s.lagun>
-- Description:	<Сервис активации. Получить список обращений, где контакт является торговым представителем.>
-- =============================================
CREATE FUNCTION ActivationServiceGetIncidentWhereContactIsSalesRepresentative()
RETURNS 
@result table
(
	IncidentId uniqueidentifier
	,OwnerId uniqueidentifier
	,Specialization uniqueidentifier
	,TicketNumber nvarchar(50)
	,SelectionType nvarchar(100)
	,Modified dateTime
	,AccountTimeZoneBias int
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
						ParameterName = 'IsActive_SalesRepresentative'
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
			,I.OwnerId
			,I.new_specialization Specialization
			,I.TicketNumber
			,'SalesRepresentativeType'
			,COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) Modified
			,Bias.BiasValue
		from [ST_MSCRM].[dbo].[IncidentBase](nolock) I
		left join [ST_MSCRM].[dbo].[SystemUserBase](nolock) S on (I.OwnerId = S.SystemUserId)
		left join [ST_MSCRM].[dbo].[ContactBase](nolock) C on (ISNULL(I.ResponsibleContactId, I.PrimaryContactId) = C.ContactId)
		cross apply (select [ST_MSCRM].[dbo].[GetBiasByAccountId](I.CustomerId) as BiasValue) as Bias
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
							ParameterName = 'StateCodes_SalesRepresentative'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.iok_KK_project is not null
			and I.iok_KK_project not in --исключить партнеров
			(
				select projectId from @excludeProject
			)
			and I.iok_parentincident is null --исключить дочерние обращения
			and I.CaseTypeCode is not null
			and I.CaseTypeCode not in --исключаем
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'ExcludeCaseTypeCodes_SalesRepresentative'
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
							ParameterName = 'StatesInternal_SalesRepresentative'
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
							ParameterName = 'RolesSystemUser_SalesRepresentative'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and C.svk_role_dlya_podderzhki is not null
			and C.svk_role_dlya_podderzhki in
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'RolesContact_SalesRepresentative'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and WorkingDayDifference.WorkingDayDifferenceValue > 
				(
					ISNULL
						(
							(select top 1 ParameterValue
								from [SupportExtensions].[dbo].[ConfigParameters]
							where 
								ParameterName = 'NumberDays_SalesRepresentative'
								and ServiceName = 'ActivationIncidentService')
						, 1) -- Значение по умолчанию включено
				)
			order by 
				COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) asc
				,Bias.BiasValue desc
				
			--CreatePhoneCall_SalesRepresentative
	end
	
	RETURN 
END
GO