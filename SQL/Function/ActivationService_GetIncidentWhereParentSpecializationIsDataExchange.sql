USE [SupportExtensions]
GO
/****** Object:  UserDefinedFunction [dbo].[ActivationService_GetIncidentWhereParentSpecializationIsDataExchange]    Script Date: 04.06.2021 7:57:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<s.lagun>
-- Create date: <19.05.2021>
-- Description:	<Сервис активации. Получить список обращений, где родительская специализация ОД.>
-- =============================================
ALTER FUNCTION [dbo].[ActivationService_GetIncidentWhereParentSpecializationIsDataExchange]()
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
						ParameterName = 'IsActive_ParentSpecializationIsDataExchange'
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
			,'ParentSpecializationIsDataExchange'
			,COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) Modified
			--,Bias.BiasValue
		from [ST_MSCRM].[dbo].[IncidentBase](nolock) I
		left join [ST_MSCRM].[dbo].[SystemUserBase](nolock) S on (I.OwnerId = S.SystemUserId)
		left join [ST_MSCRM].[dbo].[ContactBase](nolock) C on (ISNULL(I.ResponsibleContactId, I.PrimaryContactId) = C.ContactId)
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
							ParameterName = 'StateCodes_ParentSpecializationIsDataExchange'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.iok_KK_project is not null
			and I.iok_KK_project in
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'Project_ParentSpecializationIsDataExchange'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.iok_parentincident is null --исключить дочерние обращения
			and I.CaseTypeCode is not null
			and I.CaseTypeCode in
			(
				select value from [SupportExtensions].[dbo].[SplitStringCustom]
					(
						(select top 1 ParameterValue
							from [SupportExtensions].[dbo].[ConfigParameters]
						where 
							ParameterName = 'CaseTypeCodes_ParentSpecializationIsDataExchange'
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
							ParameterName = 'StatesInternal_ParentSpecializationIsDataExchange'
							and ServiceName = 'ActivationIncidentService')
					, ',')
			)
			and I.new_specialization is not null
			and I.new_specialization in
			(
					select 
						new_specializationId
					from [ST_MSCRM].[dbo].[new_specializationBase](nolock)
						where new_parentSpecialization = '227C9131-E824-E711-80D4-00155D234F02' --Обмен данными (родительская)
			)
			and S.IsDisabled = 0
			and 
			(
				(
					S.new_positionHeld is not null
					and	S.new_positionHeld not in 
					(
						select value from [SupportExtensions].[dbo].[SplitStringCustom]
							(
								(select top 1 ParameterValue
									from [SupportExtensions].[dbo].[ConfigParameters]
								where 
									ParameterName = 'ExcludePositionHeldSystemUser_LogicalOperatorOr_ParentSpecializationIsDataExchange'
									and ServiceName = 'ActivationIncidentService')
							, ',')
					)
				)
				or
				(
					S.New_rol_tehpodderzhka is not null
					and S.New_rol_tehpodderzhka not in 
					(
						select value from [SupportExtensions].[dbo].[SplitStringCustom]
							(
								(select top 1 ParameterValue
									from [SupportExtensions].[dbo].[ConfigParameters]
								where 
									ParameterName = 'ExcludeRoleSystemUser_LogicalOperatorOr_ParentSpecializationIsDataExchange'
									and ServiceName = 'ActivationIncidentService')
							, ',')
					)
				)
			)
			and WorkingDayDifference.WorkingDayDifferenceValue >= 
				(
					ISNULL
						(
							(select top 1 ParameterValue
								from [SupportExtensions].[dbo].[ConfigParameters]
							where 
								ParameterName = 'NumberDays_ParentSpecializationIsDataExchange'
								and ServiceName = 'ActivationIncidentService')
						, 1)
				)
			order by 
				COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) asc
				--,Bias.BiasValue desc
	end
	
	RETURN 
END
