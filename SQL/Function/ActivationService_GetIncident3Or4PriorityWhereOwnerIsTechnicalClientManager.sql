USE [SupportExtensions]
GO
/****** Object:  UserDefinedFunction [dbo].[ActivationService_GetIncident3Or4PriorityWhereOwnerIsTechnicalClientManager]    Script Date: 04.06.2021 7:56:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<s.lagun>
-- Create date: <01.06.2021>
-- Description:	<Сервис активации. Получить список обращений 3 и 4 приоритета, где отвественный ТКМ>
-- =============================================
ALTER FUNCTION [dbo].[ActivationService_GetIncident3Or4PriorityWhereOwnerIsTechnicalClientManager]
()
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
						from [SupportExtensions].[dbo].[ConfigParameters]
					where 
						ParameterName = 'IsActive_ThirdOrFourthPriorityAwaitClientCheckOwnerIsTkm'
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
			,'ThirdOrFourthPriorityAwaitClientCheckOwnerIsTkm'
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
							ParameterName = 'StateCodes_ThirdOrFourthPriorityAwaitClientCheckOwnerIsTkm'
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
							ParameterName = 'PriorityCodes_ThirdOrFourthPriorityAwaitClientCheckOwnerIsTkm'
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
							ParameterName = 'CaseTypeCodes_ThirdOrFourthPriorityAwaitClientCheckOwnerIsTkm'
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
							ParameterName = 'StatesInternal_ThirdOrFourthPriorityAwaitClientCheckOwnerIsTkm'
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
			and S.IsDisabled is not null
			and S.IsDisabled = 0
			and S.new_positionHeld is not null
			and S.new_positionHeld =
			(
				ISNULL
				(
					(select top 1 ParameterValue
						from [SupportExtensions].[dbo].[ConfigParameters]
					where 
						ParameterName = 'PositionHeldTkm_ThirdOrFourthPriorityAwaitClientCheckOwnerIsTkm'
						and ServiceName = 'ActivationIncidentService')
				, 100000002) -- по умолчанию ТКМ
			)
			and WorkingDayDifference.WorkingDayDifferenceValue >= 
				(
					ISNULL
						(
							(select top 1 ParameterValue
								from [SupportExtensions].[dbo].[ConfigParameters]
							where 
								ParameterName = 'NumberDays_ThirdOrFourthPriorityAwaitClientCheckOwnerIsTkm'
								and ServiceName = 'ActivationIncidentService')
						, 1)
				)
			order by 
				COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) asc
				--,Bias.BiasValue desc
	end
	
	RETURN;
END
