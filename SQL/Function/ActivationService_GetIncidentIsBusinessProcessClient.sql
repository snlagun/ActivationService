USE [SupportExtensions]
GO
/****** Object:  UserDefinedFunction [dbo].[ActivationService_GetIncidentIsBusinessProcessClient]    Script Date: 04.06.2021 7:57:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<s.lagun>
-- Create date: <17.05.2021>
-- Description:	<Сервис активации. Получить список обращений с типом БП-клиента.>
-- =============================================
ALTER FUNCTION [dbo].[ActivationService_GetIncidentIsBusinessProcessClient]
(
)
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
						ParameterName = 'IsActive_BusinessProcessClient'
						and ServiceName = 'ActivationIncidentService')
				, 'false')
		) = 'true'
	)
	begin

		insert @result
			select 
				I.IncidentId
				,I.OwnerId
				,I.new_specialization SpecializationId
				,I.iok_KK_project ProjectId
				,I.CustomerId AccountId
				,I.New_podderzhka_klienty ClientCardId
				,I.ResponsibleContactId ContactId
				,I.TicketNumber
				,'BusinessProcessClient'
				,COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) Modified
				--,Bias.BiasValue
			from [ST_MSCRM].[dbo].[IncidentBase](nolock) I
			left join [ST_MSCRM].[dbo].[svk_projectBase](nolock) P on (I.iok_KK_project = P.svk_projectId)
			--cross apply (select [SupportExtensions].[dbo].[GetBiasByAccountId](I.CustomerId) as BiasValue) as Bias
			cross apply (select [SupportExtensions].[dbo].[WorkingDayDifference]((COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE())), GETDATE()) as WorkingDayDifferenceValue) as WorkingDayDifference
			where
				P.new_bp_tab is not null
				and P.new_bp_tab = 1
				and I.StateCode is not null
				and I.StateCode in 
				(
					select value from [SupportExtensions].[dbo].[SplitStringCustom]
						(
							(select top 1 ParameterValue
								from [SupportExtensions].[dbo].[ConfigParameters]
							where 
								ParameterName = 'StateCodes_BusinessProcessClient'
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
								ParameterName = 'CaseTypeCodes_BusinessProcessClient'
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
								ParameterName = 'StatesInternal_BusinessProcessClient'
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
									ParameterName = 'NumberDays_BusinessProcessClient'
									and ServiceName = 'ActivationIncidentService')
							, 5)
					)
			order by 
				COALESCE(I.New_data_send_notif, I.ModifiedOn, I.CreatedOn, GETDATE()) asc
				--,Bias.BiasValue desc
	end
	return;
end
