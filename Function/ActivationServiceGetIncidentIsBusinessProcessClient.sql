USE [SupportExtensions]
GO
/****** Object:  UserDefinedFunction [dbo].[ActivationServiceGetIncidentIsBusinessProcessClient]    Script Date: 17.02.2021 15:31:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER FUNCTION [dbo].[ActivationServiceGetIncidentIsBusinessProcessClient]
(
)
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
						ParameterName = 'IsActive_BusinessProcessClient'
						and ServiceName = 'ActivationIncidentService')
				, 'false') -- Значение по умолчанию выключено
		) = 'true'
	)
	begin

		insert @result
			select 
				I.IncidentId
				,I.TicketNumber
			from [ST_MSCRM].[dbo].[IncidentBase](nolock) I
			left join [ST_MSCRM].[dbo].[svk_projectBase](nolock) P on (I.iok_KK_project = P.svk_projectId)
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
				and WorkingDayDifference.WorkingDayDifferenceValue > 
					(
						ISNULL
							(
								(select top 1 ParameterValue
									from [SupportExtensions].[dbo].[ConfigParameters]
								where 
									ParameterName = 'NumberDays_BusinessProcessClient'
									and ServiceName = 'ActivationIncidentService')
							, 5) -- По умолчанию 5 дней
					)
	end
	return;
end
