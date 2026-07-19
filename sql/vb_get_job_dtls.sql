CREATE PROCEDURE dbo.vb_get_job_dtls
@TriggerName varchar(50)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @cols AS NVARCHAR(MAX), @query AS NVARCHAR(MAX)
SET @cols =
(
	SELECT STRING_AGG(dtls_key,',')
	FROM (
		SELECT DISTINCT QUOTENAME(dtls_key) AS dtls_key
		FROM [dbo].[vb_tbl_trigger] a
		JOIN [dbo].[vb_tbl_job] b ON a.trigger_id = b.trigger_id
		JOIN [dbo].[vb_tbl_job_dtls] c ON b.jobid = c.jobid
		WHERE a.trigger_name = @TriggerName
	)str_agg
)
SET @query = N'
SELECT trigger_name, jobid, L2_switch_type, L3_switch_type, L4_switch_type, ' + @cols + N'
FROM (
	SELECT dtls_value, dtls_key, a.trigger_name, b.jobid, L2_switch_type, L3_switch_type, L4_switch_type
	FROM dbo.vb_tbl_trigger a
	JOIN dbo.vb_tbl_job b ON a.trigger_id = b.trigger_id
	JOIN dbo.vb_tbl_job_dtls c ON b.jobid = c.jobid
	WHERE a.trigger_name = '''+ @TriggerName +'''
)x
PIVOT ( MAX(dtls_value) FOR dtls_key IN (' + @cols + N') )p'
EXEC sp_executesql @query;
END