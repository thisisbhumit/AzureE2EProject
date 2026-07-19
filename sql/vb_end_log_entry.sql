CREATE PROCEDURE dbo.vb_end_log_entry
@jobid int,
@pipeline_id varchar(100),
@error varchar(max)
AS
BEGIN
UPDATE [dbo].[vb_tbl_log_dtls]
SET job_end_time = getdate(),
	job_status = CASE WHEN @error IS NULL THEN 'Completed' ELSE 'Failed' END,
	error_dtls = @error
WHERE jobid = @jobid AND pipeline_id = @pipeline_id
END