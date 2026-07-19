CREATE PROCEDURE dbo.vb_start_log_entry
@jobid int,
@pipeline_id varchar(100)
AS
BEGIN
INSERT INTO [dbo].[vb_tbl_log_dtls]
(jobid, pipeline_id, job_start_time, job_status, created_user, created_date, updated_user, updated_date)
SELECT @jobid, @pipeline_id, getdate(), 'Running', system_user, getdate(), system_user, getdate()
END