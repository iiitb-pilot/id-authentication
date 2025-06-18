package io.mosip.authentication.internal.service.batch;

import java.util.List;
import java.util.UUID;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import io.mosip.authentication.core.logger.IdaLogger;
import io.mosip.kernel.core.logger.spi.Logger;

/**
 * The listener interface for receiving credentialStoreJobExecution events. The
 * class that is interested in processing a credentialStoreJobExecution event
 * implements this interface, and the object created with that class is
 * registered with a component using the component's
 * <code>addCredentialStoreJobExecutionListener<code> method. When the
 * credentialStoreJobExecution event occurs, that object's appropriate method is
 * invoked.
 *
 * @see CredentialStoreJobExecutionEvent
 */
@Component
public class CredentialStoreJobExecutionListener implements JobExecutionListener {

	/** The Constant logger. */
	private static final Logger logger = IdaLogger.getLogger(CredentialStoreJobExecutionListener.class);

	/**
	 * Before job.
	 *
	 * @param jobExecution the job execution
	 */
	@Override
	public void beforeJob(JobExecution jobExecution) {
		String batchId = UUID.randomUUID().toString();
		logger.info("THAM - CredentialStoreTasklet - beforeJob", "beforeJob", "batchid = " + batchId, "Entering Method : " + System.currentTimeMillis());
		jobExecution.getExecutionContext().putString("batchId", batchId);
	}

	/**
	 * After job.
	 *
	 * @param jobExecution the job execution
	 */
	@Override
	public void afterJob(JobExecution jobExecution) {
		if (!jobExecution.getStepExecutions().isEmpty()
				&& jobExecution.getStepExecutions().iterator().next().getReadCount() > 0) {
			List<Throwable> allFailureExceptions = jobExecution.getAllFailureExceptions();
			if(allFailureExceptions != null && !allFailureExceptions.isEmpty()) {
				String exceptions = String.valueOf(allFailureExceptions);
				logger.error("CredentialStoreJobExecutionListener", "afterJob", "Failed job execution",
						exceptions);
			}
		}


		logger.info("THAM - CredentialStoreTasklet - afterJob", "afterJob", "batchid = " + jobExecution.getExecutionContext().getString("batchId"), "Start Time : " + jobExecution.getStartTime() + " Exist Time : " + jobExecution.getEndTime() + " Total Time : " + (jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime()) + " Ending Time : " + + System.currentTimeMillis());
		//	logger.info("THAM - CredentialStoreTasklet", "afterJob", "AfterJob", "Entering Method");
	}

}
