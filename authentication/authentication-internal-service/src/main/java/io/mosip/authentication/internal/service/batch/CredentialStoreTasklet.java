package io.mosip.authentication.internal.service.batch;

import io.mosip.authentication.common.service.entity.CredentialEventStore;
import io.mosip.authentication.common.service.entity.IdentityEntity;
import io.mosip.authentication.common.service.entity.TrackerEntity;
import io.mosip.authentication.common.service.repository.CredentialEventStoreRepository;
import io.mosip.authentication.common.service.repository.IdentityCacheRepository;
import io.mosip.authentication.common.service.repository.TrackerRepository;
import io.mosip.authentication.common.service.spi.idevent.CredentialStoreService;
import io.mosip.authentication.common.service.transaction.manager.IdAuthSecurityManager;
import io.mosip.authentication.core.constant.IdAuthCommonConstants;
import io.mosip.authentication.core.exception.IdAuthenticationBusinessException;
import io.mosip.authentication.core.exception.RetryingBeforeRetryIntervalException;
import io.mosip.authentication.core.logger.IdaLogger;
import io.mosip.authentication.internal.service.config.DataProcessingBatchConfig;
import io.mosip.idrepository.core.security.IdRepoSecurityManager;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.kernel.core.util.DateUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

@Component
public class CredentialStoreTasklet implements Tasklet {

    @Value("${ida.internal.batch.thread.count:10}")
    private int threadCount;

    /** The logger. */
    private static Logger LOGGER = IdaLogger.getLogger(DataProcessingBatchConfig.class);

    ForkJoinPool forkJoinPool;

    private static final String IDA_CREDENTIAL_ITEM_TASKLET = "CredentialStoreTasklet";

    @Autowired
    private IdAuthSecurityManager securityManager;

    @Autowired
    private CredentialEventStoreRepository credentialEventRepo;

    @Autowired
    private IdentityCacheRepository identityCacheRepo;

    @Autowired
    private TrackerRepository trackerRepository;

    @Value("${ida.internal.batch.page.size:100}")
    private int pageSize;

    /** The credential store service. */
    @Autowired
    private CredentialStoreService credentialStoreService;

    @PostConstruct
    public void init() {
        forkJoinPool = new ForkJoinPool(threadCount);
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
        ExecutionContext context = stepExecution.getExecutionContext();
        long startTime1 = System.currentTimeMillis();
        String batchId = chunkContext.getAttribute("batchId").toString();

        LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                "Inside CredentialStoreTasklet.execute() method");
       // LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
        //        "THAM - CredentialStoreTasklet Batch Starting");

        List<CredentialEventStore> credentialEventStoreList = credentialEventRepo.findNewOrFailedEvents(pageSize);
        LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                "THAM - Successfully Fetch Record from Table. Total Records : " + credentialEventStoreList.size() + " Time : " + (System.currentTimeMillis() - startTime1) + " ms");
        List<IdentityEntity> identityEntities = new ArrayList<>();
        try {
    //        LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
    //                "THAM - CredentialStoreTasklet Entering into Thread");

            forkJoinPool.submit(() -> credentialEventStoreList.parallelStream().forEach(credential -> {
                try {
                    long startTime = System.currentTimeMillis();

                    LOGGER.info(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                           "THAM - started processing item : " + credential.getCredentialTransactionId() + " Time : " + (System.currentTimeMillis() - startTime) + " ms");
                    identityEntities.add(credentialStoreService.processCredentialStoreEvent(credential, startTime));
                //    IdentityEntity identityEntity = credentialStoreService.processCredentialStoreEvent(credential, startTime);
                    LOGGER.info(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                            "THAM - created Identity Entity Object processing item : " + credential.getCredentialTransactionId() + " Time : " + (System.currentTimeMillis() - startTime) + " ms" + " Total Time : " + (System.currentTimeMillis() - startTime1) + " ms");

                //    identityCacheRepo.save(identityEntity);

                //    identityCacheRepo.flush();;

                    LOGGER.info(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                            "THAM - ended processing item : " + credential.getCredentialTransactionId() + " Time : " + (System.currentTimeMillis() - startTime) + " ms"+ " Total Time : " + (System.currentTimeMillis() - startTime1) + " ms");
            /*        TrackerEntity entity = new TrackerEntity();
                    entity.setId(UUID.randomUUID().toString());
                    entity.setBatchId(batchId);
                    entity.setTransactionId(credential.getCredentialTransactionId());
                    entity.setCrDTimes(DateUtils.getUTCCurrentDateTime());
                    entity.setStDTimes(DateUtils.parseDateToLocalDateTime((new Date(startTime1))));
                    entity.setTimeTaken((int) (System.currentTimeMillis() - startTime));
                    trackerRepository.save(entity); */
                } catch (IdAuthenticationBusinessException e) {
                    LOGGER.error(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                            "THAM - IdAuthenticationBusinessException : " + ExceptionUtils.getStackTrace(e));
                } catch (RetryingBeforeRetryIntervalException e) {
                    LOGGER.error(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                            "THAM - RetryingBeforeRetryIntervalException : " + ExceptionUtils.getStackTrace(e));
                } catch (Exception e) {
                    LOGGER.error(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                            "THAM - xception : " + ExceptionUtils.getStackTrace(e));
                }
            })).get();
     //       LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
     //               "THAM - CredentialStoreTasklet Exit out Thread");

            long endTime = System.currentTimeMillis();
            LOGGER.info(IdRepoSecurityManager.getUser(), "Perform" + IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - Total time taken to complete process of " + credentialEventStoreList.size() + " records (" + (endTime - startTime1) + "ms)" );
        } catch (InterruptedException e) {
            LOGGER.error(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - InterruptedException : " + ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (ExecutionException e) {
            LOGGER.error(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - ExecutionException : " + ExceptionUtils.getStackTrace(e));
        }

        if (!CollectionUtils.isEmpty(credentialEventStoreList)) {
            long startTime2 = System.currentTimeMillis();
            LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - CredentialStoreTasklet bEFORE Update Identity cache & flush Batch : " + (System.currentTimeMillis() - startTime1) + " ms");

            credentialStoreService.storeIdentityEntity(identityEntities);
            LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - CredentialStoreTasklet Update Identity cache & flush Batch : " + (System.currentTimeMillis() - startTime1) + " ms");

            credentialEventRepo.saveAll(credentialEventStoreList);
//            credentialEventRepo.flush();
            LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - CredentialStoreTasklet AFTER Update Credential_event_store : " + (System.currentTimeMillis() - startTime1) + " ms");


            long endTime = System.currentTimeMillis();
 /*           TrackerEntity entity = new TrackerEntity();
            entity.setId(UUID.randomUUID().toString());
            entity.setBatchId(batchId);
            entity.setTransactionId("Record Saving Time");
            entity.setCrDTimes(DateUtils.getUTCCurrentDateTime());
            entity.setStDTimes(DateUtils.parseDateToLocalDateTime((new Date(startTime2))));
            entity.setTimeTaken((int) (endTime - startTime2));
            trackerRepository.save(entity);*/

            LOGGER.debug(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - Total time taken to complete till update of " + credentialEventStoreList.size() + " records (" + (endTime - startTime1) + "ms)");
        }
        LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                "THAM - CredentialStoreTasklet Existing Batch : " + (System.currentTimeMillis() - startTime1) + " ms");

        return RepeatStatus.FINISHED;
    }
}
