package io.mosip.authentication.internal.service.batch;

import io.mosip.authentication.common.service.entity.CredentialEventStore;
import io.mosip.authentication.common.service.entity.IdentityEntity;
import io.mosip.authentication.common.service.repository.CredentialEventStoreRepository;
import io.mosip.authentication.common.service.repository.IdentityCacheRepository;
import io.mosip.authentication.common.service.repository.TrackerRepository;
import io.mosip.authentication.common.service.spi.idevent.CredentialStoreService;
import io.mosip.authentication.common.service.transaction.manager.IdAuthSecurityManager;
import io.mosip.authentication.core.exception.IdAuthenticationBusinessException;
import io.mosip.authentication.core.exception.RetryingBeforeRetryIntervalException;
import io.mosip.authentication.core.logger.IdaLogger;
import io.mosip.authentication.internal.service.config.DataProcessingBatchConfig;
import io.mosip.idrepository.core.security.IdRepoSecurityManager;
import io.mosip.kernel.core.exception.ExceptionUtils;
import io.mosip.kernel.core.logger.spi.Logger;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
    @Transactional
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        long startTime = System.currentTimeMillis();
        String batchId =  (String) chunkContext.getStepContext().getJobExecutionContext().get("batchId");
        List<CredentialEventStore> credentialEventStoreList = credentialEventRepo.findNewOrFailedEvents(pageSize);
        List<List<CredentialEventStore>> credentialEventStorePartition = partition(credentialEventStoreList, threadCount);

        try {
            forkJoinPool.submit(() -> credentialEventStorePartition.parallelStream().forEach(credential -> {
                List<IdentityEntity> identityEntities = new ArrayList<>(credential.size());

                for(CredentialEventStore credentialEventStore : credential) {
                    try {
                        identityEntities.add(credentialStoreService.processCredentialStoreEvent(credentialEventStore));
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
                }

                if (!identityEntities.isEmpty()) {
                   try {
                        credentialStoreService.storeIdentityEntity(identityEntities);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            })).get();
        } catch (InterruptedException e) {
            LOGGER.error(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - InterruptedException : " + ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (ExecutionException e) {
            LOGGER.error(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - ExecutionException : " + ExceptionUtils.getStackTrace(e));
        }

        if (!CollectionUtils.isEmpty(credentialEventStorePartition)) {
            credentialEventRepo.saveAll(credentialEventStoreList);
            long endTime = System.currentTimeMillis();
            LOGGER.debug(IdRepoSecurityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                    "THAM - Total time taken to complete till update of " + credentialEventStorePartition.size() + " records (" + (endTime - startTime) + "ms)");
        }
        LOGGER.info(securityManager.getUser(), IDA_CREDENTIAL_ITEM_TASKLET, "batchid = " + batchId,
                "THAM - CredentialStoreTasklet Existing Batch : " + (System.currentTimeMillis() - startTime) + " ms");

        return RepeatStatus.FINISHED;
    }

    public <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> parts = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            parts.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return parts;
    }

}
