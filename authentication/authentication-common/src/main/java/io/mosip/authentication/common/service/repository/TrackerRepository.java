package io.mosip.authentication.common.service.repository;

import io.mosip.authentication.common.service.entity.IdentityEntity;
import io.mosip.authentication.common.service.entity.TrackerEntity;
import io.mosip.kernel.core.dataaccess.spi.repository.BaseRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repository class for Identity Cache table
 * 
 * @author Loganathan Sekar
 *
 */
@Repository
public interface TrackerRepository extends BaseRepository<TrackerEntity, String> {
}
